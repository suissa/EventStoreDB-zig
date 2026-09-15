//! The `Client` type — top-level handle to a single event store
//! database. It owns the SQLite connection, the writer mutex,
//! the in-memory last-log-position cache, and the broadcast
//! waker that wakes catch-up subscribers after an append.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const Waker = @import("waker.zig").Waker;
const spinlock = @import("spinlock.zig");

pub const Client = struct {
    conn: *schema_mod.Connection,
    allocator: std.mem.Allocator,

    /// Connection used by subscription workers (catch-up
    /// `runStream`/`runAll` and persistent `runPS`).
    ///
    /// By default this is the same pointer as `conn` (the
    /// writer connection); subscriptions and writers then
    /// share one SQLite handle and contend under sustained
    /// append load — see the v0.1 stress-suite caveat.
    ///
    /// When `OpenOptions.separate_read_connection` is true,
    /// the client opens a second connection here (in the
    /// WAL mode default) so subscriptions read on their own
    /// handle. In that case `read_conn_owned` is true so
    /// `close()` knows to release the second connection
    /// separately from `conn`.
    read_conn: *schema_mod.Connection,
    read_conn_owned: bool = false,

    /// Writer mutex — SQLite is single-writer per file. We
    /// serialize all appends inside this process to avoid
    /// SQLITE_BUSY and to keep the transaction code simple.
    writer_mu: spinlock.Spinlock = .{},

    /// In-memory cache of the highest log position allocated.
    /// Maintained by `appendToStream` and loaded once at open.
    last_log_pos: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Broadcast to every catch-up subscriber when an append
    /// commits. Lets them wake instantly rather than waiting
    /// for their next poll tick.
    waker: Waker,

    /// Set to true by `close`. Other methods check this and
    /// return `error.DatabaseClosed` instead of dereferencing
    /// the connection.
    closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Live count of background worker threads (catch-up +
    /// persistent subscription loops) currently inside the
    /// client. `close()` waits on this counter reaching zero
    /// before tearing the connection down so an in-flight
    /// `readStream` cannot dereference a closed handle.
    /// Incremented by the spawn helpers in `subscribe.zig` /
    /// `persistent.zig` *after* re-checking `closed` (to close
    /// the small window where a caller races with a concurrent
    /// `close()`), and decremented via `defer` at worker exit.
    active_workers: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Open options captured for diagnostics and reused by
    /// subscription defaults.
    path: []const u8,
    poll_interval_ms: u32,
    max_batch_size: u32,

    /// Open a new client. The path can be ":memory:" for an
    /// in-memory store, or any SQLite-acceptable file path.
    pub fn open(allocator: std.mem.Allocator, opts: types.OpenOptions) errors_mod.Error!*Client {
        const conn = try schema_mod.Connection.open(allocator, opts.path, opts.busy_timeout_ms);
        errdefer conn.close();

        const client = try allocator.create(Client);
        errdefer allocator.destroy(client);

        const path_copy = try allocator.dupe(u8, opts.path);
        errdefer allocator.free(path_copy);

        // Dedicated read connection for subscription workers.
        // Opened with `SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE`
        // so the migration pragmas inside `Connection.open`
        // remain idempotent (WAL mode is per-connection but the
        // file-level schema is shared and already-current; the
        // second open just observes it). We never issue writes
        // through this handle from the library.
        const read_conn: *schema_mod.Connection = if (opts.separate_read_connection)
            try schema_mod.Connection.open(allocator, opts.path, opts.busy_timeout_ms)
        else
            conn;
        errdefer if (opts.separate_read_connection) read_conn.close();

        client.* = .{
            .conn = conn,
            .read_conn = read_conn,
            .read_conn_owned = opts.separate_read_connection,
            .allocator = allocator,
            .waker = Waker.init(allocator),
            .path = path_copy,
            .poll_interval_ms = opts.poll_interval_ms,
            .max_batch_size = opts.max_batch_size,
        };

        // Warm the last_log_pos cache.
        client.last_log_pos.store(@intCast(try conn.queryScalarI64("SELECT COALESCE(MAX(log_position), 0) FROM events")), .seq_cst);

        return client;
    }

    /// Release the connection. After this, all other methods
    /// return `error.DatabaseClosed`. In-flight subscriptions
    /// will observe a closed channel.
    pub fn close(self: *Client) void {
        if (self.closed.swap(true, .seq_cst)) return;

        // Order matters here. Step 1 flags each registered
        // `Waiter` so the worker busy-loops inside
        // `Waker.wait()` exit on their next slice, instead of
        // sitting out their full poll timeout. Step 2 then
        // waits for that exit to actually happen (workers do
        // `defer { _ = active_workers.fetchSub(1, ...) }`, so
        // reaching zero proves no read is in flight against
        // the SQLite handle).
        self.waker.deinit();

        // Hard cap so a wedged worker (blocked in
        // `Waker.wait`, in a paging-induced sleep, or in any
        // unrelated kernel call) cannot make `close()` hang
        // the process. Past the cap the worker may segfault
        // when it dereferences the destroyed `self.conn`; that
        // is no worse than the pre-fix behaviour and signals a
        // real bug to the operator rather than masking it.
        const deadline_ns: u64 = 200 * std.time.ns_per_ms;
        var threaded = std.Io.Threaded.init_single_threaded;
        const io = threaded.io();
        const start = std.Io.Clock.now(.boot, io).nanoseconds;
        while (self.active_workers.load(.seq_cst) != 0) {
            const elapsed = std.Io.Clock.now(.boot, io).nanoseconds - start;
            if (elapsed > deadline_ns) break;
            std.atomic.spinLoopHint();
        }

        if (self.read_conn_owned) self.read_conn.close();
        self.conn.close();
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    /// Return the most recent log position. Used by
    /// `appendToStream` to validate the in-memory cache.
    pub fn lastLogPosition(self: *Client) u64 {
        return self.last_log_pos.load(.seq_cst);
    }

    /// Aggregate statistics. Useful for the CLI's `stats`
    /// command and for tests.
    pub fn stats(self: *Client) errors_mod.Error!types.Stats {
        if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

        return .{
            .stream_count = try scalarI64(self.conn, "SELECT COUNT(*) FROM streams"),
            .event_count = try scalarI64(self.conn, "SELECT COUNT(*) FROM events"),
            .tombstoned_streams = try scalarI64(self.conn, "SELECT COUNT(*) FROM streams WHERE deleted_at IS NOT NULL"),
            .persistent_groups = try scalarI64(self.conn, "SELECT COUNT(*) FROM persistent_subscriptions"),
            .snapshots = try scalarI64(self.conn, "SELECT COUNT(*) FROM snapshots"),
            .db_size_bytes = try scalarI64(self.conn, "SELECT CAST(page_count AS INTEGER) * page_size FROM pragma_page_count(), pragma_page_size()"),
            .last_log_position = self.lastLogPosition(),
        };
    }
};

pub fn scalarI64(conn: *schema_mod.Connection, sql: []const u8) !i64 {
    return conn.queryScalarI64(sql);
}
