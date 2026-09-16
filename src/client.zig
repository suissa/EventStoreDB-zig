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
    /// read cannot dereference a closed handle.
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

        client.last_log_pos.store(@intCast(try conn.queryScalarI64("SELECT COALESCE(MAX(log_position), 0) FROM events")), .seq_cst);

        return client;
    }

    /// Release the connection. After this, all other methods
    /// return `error.DatabaseClosed`.
    ///
    /// Shutdown is deterministic: once `closed` is published we
    /// wake every catch-up waiter and do not destroy SQLite or the
    /// Client allocation until every registered background worker
    /// has exited. A bounded timeout here would turn a slow worker
    /// into a use-after-free, so worker lifetime is part of the
    /// Client ownership contract rather than a best-effort wait.
    pub fn close(self: *Client) void {
        if (self.closed.swap(true, .seq_cst)) return;

        self.waker.deinit();

        while (self.active_workers.load(.seq_cst) != 0) {
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
