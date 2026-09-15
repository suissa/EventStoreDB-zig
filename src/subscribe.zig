//! Catch-up subscriptions for a single stream and the global
//! all-stream log.
//!
//! Zig 0.16 removed `std.Thread.Channel` and the `std.Thread.Mutex`
//! family; the only remaining thread-level synchronisation is
//! `std.atomic.Mutex` (a two-state enum with no kernel waits).
//! Subscriptions in this file use a small bounded queue protected
//! by that mutex plus a wake flag that the append path sets after
//! a commit. The receiver busy-waits on the flag, which is fine
//! for the throughput this store targets and matches the poll
//! model the tests already exercise.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const Waker = @import("waker.zig").Waker;

const Queue = struct {
    mu: std.atomic.Mutex = .unlocked,
    head: usize = 0,
    tail: usize = 0,
    buf: [4096]types.RecordedEventOrErr = undefined,
    closed: bool = false,
    allocator: std.mem.Allocator,

    fn put(self: *Queue, item: types.RecordedEventOrErr) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        const next_tail = (self.tail + 1) % self.buf.len;
        if (next_tail == self.head) {
            // Queue is full: free the event's slices before
            // dropping it. The receiver is too slow; we have
            // already moved ownership here.
            if (item == .event) freeEvent(self.allocator, &item.event);
            return;
        }
        self.buf[self.tail] = item;
        self.tail = next_tail;
    }

    fn get(self: *Queue) ?types.RecordedEventOrErr {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        if (self.head == self.tail) {
            if (self.closed) return .{ .closed = {} };
            return null;
        }
        const item = self.buf[self.head];
        self.head = (self.head + 1) % self.buf.len;
        return item;
    }

    fn tryGet(self: *Queue) ?types.RecordedEventOrErr {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        if (self.head == self.tail) return null;
        const item = self.buf[self.head];
        self.head = (self.head + 1) % self.buf.len;
        return item;
    }

    fn close(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        self.closed = true;
    }

    /// Free every event slice still buffered. Called after the
    /// subscription closes so a slow consumer that left events
    /// queued does not leak them.
    fn drain(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        while (self.head != self.tail) {
            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            if (item == .event) freeEvent(self.allocator, &item.event);
        }
    }
};

fn freeEvent(allocator: std.mem.Allocator, ev: *const types.RecordedEvent) void {
    allocator.free(@constCast(ev.stream_id));
    allocator.free(@constCast(ev.event_type));
    allocator.free(@constCast(ev.data));
    if (ev.metadata) |m| allocator.free(@constCast(m));
}

const RunContext = struct {
    client: *Client,
    stream_id: []const u8,
    allocator: std.mem.Allocator,
    from_rev: i64,
    from_all: bool,
    from_pos_commit: u64,
    poll_interval_ms: u32,
    queue: *Queue,
    done: *std.atomic.Value(bool),
};

/// Subscribe to a single stream. Returns immediately; events
/// are delivered on the queue returned in `Subscription`.
pub fn subscribeToStream(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    // Register the worker with the client *before* spawning, so
    // a concurrent `close()` waits for this new worker before
    // tearing down the connection. The race "closed flips
    // between our load and our increment" is closed by
    // re-checking `closed` after the increment and rolling back
    // if we lose to a concurrent close.
    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const from_rev: i64 = switch (opts.from) {
        .start, .start_backward => 0,
        .end => blk: {
            // Use a prepared statement so the stream_id is bound,
            // not concatenated.
            const conn = self.conn;
            const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT revision FROM streams WHERE stream_id = ?");
            defer bind.finalize(conn.allocator, stmt);
            _ = bind.bindText(stmt, 1, stream_id);
            const rc = c.sqlite3_step(stmt);
            const v: i64 = if (rc == c.SQLITE_ROW) c.sqlite3_column_int64(stmt, 0) else -1;
            break :blk v + 1;
        },
        .revision => |r| @intCast(r),
        .position => 0,
    };

    const queue = try allocator.create(Queue);
    queue.* = .{ .allocator = allocator };
    const done = try allocator.create(std.atomic.Value(bool));
    done.* = std.atomic.Value(bool).init(false);

    var sub: Subscription = .{
        .queue = queue,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, stream_id),
        .done = done,
    };

    // The RunContext owns its own copy of the stream id so the
    // receiver can free `sub.stream_id` without invalidating the
    // worker's read cursor. The worker thread destroys its copy
    // when it exits.
    const ctx_stream_id = try allocator.dupe(u8, stream_id);

    const ctx = try allocator.create(RunContext);
    ctx.* = .{
        .client = self,
        .stream_id = ctx_stream_id,
        .allocator = allocator,
        .from_rev = from_rev,
        .from_all = false,
        .from_pos_commit = 0,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .queue = queue,
        .done = done,
    };
    sub.thread = try std.Thread.spawn(.{}, runStream, .{ctx});
    return sub;
}

/// Subscribe to the global all-stream log.
pub fn subscribeToAll(
    self: *Client,
    allocator: std.mem.Allocator,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    // Same race-closing pattern as `subscribeToStream` —
    // see the comment block there for the reasoning.
    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const from_pos_commit: u64 = switch (opts.from) {
        .start, .position, .start_backward => 0,
        .end, .revision => self.lastLogPosition() + 1,
    };

    const queue = try allocator.create(Queue);
    queue.* = .{ .allocator = allocator };
    const done = try allocator.create(std.atomic.Value(bool));
    done.* = std.atomic.Value(bool).init(false);

    var sub: Subscription = .{
        .queue = queue,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, "$all"),
        .done = done,
    };

    // The RunContext owns its own copy of the stream id so the
    // receiver can free `sub.stream_id` without invalidating the
    // worker's read cursor. The worker thread destroys its copy
    // when it exits.
    const ctx_stream_id = try allocator.dupe(u8, "$all");

    const ctx = try allocator.create(RunContext);
    ctx.* = .{
        .client = self,
        .stream_id = ctx_stream_id,
        .allocator = allocator,
        .from_rev = 0,
        .from_all = true,
        .from_pos_commit = from_pos_commit,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .queue = queue,
        .done = done,
    };
    sub.thread = try std.Thread.spawn(.{}, runAll, .{ctx});
    return sub;
}

pub const Subscription = struct {
    queue: *Queue,
    thread: ?std.Thread = null,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    done: *std.atomic.Value(bool),
    closed: bool = false,

    /// Receive the next event. Blocks until one is available
    /// or the subscription is closed. Returns `null` on close.
    pub fn receive(self: *Subscription) ?types.RecordedEventOrErr {
        while (true) {
            if (self.closed) return null;
            if (self.queue.get()) |item| {
                if (item == .closed) {
                    self.closed = true;
                    return null;
                }
                return item;
            }
            if (self.done.load(.seq_cst)) return null;
            std.atomic.spinLoopHint();
        }
    }

    /// Try to receive without blocking. Returns null if no
    /// event is immediately available.
    pub fn tryReceive(self: *Subscription) ?types.RecordedEventOrErr {
        if (self.closed) return null;
        return self.queue.tryGet();
    }

    /// Stop the subscription. Safe to call multiple times.
    pub fn close(self: *Subscription) void {
        if (self.closed) return;
        self.closed = true;
        self.queue.close();
        self.done.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        // After the worker thread is joined, anything left in
        // the queue is owned by us. Free it.
        self.queue.drain();
        self.allocator.free(self.stream_id);
        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.done);
    }
};

fn runStream(ctx: *RunContext) void {
    // Defer ordering matters here: Zig defers are LIFO, so
    // the LAST-declared defer runs FIRST. We want at exit:
    //
    //   1. decrement `active_workers`     <-- declared LAST
    //   2. unregister the Waiter          <-- declared 3rd
    //   3. free `ctx.stream_id`           <-- declared 2nd
    //   4. destroy `ctx`                  <-- declared 1st
    //
    // The previous version declared them in the opposite
    // order and segfaulted: `destroy(ctx)` ran first, after
    // which `ctx.client.active_workers.fetchSub` and
    // `ctx.allocator.free(ctx.stream_id)` both read through
    // the freed ctx pointer.
    var cursor: i64 = ctx.from_rev;
    const waiter = ctx.client.waker.register() catch return;
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.stream_id);
    defer ctx.client.waker.unregister(waiter);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    // Bail out promptly if the client was already closed before the
    // worker even spun up. Otherwise we touch `self.client.conn` on a
    // destroyed connection and segfault.
    //
    // Subscription reads are routed through `Client.read_conn`
    // (set in `Client.open`). When the caller enabled
    // `OpenOptions.separate_read_connection`, `read_conn` is
    // its own SQLite file handle and writers on `conn` no
    // longer block this worker — addressing the v0.1 stress
    // test's 500–800-of-1 000 delivery race.
    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readStreamOnConn(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            ctx.stream_id,
            .{ .from = .{ .revision = @intCast(@max(0, cursor)) }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            ctx.queue.put(.{ .err = err });
            continue;
        };

        // `readStream` returns an owned `[]RecordedEvent` (the outer
        // array). The inner slices on each row are shared with the
        // queue (which now owns them); the outer array is ours to
        // free after the for-loop hands ownership over.
        const outer = res.events;
        if (outer.len == 0) {
            if (res.is_end_of_stream) {
                _ = Waker.wait(waiter, ctx.poll_interval_ms);
            }
            continue;
        }

        for (outer) |ev| {
            ctx.queue.put(.{ .event = ev });
            cursor = @intCast(ev.revision + 1);
        }
        ctx.allocator.free(outer);

        if (outer.len >= ctx.client.max_batch_size) continue;
        _ = Waker.wait(waiter, ctx.poll_interval_ms);
    }
}

fn runAll(ctx: *RunContext) void {
    // See `runStream` for the LIFO defer-order rationale.
    var cursor: u64 = ctx.from_pos_commit;
    const waiter = ctx.client.waker.register() catch return;
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.stream_id);
    defer ctx.client.waker.unregister(waiter);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readAllOnConn(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            .{ .from = .{ .position = .{ .commit = cursor, .prepare = cursor } }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            ctx.queue.put(.{ .err = err });
            continue;
        };

        const outer = res.events;
        if (outer.len == 0) {
            if (res.is_end_of_stream) {
                _ = Waker.wait(waiter, ctx.poll_interval_ms);
            }
            continue;
        }

        for (outer) |ev| {
            ctx.queue.put(.{ .event = ev });
            cursor = ev.log_position + 1;
        }
        ctx.allocator.free(outer);

        if (outer.len >= ctx.client.max_batch_size) continue;
        _ = Waker.wait(waiter, ctx.poll_interval_ms);
    }
}

const readStreamOnConn = @import("read.zig").readStreamOnConn;
const readAllOnConn = @import("read.zig").readAllOnConn;
