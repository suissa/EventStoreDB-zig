//! Catch-up subscriptions for a single stream and the global
//! all-stream log. Each subscription is backed by a small thread
//! that polls the DB; the broadcast waker interrupts the poll
//! immediately after an append commits, so the latency on a
//! busy stream is sub-millisecond.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const Waker = @import("waker.zig").Waker;

const RunContext = struct {
    client: *Client,
    stream_id: []const u8,
    allocator: std.mem.Allocator,
    from_rev: i64,
    from_all: bool,
    from_pos_commit: u64,
    poll_interval_ms: u32,
    channel: std.Thread.Channel(types.RecordedEventOrErr),
};

const RecordedEventOrErr = union(enum) {
    event: types.RecordedEvent,
    err: errors_mod.Error,
    closed: void,
};

/// Subscribe to a single stream. Returns immediately; events
/// are delivered on the channel returned in `Subscription.events`.
pub fn subscribeToStream(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    const from_rev: i64 = switch (opts.from) {
        .start => 0,
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

    const buffer_size: u32 = if (opts.buffer_size == 0) 256 else opts.buffer_size;
    const channel = std.Thread.Channel(types.RecordedEventOrErr).init(buffer_size);

    const sub = Subscription{
        .channel = channel,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, stream_id),
    };

    const ctx = try allocator.create(RunContext);
    ctx.* = .{
        .client = self,
        .stream_id = sub.stream_id,
        .allocator = allocator,
        .from_rev = from_rev,
        .from_all = false,
        .from_pos_commit = 0,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .channel = channel,
    };
    const thread = try std.Thread.spawn(.{}, runStream, .{ctx});
    sub.thread = thread;
    return sub;
}

/// Subscribe to the global all-stream log.
pub fn subscribeToAll(
    self: *Client,
    allocator: std.mem.Allocator,
    opts: types.SubscribeOptions,
) (errors_mod.Error || std.mem.Allocator.Error)!Subscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const from_pos_commit: u64 = switch (opts.from) {
        .start, .position => 0,
        .end, .revision => self.lastLogPosition() + 1,
    };

    const buffer_size: u32 = if (opts.buffer_size == 0) 256 else opts.buffer_size;
    const channel = std.Thread.Channel(types.RecordedEventOrErr).init(buffer_size);

    const sub = Subscription{
        .channel = channel,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, "$all"),
    };

    const ctx = try allocator.create(RunContext);
    ctx.* = .{
        .client = self,
        .stream_id = sub.stream_id,
        .allocator = allocator,
        .from_rev = 0,
        .from_all = true,
        .from_pos_commit = from_pos_commit,
        .poll_interval_ms = if (opts.poll_interval_ms == 0) self.poll_interval_ms else opts.poll_interval_ms,
        .channel = channel,
    };
    const thread = try std.Thread.spawn(.{}, runAll, .{ctx});
    sub.thread = thread;
    return sub;
}

pub const Subscription = struct {
    channel: std.Thread.Channel(types.RecordedEventOrErr),
    thread: ?std.Thread = null,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    closed: bool = false,

    /// Receive the next event. Blocks until one is available
    /// or the subscription is closed. Returns `null` on close.
    pub fn receive(self: *Subscription) ?types.RecordedEventOrErr {
        if (self.closed) return null;
        return self.channel.get();
    }

    /// Try to receive without blocking. Returns null if no
    /// event is immediately available.
    pub fn tryReceive(self: *Subscription) ?types.RecordedEventOrErr {
        if (self.closed) return null;
        return self.channel.try_get();
    }

    /// Stop the subscription. Safe to call multiple times.
    pub fn close(self: *Subscription) void {
        if (self.closed) return;
        self.closed = true;
        self.channel.put(.{ .closed = {} });
        if (self.thread) |t| t.join();
        self.allocator.free(self.stream_id);
    }
};

fn runStream(ctx: *RunContext) void {
    defer {
        ctx.channel.put(.{ .closed = {} });
        ctx.allocator.destroy(ctx);
    }
    var cursor: i64 = ctx.from_rev;
    const waiter = ctx.client.waker.register() catch return;
    defer ctx.client.waker.unregister(waiter);

    while (true) {
        const res = readStream(
            ctx.client,
            ctx.allocator,
            ctx.stream_id,
            .{ .from = .{ .revision = @intCast(@max(0, cursor)) }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            ctx.channel.put(.{ .err = err });
            return;
        };

        if (res.events.len == 0) {
            if (res.is_end_of_stream) {
                _ = waiter.wait(ctx.poll_interval_ms);
            }
            continue;
        }

        for (res.events) |ev| {
            ctx.channel.put(.{ .event = ev });
            cursor = @intCast(ev.revision + 1);
        }

        if (res.events.len >= ctx.client.max_batch_size) continue;
        _ = waiter.wait(ctx.poll_interval_ms);
    }
}

fn runAll(ctx: *RunContext) void {
    defer {
        ctx.channel.put(.{ .closed = {} });
        ctx.allocator.destroy(ctx);
    }
    var cursor: u64 = ctx.from_pos_commit;
    const waiter = ctx.client.waker.register() catch return;
    defer ctx.client.waker.unregister(waiter);

    while (true) {
        const res = readAll(
            ctx.client,
            ctx.allocator,
            .{ .from = .{ .position = .{ .commit = cursor, .prepare = cursor } }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            ctx.channel.put(.{ .err = err });
            return;
        };

        if (res.events.len == 0) {
            if (res.is_end_of_stream) {
                _ = waiter.wait(ctx.poll_interval_ms);
            }
            continue;
        }

        for (res.events) |ev| {
            ctx.channel.put(.{ .event = ev });
            cursor = ev.log_position + 1;
        }

        if (res.events.len >= ctx.client.max_batch_size) continue;
        _ = waiter.wait(ctx.poll_interval_ms);
    }
}

const readStream = @import("read.zig").readStream;
const readAll = @import("read.zig").readAll;
