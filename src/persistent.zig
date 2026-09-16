//! Persistent subscriptions. Each consumer group is a row in
//! `persistent_subscriptions` plus an in-flight table
//! `persistent_acks` that records every delivered-but-not-acked
//! event.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const readStream = @import("read.zig").readStreamOnConn;

const PersistentMessageOrErr = union(enum) {
    message: types.PersistentMessage,
    err: errors_mod.Error,
    closed: void,
};

const Queue = struct {
    mu: std.atomic.Mutex = .unlocked,
    head: usize = 0,
    tail: usize = 0,
    buf: [4096]PersistentMessageOrErr = undefined,
    closed: bool = false,
    allocator: std.mem.Allocator,

    fn put(self: *Queue, item: PersistentMessageOrErr) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        const next_tail = (self.tail + 1) % self.buf.len;
        if (next_tail == self.head) {
            freeItem(self.allocator, item);
            return;
        }
        self.buf[self.tail] = item;
        self.tail = next_tail;
    }

    fn get(self: *Queue) ?PersistentMessageOrErr {
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

    fn close(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        self.closed = true;
    }

    fn drain(self: *Queue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        while (self.head != self.tail) {
            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            freeItem(self.allocator, item);
        }
    }
};

fn freeItem(allocator: std.mem.Allocator, item: PersistentMessageOrErr) void {
    if (item == .message) types.freeEvent(allocator, item.message.event);
}

pub fn createPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    opts: types.PersistentOptions,
    cfg: types.PersistentConfig,
    overwrite: bool,
) (errors_mod.Error || std.mem.Allocator.Error)!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    if (!overwrite) {
        const stmt = bind.prepare(
            self.conn.db,
            self.conn.allocator,
            "SELECT 1 FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?",
        ) catch return error.Sqlite;
        defer bind.finalize(self.conn.allocator, stmt);
        _ = bind.bindText(stmt, 1, group_name);
        _ = bind.bindText(stmt, 2, stream_id);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) return error.PersistentSubscriptionExists;
    }

    const from_rev: i64 = switch (opts.from) {
        .start, .start_backward => 0,
        .end => 0,
        .revision => |r| @intCast(r),
        .position => 0,
    };

    const cfg_buf = stringifyConfig(allocator, cfg) catch return error.Sqlite;
    defer allocator.free(cfg_buf);

    const sql =
        \\INSERT INTO persistent_subscriptions(group_name, stream_id, start_from, last_position, revision, config, status, created_at, updated_at)
        \\VALUES (?, ?, ?, ?, 1, ?, 'Live', ?, ?)
        \\ON CONFLICT(group_name, stream_id) DO UPDATE SET
        \\  start_from = excluded.start_from,
        \\  last_position = excluded.last_position,
        \\  config = excluded.config,
        \\  status = 'Live',
        \\  updated_at = excluded.updated_at
    ;
    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindI64(stmt, 3, from_rev);
    _ = bind.bindI64(stmt, 4, from_rev);
    _ = bind.bindBlob(stmt, 5, cfg_buf);
    _ = bind.bindI64(stmt, 6, schema_mod.nowMs());
    _ = bind.bindI64(stmt, 7, schema_mod.nowMs());
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
}

pub fn deletePersistentSubscription(
    self: *Client,
    group_name: []const u8,
    stream_id: []const u8,
) errors_mod.Error!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (group_name.len == 0 or stream_id.len == 0) return error.InvalidArgument;

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    const s1 = bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "DELETE FROM persistent_acks WHERE group_name = ? AND stream_id = ?",
    ) catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s1);
    _ = bind.bindText(s1, 1, group_name);
    _ = bind.bindText(s1, 2, stream_id);
    _ = c.sqlite3_step(s1);

    const s2 = bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "DELETE FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?",
    ) catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s2);
    _ = bind.bindText(s2, 1, group_name);
    _ = bind.bindText(s2, 2, stream_id);
    _ = c.sqlite3_step(s2);
}

const PSRunContext = struct {
    client: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    queue: *Queue,
    done: *std.atomic.Value(bool),
    last_position: u64,
};

pub fn connectPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!PersistentSubscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

    _ = self.active_workers.fetchAdd(1, .seq_cst);
    errdefer _ = self.active_workers.fetchSub(1, .seq_cst);
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;

    const stmt = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "SELECT last_position, config FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?",
    );
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, group_name);
    _ = bind.bindText(stmt, 2, stream_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.PersistentSubscriptionNotFound;

    const last_pos = c.sqlite3_column_int64(stmt, 0);

    const queue = try allocator.create(Queue);
    errdefer allocator.destroy(queue);
    queue.* = .{ .allocator = allocator };

    const done = try allocator.create(std.atomic.Value(bool));
    errdefer allocator.destroy(done);
    done.* = std.atomic.Value(bool).init(false);

    const sub_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(sub_stream_id);
    const sub_group_name = try allocator.dupe(u8, group_name);
    errdefer allocator.free(sub_group_name);

    const ctx = try allocator.create(PSRunContext);
    errdefer allocator.destroy(ctx);
    const ctx_stream_id = try allocator.dupe(u8, stream_id);
    errdefer allocator.free(ctx_stream_id);
    const ctx_group_name = try allocator.dupe(u8, group_name);
    errdefer allocator.free(ctx_group_name);

    ctx.* = .{
        .client = self,
        .allocator = allocator,
        .stream_id = ctx_stream_id,
        .group_name = ctx_group_name,
        .queue = queue,
        .done = done,
        .last_position = @intCast(last_pos),
    };

    const thread = try std.Thread.spawn(.{}, runPS, .{ctx});

    return .{
        .client = self,
        .allocator = allocator,
        .stream_id = sub_stream_id,
        .group_name = sub_group_name,
        .queue = queue,
        .done = done,
        .thread = thread,
        .last_position = @intCast(last_pos),
    };
}

pub const PersistentSubscription = struct {
    client: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    queue: *Queue,
    done: *std.atomic.Value(bool),
    thread: ?std.Thread = null,
    last_position: u64,
    closed: bool = false,

    pub fn receive(self: *PersistentSubscription) ?PersistentMessageOrErr {
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

    pub fn tryReceive(self: *PersistentSubscription) ?PersistentMessageOrErr {
        if (self.closed) return null;
        return self.queue.get();
    }

    pub fn ack(self: *PersistentSubscription, ev_id: types.Uuid) errors_mod.Error!void {
        if (self.closed) return error.SubscriptionClosed;
        if (self.client.closed.load(.seq_cst)) return error.DatabaseClosed;
        self.client.writer_mu.lock();
        defer self.client.writer_mu.unlock();
        const stmt = bind.prepare(
            self.client.conn.db,
            self.client.conn.allocator,
            "DELETE FROM persistent_acks WHERE group_name = ? AND stream_id = ? AND event_id = ?",
        ) catch return error.Sqlite;
        defer bind.finalize(self.client.conn.allocator, stmt);
        _ = bind.bindText(stmt, 1, self.group_name);
        _ = bind.bindText(stmt, 2, self.stream_id);
        _ = bind.bindBlob(stmt, 3, &ev_id);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
    }

    pub fn nack(self: *PersistentSubscription, ev_id: types.Uuid, park: bool) errors_mod.Error!void {
        if (self.closed) return error.SubscriptionClosed;
        if (self.client.closed.load(.seq_cst)) return error.DatabaseClosed;
        self.client.writer_mu.lock();
        defer self.client.writer_mu.unlock();
        const stmt = bind.prepare(
            self.client.conn.db,
            self.client.conn.allocator,
            "UPDATE persistent_acks SET retry_count = retry_count + 1, parked = ? WHERE group_name = ? AND stream_id = ? AND event_id = ?",
        ) catch return error.Sqlite;
        defer bind.finalize(self.client.conn.allocator, stmt);
        _ = bind.bindI64(stmt, 1, if (park) 1 else 0);
        _ = bind.bindText(stmt, 2, self.group_name);
        _ = bind.bindText(stmt, 3, self.stream_id);
        _ = bind.bindBlob(stmt, 4, &ev_id);
        if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
    }

    pub fn close(self: *PersistentSubscription) void {
        if (self.closed) return;
        self.closed = true;
        self.queue.close();
        self.done.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        self.queue.drain();
        self.allocator.free(self.stream_id);
        self.allocator.free(self.group_name);
        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.done);
    }
};

fn runPS(ctx: *PSRunContext) void {
    defer ctx.allocator.destroy(ctx);
    defer ctx.allocator.free(ctx.group_name);
    defer ctx.allocator.free(ctx.stream_id);
    defer _ = ctx.client.active_workers.fetchSub(1, .seq_cst);

    var cursor: u64 = ctx.last_position;
    while (!ctx.done.load(.seq_cst) and !ctx.client.closed.load(.seq_cst)) {
        const res = readStream(
            ctx.client,
            ctx.allocator,
            ctx.client.read_conn,
            ctx.stream_id,
            .{ .from = .{ .revision = cursor }, .direction = .forward, .limit = ctx.client.max_batch_size },
        ) catch |err| {
            if (ctx.client.closed.load(.seq_cst)) break;
            ctx.queue.put(.{ .err = err });
            sleepMs(ctx.client.poll_interval_ms);
            continue;
        };

        const outer = res.events;
        if (outer.len == 0) {
            ctx.allocator.free(outer);
            sleepMs(ctx.client.poll_interval_ms);
            continue;
        }

        for (outer) |ev| {
            if (ctx.done.load(.seq_cst) or ctx.client.closed.load(.seq_cst)) {
                types.freeEvent(ctx.allocator, ev);
                continue;
            }

            ctx.client.writer_mu.lock();
            const stmt = bind.prepare(
                ctx.client.conn.db,
                ctx.client.conn.allocator,
                "INSERT OR REPLACE INTO persistent_acks(group_name, stream_id, event_id, log_position, retry_count, parked, enqueued_at) VALUES (?, ?, ?, ?, 0, 0, ?)",
            ) catch {
                ctx.client.writer_mu.unlock();
                types.freeEvent(ctx.allocator, ev);
                continue;
            };
            _ = bind.bindText(stmt, 1, ctx.group_name);
            _ = bind.bindText(stmt, 2, ctx.stream_id);
            _ = bind.bindBlob(stmt, 3, &ev.event_id);
            _ = bind.bindI64(stmt, 4, @intCast(ev.log_position));
            _ = bind.bindI64(stmt, 5, schema_mod.nowMs());
            const rc = c.sqlite3_step(stmt);
            bind.finalize(ctx.client.conn.allocator, stmt);
            ctx.client.writer_mu.unlock();

            if (rc != c.SQLITE_DONE) {
                types.freeEvent(ctx.allocator, ev);
                ctx.queue.put(.{ .err = error.Sqlite });
                continue;
            }

            ctx.queue.put(.{ .message = .{ .event = ev, .retry_count = 0 } });
            cursor = ev.revision + 1;
        }
        ctx.allocator.free(outer);
    }

    ctx.queue.close();
}

fn sleepMs(ms: u32) void {
    const slice_ns: u64 = std.time.ns_per_ms;
    const deadline_ns: u64 = @as(u64, ms) * slice_ns;
    var elapsed: u64 = 0;
    while (elapsed < deadline_ns) : (elapsed += slice_ns) {
        std.atomic.spinLoopHint();
    }
}

fn stringifyConfig(allocator: std.mem.Allocator, cfg: types.PersistentConfig) ![]u8 {
    var buf: [256]u8 = undefined;
    const slice = try std.fmt.bufPrint(&buf,
        \\{{"resolve_link_tos":{},"extra_statistics":{},"max_retry_count":{d},"check_point_after":{d},"min_check_point_count":{d},"max_check_point_count":{d},"live_buffer_size":{d},"read_batch_size":{d}}}
    , .{
        cfg.resolve_link_tos,
        cfg.extra_statistics,
        cfg.max_retry_count,
        cfg.check_point_after,
        cfg.min_check_point_count,
        cfg.max_check_point_count,
        cfg.live_buffer_size,
        cfg.read_batch_size,
    });
    return allocator.dupe(u8, slice);
}
