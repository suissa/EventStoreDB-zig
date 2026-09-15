//! Persistent subscriptions. Each consumer group is a row in
//! `persistent_subscriptions` plus an in-flight table
//! `persistent_acks` that records every delivered-but-not-acked
//! event. The engine advances the group's checkpoint when the
//! caller invokes `ack`; on restart it resumes from the last
//! checkpoint and re-delivers any parked / unacked messages.
//!
//! This module mirrors the queue/wake pattern from `subscribe.zig`
//! because Zig 0.16 removed `std.Thread.Channel` and
//! `std.Thread.Condition`. See the comment at the top of that
//! file for the rationale.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const readStream = @import("read.zig").readStreamOnConn;

const Queue = struct {
    mu: std.atomic.Mutex = .unlocked,
    head: usize = 0,
    tail: usize = 0,
    buf: [4096]PersistentMessageOrErr = undefined,
    closed: bool = false,

    fn put(self: *Queue, item: PersistentMessageOrErr) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        defer self.mu.unlock();
        const next_tail = (self.tail + 1) % self.buf.len;
        if (next_tail == self.head) return; // drop on full
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
};

const PersistentMessageOrErr = union(enum) {
    message: types.PersistentMessage,
    err: errors_mod.Error,
    closed: void,
};

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

pub fn connectPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error || std.Thread.SpawnError)!PersistentSubscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

    // See the matching comment in `subscribe.zig` for the race
    // window we are closing: increment first, re-check `closed`
    // so a concurrent `Client.close()` waits on the worker we
    // are about to spawn instead of tearing the handle down
    // under its feet.
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
    queue.* = .{};
    const done = try allocator.create(std.atomic.Value(bool));
    done.* = std.atomic.Value(bool).init(false);

    const sub = PersistentSubscription{
        .client = self,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, stream_id),
        .group_name = try allocator.dupe(u8, group_name),
        .queue = queue,
        .done = done,
        .last_position = @intCast(last_pos),
    };
    const thread = try std.Thread.spawn(.{}, runPS, .{&sub});
    sub.thread = thread;
    return sub;
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
        _ = c.sqlite3_step(stmt);
    }

    pub fn nack(self: *PersistentSubscription, ev_id: types.Uuid, park: bool) errors_mod.Error!void {
        if (self.closed) return error.SubscriptionClosed;
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
        _ = c.sqlite3_step(stmt);
    }

    pub fn close(self: *PersistentSubscription) void {
        if (self.closed) return;
        self.closed = true;
        self.queue.close();
        self.done.store(true, .seq_cst);
        if (self.thread) |t| t.join();
        self.allocator.free(self.stream_id);
        self.allocator.free(self.group_name);
        self.allocator.destroy(self.queue);
        self.allocator.destroy(self.done);
    }
};

fn runPS(sub: *PersistentSubscription) void {
    // Counter bookkeeping for `Client.close()`
    // determinism — see the matching comment in
    // `subscribe.zig::runStream`.
    defer _ = sub.client.active_workers.fetchSub(1, .seq_cst);
    var cursor: u64 = sub.last_position;
    // Persistent subscription reads also go through
    // `Client.read_conn` so that the persistent worker does
    // not contend with `appendToStream` on the writer
    // connection. See `subscribe.zig::runStream` for the
    // matching rationale.
    while (!sub.done.load(.seq_cst)) {
        const res = readStream(
            sub.client,
            sub.allocator,
            sub.client.read_conn,
            sub.stream_id,
            .{ .from = .{ .revision = cursor }, .direction = .forward, .limit = sub.client.max_batch_size },
        ) catch |err| {
            sub.queue.put(.{ .err = err });
            sleepMs(sub.client.poll_interval_ms);
            continue;
        };

        if (res.events.len == 0) {
            sleepMs(sub.client.poll_interval_ms);
            continue;
        }

        for (res.events) |ev| {
            sub.client.writer_mu.lock();
            defer sub.client.writer_mu.unlock();
            const stmt = bind.prepare(
                sub.client.conn.db,
                sub.client.conn.allocator,
                "INSERT OR REPLACE INTO persistent_acks(group_name, stream_id, event_id, log_position, retry_count, parked, enqueued_at) VALUES (?, ?, ?, ?, 0, 0, ?)",
            ) catch continue;
            defer bind.finalize(sub.client.conn.allocator, stmt);
            _ = bind.bindText(stmt, 1, sub.group_name);
            _ = bind.bindText(stmt, 2, sub.stream_id);
            _ = bind.bindBlob(stmt, 3, &ev.event_id);
            _ = bind.bindI64(stmt, 4, @intCast(@as(i64, @intCast(ev.log_position))));
            _ = bind.bindI64(stmt, 5, schema_mod.nowMs());
            _ = c.sqlite3_step(stmt);

            sub.queue.put(.{ .message = .{ .event = ev, .retry_count = 0 } });
            cursor = ev.revision + 1;
        }

        if (res.events.len == 0) {
            sleepMs(sub.client.poll_interval_ms);
        }
    }
}

fn sleepMs(ms: u32) void {
    const slice_ns: u64 = std.time.ns_per_ms;
    const deadline_ns: u64 = @as(u64, ms) * slice_ns;
    var elapsed: u64 = 0;
    while (elapsed < deadline_ns) : (elapsed += slice_ns) {
        std.atomic.spinLoopHint();
    }
}

/// Hand-rolled `PersistentConfig` -> JSON encoder. The stdlib
/// `std.json` API is unstable in Zig 0.16; this struct has six
/// fields and we want a stable blob so the persistent group row
/// can be decoded back to a `PersistentConfig` later. A
/// third-party caller that wants the real JSON can wrap this.
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
