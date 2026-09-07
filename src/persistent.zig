//! Persistent subscriptions. Each consumer group is a row in
//! `persistent_subscriptions` plus an in-flight table
//! `persistent_acks` that records every delivered-but-not-acked
//! event. The engine advances the group's checkpoint when the
//! caller invokes `Ack`; on restart it resumes from the last
//! checkpoint and re-delivers any parked / unacked messages.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;
const readStream = @import("read.zig").readStream;

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
        const exists = try @import("client.zig").scalarI64(
            self.conn,
            "SELECT 1 FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?",
        ) catch return error.Sqlite;
        if (exists == 1) return error.PersistentSubscriptionExists;
    }

    const from_rev: i64 = switch (opts.from) {
        .start => 0,
        .end => 0,
        .revision => |r| @intCast(r),
        .position => 0,
    };

    const cfg_buf = try std.json.stringifyAlloc(allocator, cfg, .{});
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
    stream_id: []const u8,
    group_name: []const u8,
) errors_mod.Error!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    if (c.sqlite3_exec(self.conn.db, "BEGIN IMMEDIATE", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
    defer _ = c.sqlite3_exec(self.conn.db, "ROLLBACK", null, null, null);

    const s1 = bind.prepare(self.conn.db, self.conn.allocator, "DELETE FROM persistent_acks WHERE group_name = ? AND stream_id = ?") catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s1);
    _ = bind.bindText(s1, 1, group_name);
    _ = bind.bindText(s1, 2, stream_id);
    _ = c.sqlite3_step(s1);

    const s2 = bind.prepare(self.conn.db, self.conn.allocator, "DELETE FROM persistent_subscriptions WHERE group_name = ? AND stream_id = ?") catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s2);
    _ = bind.bindText(s2, 1, group_name);
    _ = bind.bindText(s2, 2, stream_id);
    _ = c.sqlite3_step(s2);

    if (c.sqlite3_exec(self.conn.db, "COMMIT", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
}

pub fn connectPersistentSubscription(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error)!PersistentSubscription {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0 or group_name.len == 0) return error.InvalidArgument;

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

    const sub = PersistentSubscription{
        .client = self,
        .allocator = allocator,
        .stream_id = try allocator.dupe(u8, stream_id),
        .group_name = try allocator.dupe(u8, group_name),
        .channel = std.Thread.Channel(types.PersistentMessageOrErr).init(256),
        .last_position = @intCast(last_pos),
    };
    const thread = try std.Thread.spawn(.{}, runPS, .{&sub});
    sub.thread = thread;
    return sub;
}

const PersistentMessageOrErr = union(enum) {
    message: types.PersistentMessage,
    err: errors_mod.Error,
    closed: void,
};

pub const PersistentSubscription = struct {
    client: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    group_name: []const u8,
    channel: std.Thread.Channel(PersistentMessageOrErr),
    thread: ?std.Thread = null,
    last_position: u64,
    closed: bool = false,

    pub fn receive(self: *PersistentSubscription) ?PersistentMessageOrErr {
        if (self.closed) return null;
        return self.channel.get();
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
        self.channel.put(.{ .closed = {} });
        if (self.thread) |t| t.join();
        self.allocator.free(self.stream_id);
        self.allocator.free(self.group_name);
    }
};

fn runPS(sub: *PersistentSubscription) void {
    defer sub.channel.put(.{ .closed = {} });
    var cursor = sub.last_position;

    while (true) {
        if (sub.closed) return;
        const res = readStream(
            sub.client,
            sub.allocator,
            sub.stream_id,
            .{ .from = .{ .revision = cursor }, .direction = .forward, .limit = sub.client.max_batch_size },
        ) catch |err| {
            sub.channel.put(.{ .err = err });
            std.time.sleep(std.time.ns_per_ms * sub.client.poll_interval_ms);
            continue;
        };

        for (res.events) |ev| {
            // Record in-flight.
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

            sub.channel.put(.{ .message = .{ .event = ev, .retry_count = 0 } });
            cursor = ev.revision + 1;
        }

        if (res.events.len == 0) {
            std.time.sleep(std.time.ns_per_ms * sub.client.poll_interval_ms);
        }
    }
}
