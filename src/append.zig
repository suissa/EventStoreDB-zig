//! `appendToStream` — appends one or more events to a stream
//! in a single transaction, with idempotency by `event_id` and
//! optimistic concurrency via `ExpectedRevision`.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const uuid_mod = @import("uuid.zig");
const Client = @import("client.zig").Client;

/// Append the given events to a single stream. The stream is
/// created if it does not exist (subject to ExpectedRevision).
///
/// On a duplicate `event_id`, the existing event is reused
/// without creating a new row. If ALL events in the batch are
/// duplicates, the call returns successfully without
/// enforcing `ExpectedRevision` — this matches the at-least-once
/// idempotent contract of the official EventStoreDB server.
pub fn appendToStream(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    opts: types.AppendOptions,
    events: []const types.EventData,
) (errors_mod.Error || std.mem.Allocator.Error)!types.AppendResult {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;
    if (events.len == 0) return error.InvalidArgument;

    // Normalize: fill in missing event IDs with a fresh UUIDv4.
    var prepared = try allocator.alloc(types.EventData, events.len);
    defer allocator.free(prepared);
    for (events, 0..) |e, i| {
        prepared[i] = e;
        if (std.mem.allEqual(u8, &e.event_id, 0)) {
            prepared[i].event_id = uuid_mod.newV4();
        }
    }

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    // Idempotency pre-check: classify each event as existing or
    // new, and look up the existing ones.
    var existing_by_id = std.AutoHashMap(types.Uuid, types.RecordedEvent).init(allocator);
    defer existing_by_id.deinit();
    var all_existing = true;

    for (prepared) |e| {
        if (try lookupCommitted(self, allocator, e.event_id)) |existing| {
            if (!std.mem.eql(u8, existing.stream_id, stream_id)) {
                return error.InvalidArgument; // event_id reused across streams
            }
            try existing_by_id.put(e.event_id, existing);
        } else {
            all_existing = false;
        }
    }

    // All events are idempotent: skip the version check and
    // return the existing rows directly. No transaction needed.
    if (all_existing and existing_by_id.count() == prepared.len) {
        var recorded = try allocator.alloc(types.RecordedEvent, prepared.len);
        errdefer allocator.free(recorded);
        var last_log: u64 = 0;
        var last_tx: u64 = 0;
        for (prepared, 0..) |e, i| {
            const rec = existing_by_id.get(e.event_id).?;
            recorded[i] = rec;
            if (rec.log_position > last_log) last_log = rec.log_position;
            if (rec.transaction_position > last_tx) last_tx = rec.transaction_position;
        }
        return .{
            .next_revision = try currentRevision(self, stream_id) + 1,
            .log_position = last_log,
            .transaction_position = last_tx,
            .events = recorded,
        };
    }

    // Some events are new. Open a transaction.
    const conn = self.conn;
    const begin_rc = c.sqlite3_exec(conn.db, "BEGIN IMMEDIATE", null, null, null);
    if (begin_rc != c.SQLITE_OK) return error.Sqlite;
    defer _ = c.sqlite3_exec(conn.db, "ROLLBACK", null, null, null);

    const curRev = try loadStream(conn, stream_id);
    if (curRev.tombstoned) return error.StreamTombstoned;
    try checkExpectedRevision(opts.expected_revision, curRev.revision, stream_id);

    const first_log = try nextLogPosition(conn);
    const tx_pos = try nextTxPosition(conn);

    var recorded = try std.ArrayList(types.RecordedEvent).initCapacity(allocator, prepared.len);
    defer recorded.deinit();

    var log_pos: u64 = first_log;
    var new_rev: i64 = curRev.revision;
    for (prepared) |e| {
        if (existing_by_id.get(e.event_id)) |existing| {
            try recorded.append(existing);
            continue;
        }
        new_rev += 1;
        const rev: u64 = @intCast(new_rev);

        const rc = insertEvent(
            conn,
            e.event_id,
            stream_id,
            rev,
            log_pos,
            tx_pos,
            e.event_type,
            e.data,
            e.metadata,
            schema_mod.nowMs(),
        );
        if (rc != c.SQLITE_OK) return error.Sqlite;

        const rc2 = insertCommittedId(
            conn,
            e.event_id,
            log_pos,
            stream_id,
            rev,
            schema_mod.nowMs(),
        );
        if (rc2 != c.SQLITE_OK) return error.Sqlite;

        try recorded.append(.{
            .event_id = e.event_id,
            .stream_id = stream_id,
            .event_type = e.event_type,
            .data = e.data,
            .metadata = e.metadata,
            .revision = rev,
            .log_position = log_pos,
            .transaction_position = tx_pos,
        });
        log_pos += 1;
    }

    try upsertStreamAfter(conn, stream_id, @intCast(new_rev));

    const commit_rc = c.sqlite3_exec(conn.db, "COMMIT", null, null, null);
    if (commit_rc != c.SQLITE_OK) {
        return error.Sqlite;
    }

    if (recorded.items.len > 0) {
        const last = recorded.items[recorded.items.len - 1];
        self.last_log_pos.store(last.log_position, .seq_cst);
    }
    self.waker.signal();

    return .{
        .next_revision = @as(u64, @intCast(new_rev + 1)),
        .log_position = if (recorded.items.len > 0) recorded.items[recorded.items.len - 1].log_position else 0,
        .transaction_position = if (recorded.items.len > 0) recorded.items[recorded.items.len - 1].transaction_position else 0,
        .events = try recorded.toOwnedSlice(),
    };
}

const StreamState = struct {
    revision: i64,
    tombstoned: bool,
};

fn loadStream(conn: *schema_mod.Connection, stream_id: []const u8) !StreamState {
    const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT revision, deleted_at FROM streams WHERE stream_id = ?");
    defer bind.finalize(conn.allocator, stmt);
    if (bind.bindText(stmt, 1, stream_id) != c.SQLITE_OK) return error.Sqlite;
    const rc = c.sqlite3_step(stmt);
    if (rc == c.SQLITE_ROW) {
        const rev = c.sqlite3_column_int64(stmt, 0);
        const deleted: c_int = c.sqlite3_column_type(stmt, 1);
        return .{ .revision = rev, .tombstoned = deleted != c.SQLITE_NULL };
    }
    return .{ .revision = -1, .tombstoned = false };
}

fn currentRevision(self: *Client, stream_id: []const u8) !i64 {
    const state = try loadStream(self.conn, stream_id);
    return state.revision;
}

fn upsertStreamAfter(conn: *schema_mod.Connection, stream_id: []const u8, new_rev: i64) !void {
    const sql =
        \\INSERT INTO streams(stream_id, stream_type, revision, max_count, truncate_before, created_at, updated_at)
        \\VALUES (?, 0, ?, 0, 0, ?, ?)
        \\ON CONFLICT(stream_id) DO UPDATE SET
        \\  revision = excluded.revision,
        \\  updated_at = excluded.updated_at
    ;
    const stmt = try bind.prepare(conn.db, conn.allocator, sql);
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    _ = bind.bindI64(stmt, 2, new_rev);
    _ = bind.bindI64(stmt, 3, schema_mod.nowMs());
    _ = bind.bindI64(stmt, 4, schema_mod.nowMs());
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
}

fn nextLogPosition(conn: *schema_mod.Connection) !u64 {
    const v = try conn.queryScalarI64("SELECT COALESCE(MAX(log_position), 0) + 1 FROM events");
    return @intCast(v);
}

fn nextTxPosition(conn: *schema_mod.Connection) !u64 {
    const v = try conn.queryScalarI64("SELECT COALESCE(MAX(transaction_position), 0) + 1 FROM events");
    return @intCast(v);
}

fn checkExpectedRevision(exp: types.ExpectedRevision, current: i64, _: []const u8) errors_mod.Error!void {
    switch (exp) {
        .no_stream => if (current >= 0) return error.WrongExpectedVersion,
        .stream_exists => if (current < 0) return error.WrongExpectedVersion,
        .revision => |want| if (current != @as(i64, @intCast(want))) return error.WrongExpectedVersion,
        .any => {},
    }
}

fn insertEvent(
    conn: *schema_mod.Connection,
    event_id: types.Uuid,
    stream_id: []const u8,
    revision: u64,
    log_pos: u64,
    tx_pos: u64,
    event_type: []const u8,
    data: []const u8,
    metadata: ?[]const u8,
    created_at: i64,
) c_int {
    const sql =
        \\INSERT INTO events(event_id, stream_id, event_number, log_position, transaction_position, event_type, data, metadata, created_at)
        \\VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ;
    const stmt = bind.prepare(conn.db, conn.allocator, sql) catch return c.SQLITE_MISUSE;
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindBlob(stmt, 1, &event_id);
    _ = bind.bindText(stmt, 2, stream_id);
    _ = bind.bindI64(stmt, 3, @intCast(@as(i64, @intCast(revision))));
    _ = bind.bindI64(stmt, 4, @intCast(@as(i64, @intCast(log_pos))));
    _ = bind.bindI64(stmt, 5, @intCast(@as(i64, @intCast(tx_pos))));
    _ = bind.bindText(stmt, 6, event_type);
    _ = bind.bindBlob(stmt, 7, data);
    _ = bind.bindOptionalBlob(stmt, 8, metadata);
    _ = bind.bindI64(stmt, 9, created_at);
    return c.sqlite3_step(stmt);
}

fn insertCommittedId(
    conn: *schema_mod.Connection,
    event_id: types.Uuid,
    log_pos: u64,
    stream_id: []const u8,
    revision: u64,
    committed_at: i64,
) c_int {
    const sql =
        \\INSERT INTO committed_event_ids(event_id, log_position, stream_id, event_number, committed_at)
        \\VALUES (?, ?, ?, ?, ?)
    ;
    const stmt = bind.prepare(conn.db, conn.allocator, sql) catch return c.SQLITE_MISUSE;
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindBlob(stmt, 1, &event_id);
    _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(log_pos))));
    _ = bind.bindText(stmt, 3, stream_id);
    _ = bind.bindI64(stmt, 4, @intCast(@as(i64, @intCast(revision))));
    _ = bind.bindI64(stmt, 5, committed_at);
    return c.sqlite3_step(stmt);
}

fn lookupCommitted(self: *Client, allocator: std.mem.Allocator, id: types.Uuid) !?types.RecordedEvent {
    const conn = self.conn;
    const sql =
        \\SELECT e.event_id, e.stream_id, e.event_number, e.log_position, e.transaction_position,
        \\       e.event_type, e.data, e.metadata
        \\FROM committed_event_ids c
        \\JOIN events e ON e.event_id = c.event_id
        \\WHERE c.event_id = ?
    ;
    const stmt = try bind.prepare(conn.db, conn.allocator, sql);
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindBlob(stmt, 1, &id);
    const rc = c.sqlite3_step(stmt);
    if (rc != c.SQLITE_ROW) return null;

    const ev_id: types.Uuid = blk: {
        var out: types.Uuid = undefined;
        const len = c.sqlite3_column_bytes(stmt, 0);
        if (len != 16) return error.Sqlite;
        const src: [*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, 0));
        @memcpy(&out, src[0..16]);
        break :blk out;
    };
    const stream_id = try dupText(allocator, stmt, 1);
    const event_type = try dupText(allocator, stmt, 5);
    const data = try dupBlob(allocator, stmt, 6);
    const metadata: ?[]const u8 = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else try dupBlob(allocator, stmt, 7);
    return .{
        .event_id = ev_id,
        .stream_id = stream_id,
        .event_type = event_type,
        .data = data,
        .metadata = metadata,
        .revision = @intCast(c.sqlite3_column_int64(stmt, 2)),
        .log_position = @intCast(c.sqlite3_column_int64(stmt, 3)),
        .transaction_position = @intCast(c.sqlite3_column_int64(stmt, 4)),
    };
}

fn dupText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, col);
    const ptr: [*]const u8 = @ptrCast(c.sqlite3_column_text(stmt, col));
    return allocator.dupe(u8, ptr[0..@intCast(len)]);
}

fn dupBlob(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) ![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, col);
    const ptr: [*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, col));
    return allocator.dupe(u8, ptr[0..@intCast(len)]);
}
