//! Stream metadata and projector state: listing, soft-delete,
//! and projection-checkpoint persistence.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;

pub fn listStreams(
    self: *Client,
    allocator: std.mem.Allocator,
    limit: u32,
    offset: u32,
) (errors_mod.Error || std.mem.Allocator.Error)![]types.StreamInfo {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    const lim: u32 = if (limit == 0) 100 else limit;
    const stmt = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "SELECT stream_id, revision, max_count, truncate_before, deleted_at FROM streams ORDER BY updated_at DESC LIMIT ? OFFSET ?",
    );
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindI64(stmt, 1, @intCast(@as(i64, @intCast(lim))));
    _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(offset))));

    var out = try std.ArrayList(types.StreamInfo).initCapacity(allocator, 8);
    defer out.deinit();
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const id = try dupText(allocator, stmt, 0);
        const rev: u64 = @intCast(c.sqlite3_column_int64(stmt, 1));
        const max: i64 = c.sqlite3_column_int64(stmt, 2);
        const trunc: u64 = @intCast(c.sqlite3_column_int64(stmt, 3));
        const deleted = c.sqlite3_column_type(stmt, 4) != c.SQLITE_NULL;
        try out.append(.{
            .stream_id = id,
            .revision = rev,
            .max_count = max,
            .truncate_before = trunc,
            .deleted = deleted,
        });
    }
    return out.toOwnedSlice();
}

pub fn getStreamInfo(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error)!types.StreamInfo {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;
    const stmt = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "SELECT revision, max_count, truncate_before, deleted_at FROM streams WHERE stream_id = ?",
    );
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StreamNotFound;
    return .{
        .stream_id = try allocator.dupe(u8, stream_id),
        .revision = @intCast(c.sqlite3_column_int64(stmt, 0)),
        .max_count = c.sqlite3_column_int64(stmt, 1),
        .truncate_before = @intCast(c.sqlite3_column_int64(stmt, 2)),
        .deleted = c.sqlite3_column_type(stmt, 3) != c.SQLITE_NULL,
    };
}

pub fn setStreamMetadata(
    self: *Client,
    stream_id: []const u8,
    meta: types.StreamMetadata,
) (errors_mod.Error || std.mem.Allocator.Error)!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    self.writer_mu.lock();
    defer self.writer_mu.unlock();

    if (c.sqlite3_exec(self.conn.db, "BEGIN IMMEDIATE", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
    defer _ = c.sqlite3_exec(self.conn.db, "ROLLBACK", null, null, null);

    const s1 = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "INSERT OR IGNORE INTO streams(stream_id, stream_type, revision, created_at, updated_at) VALUES (?, 0, -1, ?, ?)",
    );
    defer bind.finalize(self.conn.allocator, s1);
    _ = bind.bindText(s1, 1, stream_id);
    _ = bind.bindI64(s1, 2, schema_mod.nowMs());
    _ = bind.bindI64(s1, 3, schema_mod.nowMs());
    _ = c.sqlite3_step(s1);

    const s2 = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "UPDATE streams SET max_count = CASE WHEN ? > 0 THEN ? ELSE max_count END, truncate_before = CASE WHEN ? > 0 THEN ? ELSE truncate_before END, custom_metadata = COALESCE(?, custom_metadata), updated_at = ? WHERE stream_id = ?",
    );
    defer bind.finalize(self.conn.allocator, s2);
    _ = bind.bindI64(s2, 1, meta.max_count);
    _ = bind.bindI64(s2, 2, meta.max_count);
    _ = bind.bindI64(s2, 3, @intCast(@as(i64, @intCast(meta.truncate_before))));
    _ = bind.bindI64(s2, 4, @intCast(@as(i64, @intCast(meta.truncate_before))));
    _ = bind.bindOptionalBlob(s2, 5, meta.custom_metadata);
    _ = bind.bindI64(s2, 6, schema_mod.nowMs());
    _ = bind.bindText(s2, 7, stream_id);
    _ = c.sqlite3_step(s2);

    if (c.sqlite3_exec(self.conn.db, "COMMIT", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
}

pub fn deleteStream(self: *Client, stream_id: []const u8) errors_mod.Error!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;
    self.writer_mu.lock();
    defer self.writer_mu.unlock();
    if (c.sqlite3_exec(self.conn.db, "BEGIN IMMEDIATE", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
    defer _ = c.sqlite3_exec(self.conn.db, "ROLLBACK", null, null, null);

    const s1 = bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "INSERT OR IGNORE INTO streams(stream_id, stream_type, revision, created_at, updated_at, deleted_at) VALUES (?, 0, -1, ?, ?, ?)",
    ) catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s1);
    _ = bind.bindText(s1, 1, stream_id);
    _ = bind.bindI64(s1, 2, schema_mod.nowMs());
    _ = bind.bindI64(s1, 3, schema_mod.nowMs());
    _ = bind.bindI64(s1, 4, schema_mod.nowMs());
    _ = c.sqlite3_step(s1);

    const s2 = bind.prepare(self.conn.db, self.conn.allocator, "UPDATE streams SET deleted_at = ?, updated_at = ? WHERE stream_id = ?") catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, s2);
    _ = bind.bindI64(s2, 1, schema_mod.nowMs());
    _ = bind.bindI64(s2, 2, schema_mod.nowMs());
    _ = bind.bindText(s2, 3, stream_id);
    _ = c.sqlite3_step(s2);

    if (c.sqlite3_exec(self.conn.db, "COMMIT", null, null, null) != c.SQLITE_OK) {
        return error.Sqlite;
    }
}

pub fn saveProjectionState(
    self: *Client,
    name: []const u8,
    position: u64,
    state: ?[]const u8,
) errors_mod.Error!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    const sql =
        \\INSERT INTO projection_checkpoints(projection_name, last_processed_position, state, updated_at)
        \\VALUES (?, ?, ?, ?)
        \\ON CONFLICT(projection_name) DO UPDATE SET
        \\  last_processed_position = excluded.last_processed_position,
        \\  state = excluded.state,
        \\  updated_at = excluded.updated_at
    ;
    const stmt = bind.prepare(self.conn.db, self.conn.allocator, sql) catch return error.Sqlite;
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, name);
    _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(position))));
    _ = bind.bindOptionalBlob(stmt, 3, state);
    _ = bind.bindI64(stmt, 4, schema_mod.nowMs());
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
}

pub fn loadProjectionState(
    self: *Client,
    allocator: std.mem.Allocator,
    name: []const u8,
) (errors_mod.Error || std.mem.Allocator.Error)!types.ProjectionState {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    const stmt = try bind.prepare(
        self.conn.db,
        self.conn.allocator,
        "SELECT projection_name, last_processed_position, state FROM projection_checkpoints WHERE projection_name = ?",
    );
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, name);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.StreamNotFound;

    const nm = try dupText(allocator, stmt, 0);
    const pos: u64 = @intCast(c.sqlite3_column_int64(stmt, 1));
    const st: ?[]const u8 = if (c.sqlite3_column_type(stmt, 2) == c.SQLITE_NULL) null else try dupBlob(allocator, stmt, 2);
    return .{ .name = nm, .last_position = pos, .state = st };
}

fn dupText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) std.mem.Allocator.Error![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, col);
    const ptr: [*]const u8 = @ptrCast(c.sqlite3_column_text(stmt, col));
    return allocator.dupe(u8, ptr[0..@intCast(len)]);
}

fn dupBlob(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) std.mem.Allocator.Error![]const u8 {
    const len = c.sqlite3_column_bytes(stmt, col);
    const ptr: [*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, col));
    return allocator.dupe(u8, ptr[0..@intCast(len)]);
}
