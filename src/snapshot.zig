//! Snapshot store. Saves and loads per-stream aggregate
//! snapshots. Use the standard CQRS pattern: load the latest
//! snapshot for a stream, then replay only events strictly
//! above its revision.

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;

pub fn saveSnapshot(
    self: *Client,
    stream_id: []const u8,
    revision: u64,
    payload: []const u8,
    metadata: ?[]const u8,
) (errors_mod.Error || std.mem.Allocator.Error)!void {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    const sql =
        \\INSERT INTO snapshots(stream_id, revision, payload, metadata, created_at)
        \\VALUES (?, ?, ?, ?, ?)
        \\ON CONFLICT(stream_id, revision) DO UPDATE SET
        \\  payload = excluded.payload,
        \\  metadata = excluded.metadata,
        \\  created_at = excluded.created_at
    ;
    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(revision))));
    _ = bind.bindBlob(stmt, 3, payload);
    _ = bind.bindOptionalBlob(stmt, 4, metadata);
    _ = bind.bindI64(stmt, 5, schema_mod.nowMs());
    if (c.sqlite3_step(stmt) != c.SQLITE_DONE) return error.Sqlite;
}

pub fn loadSnapshot(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    revision: u64,
) (errors_mod.Error || std.mem.Allocator.Error)!types.Snapshot {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    const sql = if (revision == 0)
        \\SELECT stream_id, revision, payload, metadata
        \\FROM snapshots WHERE stream_id = ?
        \\ORDER BY revision DESC LIMIT 1
    else
        \\SELECT stream_id, revision, payload, metadata
        \\FROM snapshots WHERE stream_id = ? AND revision = ?
    ;

    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    if (revision != 0) _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(revision))));

    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return error.SnapshotNotFound;

    const s_id = try dupText(allocator, stmt, 0);
    const rev = c.sqlite3_column_int64(stmt, 1);
    const payload = try dupBlob(allocator, stmt, 2);
    const metadata: ?[]const u8 = if (c.sqlite3_column_type(stmt, 3) == c.SQLITE_NULL) null else try dupBlob(allocator, stmt, 3);

    return .{
        .stream_id = s_id,
        .revision = @intCast(rev),
        .payload = payload,
        .metadata = metadata,
    };
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
