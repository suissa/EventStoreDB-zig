//! Tiny statement binders. We avoid a generic `bindArgs` because
//! the type-tagging gymnastics for `?[]u8` / `*const [N]u8` / `i64`
//! are subtle and easy to break silently. Instead, every call site
//! uses the explicit `bind*` helpers below. Verbose but bulletproof.

const std = @import("std");
const c = @import("c.zig").c;

pub fn prepare(conn_db: *c.sqlite3, allocator: std.mem.Allocator, sql: []const u8) !*c.sqlite3_stmt {
    const sql_z = try allocator.dupeZ(u8, sql);
    errdefer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(conn_db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) {
        return error.PrepareFailed;
    }
    return stmt.?;
}

pub fn finalize(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) void {
    _ = c.sqlite3_finalize(stmt);
    // The sql_z was leaked; we can't free it after prepare because
    // SQLite stores a pointer into it. To keep the API simple we
    // intentionally leak the small SQL string for the life of the
    // program — see note in schema.zig. (Use Arena allocator to
    // bound the lifetime at higher levels.)
    _ = allocator;
}

pub fn bindText(stmt: *c.sqlite3_stmt, idx: c_int, value: []const u8) c_int {
    return c.sqlite3_bind_text(stmt, idx, value.ptr, @intCast(value.len), c.SQLITE_TRANSIENT);
}

pub fn bindOptionalText(stmt: *c.sqlite3_stmt, idx: c_int, value: ?[]const u8) c_int {
    if (value) |v| return c.sqlite3_bind_text(stmt, idx, v.ptr, @intCast(v.len), c.SQLITE_TRANSIENT);
    return c.sqlite3_bind_null(stmt, idx);
}

pub fn bindI64(stmt: *c.sqlite3_stmt, idx: c_int, value: i64) c_int {
    return c.sqlite3_bind_int64(stmt, idx, value);
}

pub fn bindOptionalI64(stmt: *c.sqlite3_stmt, idx: c_int, value: ?i64) c_int {
    if (value) |v| return c.sqlite3_bind_int64(stmt, idx, v);
    return c.sqlite3_bind_null(stmt, idx);
}

pub fn bindBlob(stmt: *c.sqlite3_stmt, idx: c_int, value: []const u8) c_int {
    return c.sqlite3_bind_blob(stmt, idx, value.ptr, @intCast(value.len), c.SQLITE_TRANSIENT);
}

pub fn bindOptionalBlob(stmt: *c.sqlite3_stmt, idx: c_int, value: ?[]const u8) c_int {
    if (value) |v| return c.sqlite3_bind_blob(stmt, idx, v.ptr, @intCast(v.len), c.SQLITE_TRANSIENT);
    return c.sqlite3_bind_null(stmt, idx);
}
