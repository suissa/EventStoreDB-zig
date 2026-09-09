//! Security: the open path is passed to SQLite without escaping,
//! but SQLite itself rejects paths it cannot open. We assert the
//! library surfaces those as `CannotOpenDatabase` and never
//! silently succeeds with a temp/in-memory store.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "security: open with NUL in the path is rejected" {
    try testing.expectError(error.CannotOpenDatabase, esdb.Client.open(common.conn_alloc, .{
        .path = "ok\x00bad",
    }));
}

test "security: open with very long path is rejected (not silently truncated)" {
    var long_path: [8192]u8 = undefined;
    @memset(&long_path, 'a');
    const result = esdb.Client.open(common.conn_alloc, .{ .path = &long_path });
    try testing.expectError(error.CannotOpenDatabase, result);
}
