//! Unit tests for projection checkpoint state.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "projection: save then load returns the same state" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveProjectionState(c, "p1", 42, "{\"cursor\":\"abc\"}");
    const loaded = try esdb.loadProjectionState(c, a, "p1");
    defer esdb.freeProjectionState(a, loaded);
    try testing.expectEqualStrings("p1", loaded.name);
    try testing.expectEqual(@as(u64, 42), loaded.last_position);
    try testing.expectEqualStrings("{\"cursor\":\"abc\"}", loaded.state.?);
}

test "projection: overwrite updates last_position" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveProjectionState(c, "p", 1, "v1");
    try esdb.saveProjectionState(c, "p", 2, "v2");
    const loaded = try esdb.loadProjectionState(c, a, "p");
    defer esdb.freeProjectionState(a, loaded);
    try testing.expectEqual(@as(u64, 2), loaded.last_position);
    try testing.expectEqualStrings("v2", loaded.state.?);
}

test "projection: null state is preserved" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveProjectionState(c, "p", 0, null);
    const loaded = try esdb.loadProjectionState(c, a, "p");
    defer esdb.freeProjectionState(a, loaded);
    try testing.expect(loaded.state == null);
}

test "projection: missing name returns NotFound" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.NotFound, esdb.loadProjectionState(c, a, "missing"));
}
