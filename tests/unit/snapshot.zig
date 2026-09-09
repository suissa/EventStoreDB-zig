//! Unit tests for `saveSnapshot` and `loadSnapshot`.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

fn freeSnapshot(a: std.mem.Allocator, s: esdb.Snapshot) void {
    a.free(s.stream_id);
    a.free(s.payload);
    if (s.metadata) |m| a.free(m);
}

test "snapshot: save and load round-trip" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 5, "payload", "meta");
    const snap = try esdb.loadSnapshot(c, a, "s", 0);
    defer freeSnapshot(a, snap);
    try testing.expectEqual(@as(u64, 5), snap.revision);
    try testing.expectEqualStrings("payload", snap.payload);
    try testing.expectEqualStrings("meta", snap.metadata.?);
}

test "snapshot: load without metadata returns null" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 1, "p", null);
    const snap = try esdb.loadSnapshot(c, a, "s", 0);
    defer freeSnapshot(a, snap);
    try testing.expect(snap.metadata == null);
}

test "snapshot: latest revision wins" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 1, "v1", null);
    try esdb.saveSnapshot(c, "s", 2, "v2", null);
    try esdb.saveSnapshot(c, "s", 3, "v3", null);
    const snap = try esdb.loadSnapshot(c, a, "s", 0);
    defer freeSnapshot(a, snap);
    try testing.expectEqual(@as(u64, 3), snap.revision);
    try testing.expectEqualStrings("v3", snap.payload);
}

test "snapshot: explicit revision returns that revision" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 1, "v1", null);
    try esdb.saveSnapshot(c, "s", 2, "v2", null);
    const snap = try esdb.loadSnapshot(c, a, "s", 1);
    defer freeSnapshot(a, snap);
    try testing.expectEqual(@as(u64, 1), snap.revision);
    try testing.expectEqualStrings("v1", snap.payload);
}

test "snapshot: missing stream returns SnapshotNotFound" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try testing.expectError(error.SnapshotNotFound, esdb.loadSnapshot(c, a, "missing", 0));
}

test "snapshot: empty stream id is rejected" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.saveSnapshot(c, "", 0, "p", null));
}
