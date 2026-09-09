//! Unit tests for stream metadata: `listStreams`, `getStreamInfo`,
//! and `setStreamMetadata`.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "listStreams: empty store returns no streams" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const list = try esdb.listStreams(c, a, 100, 0);
    defer esdb.freeStreamInfo(a, list);
    try testing.expectEqual(@as(usize, 0), list.len);
}

test "listStreams: returns every non-empty stream" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "alpha", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    {
        const r = try esdb.appendToStream(c, a, "beta", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    const list = try esdb.listStreams(c, a, 100, 0);
    defer esdb.freeStreamInfo(a, list);
    try testing.expectEqual(@as(usize, 2), list.len);
}

test "getStreamInfo: missing stream returns StreamNotFound" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.StreamNotFound, esdb.getStreamInfo(c, a, "missing"));
}

test "getStreamInfo: returns revision and metadata" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "2" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    try esdb.setStreamMetadata(c, "s", .{ .max_count = 100, .custom_metadata = "{\"owner\":\"x\"}" });

    const info = try esdb.getStreamInfo(c, a, "s");
    defer a.free(info.stream_id);
    // After 2 appends, the last 0-based revision is 1.
    try testing.expectEqual(@as(u64, 1), info.revision);
    try testing.expectEqual(@as(i64, 100), info.max_count);
    try testing.expect(!info.deleted);
}

test "setStreamMetadata: overwrites max_count and custom_metadata" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    try esdb.setStreamMetadata(c, "s", .{ .max_count = 10, .truncate_before = 5, .custom_metadata = "v1" });
    try esdb.setStreamMetadata(c, "s", .{ .max_count = 50, .custom_metadata = "v2" });

    const info = try esdb.getStreamInfo(c, a, "s");
    defer a.free(info.stream_id);
    try testing.expectEqual(@as(i64, 50), info.max_count);
}

test "setStreamMetadata: empty stream id is rejected" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.setStreamMetadata(c, "", .{}));
}
