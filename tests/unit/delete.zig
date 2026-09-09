//! Unit tests for `deleteStream` and the soft-tombstone behavior.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "delete: stream with no events is tombstoned and excluded from lists" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    // Create an empty stream by appending + reading its metadata,
    // or by listing after creating then deleting.
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);

    try esdb.deleteStream(c, "s");

    const info = try esdb.getStreamInfo(c, a, "s");
    defer a.free(info.stream_id);
    try testing.expect(info.deleted);

    // Append to a tombstoned stream should fail.
    try testing.expectError(error.StreamTombstoned, esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "2" },
    }));
}

test "delete: readStream on tombstoned stream returns StreamTombstoned" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
    });
    defer esdb.freeEvents(a, r.events);

    try esdb.deleteStream(c, "s");

    // Per the EventStoreDB protocol, reading a tombstoned
    // stream is a hard error: the history is preserved in
    // the events table but the stream metadata says "deleted".
    try testing.expectError(error.StreamTombstoned, esdb.readStream(c, a, "s", .{}));
}

test "delete: invalid empty stream id is rejected" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.deleteStream(c, ""));
}
