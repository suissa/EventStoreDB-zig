//! Test suite. Run with `zig build test`.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");

fn newClient(allocator: std.mem.Allocator) !*esdb.Client {
    const c = try esdb.Client.open(allocator, .{ .path = ":memory:" });
    errdefer c.close();
    return c;
}

test "open and close" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();
    try testing.expectEqual(@as(u64, 0), c.lastLogPosition());
}

test "append and read stream" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    const res = try c.appendToStream(a, "s1", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "{\"v\":1}" },
        .{ .event_type = "B", .data = "{\"v\":2}" },
    });
    defer a.free(res.events);
    try testing.expectEqual(@as(u64, 2), res.next_revision);
    try testing.expectEqual(@as(u64, 2), res.log_position);
    try testing.expectEqual(@as(u64, 2), c.lastLogPosition());

    const page = try c.readStream(a, "s1", .{});
    defer a.free(page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
    try testing.expectEqualStrings("A", page.events[0].event_type);
    try testing.expectEqualStrings("B", page.events[1].event_type);
    try testing.expect(page.is_end_of_stream);
}

test "append with expected revision" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    // Stream does not exist yet.
    try testing.expectError(error.WrongExpectedVersion, c.appendToStream(a, "s", .{ .expected_revision = .stream_exists }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));

    // Create it.
    _ = try c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    });

    // Stream already exists -> NoStream fails.
    try testing.expectError(error.WrongExpectedVersion, c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));

    // Stale revision fails.
    try testing.expectError(error.WrongExpectedVersion, c.appendToStream(a, "s", .{ .expected_revision = .{ .revision = 0 } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));
}

test "idempotency" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    var id: esdb.Uuid = .{0} ** 16;
    id[0] = 1;
    const ev = esdb.EventData{ .event_id = id, .event_type = "T", .data = "{}" };

    const first = try c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev});
    defer a.free(first.events);
    const second = try c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev});
    defer a.free(second.events);
    try testing.expectEqual(@as(usize, 1), second.events.len);
    try testing.expectEqual(first.events[0].log_position, second.events[0].log_position);
    try testing.expectEqual(@as(u64, 1), c.lastLogPosition());
}

test "read all" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    _ = try c.appendToStream(a, "a", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    _ = try c.appendToStream(a, "b", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "Y", .data = "2" },
    });

    const page = try c.readAll(a, .{});
    defer a.free(page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
}

test "snapshot round trip" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    try c.saveSnapshot("s", 5, "payload", "meta");
    const snap = try c.loadSnapshot(a, "s", 0);
    defer a.free(snap.payload);
    defer a.free(snap.metadata.?);
    try testing.expectEqual(@as(u64, 5), snap.revision);
    try testing.expectEqualStrings("payload", snap.payload);

    try testing.expectError(error.SnapshotNotFound, c.loadSnapshot(a, "missing", 0));
}

test "delete stream" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    _ = try c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "T", .data = "1" },
    });

    try c.deleteStream("s");
    try testing.expectError(error.StreamTombstoned, c.appendToStream(a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "T", .data = "2" },
    }));
}

test "uuid v4 layout" {
    var u = esdb.uuid.newV4();
    // Version nibble must be 0100xxxx.
    try testing.expectEqual(@as(u8, 0x40), u[6] & 0xF0);
    // Variant bits must be 10xxxxxx.
    try testing.expectEqual(@as(u8, 0x80), u[8] & 0xC0);
    _ = u;
}
