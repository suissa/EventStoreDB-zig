//! Security: hostile content (binary blobs with NUL bytes, very
//! long strings, non-UTF-8 bytes, surrogate pairs, combining
//! marks). All of it must be stored verbatim and read back
//! unchanged.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "security: data blobs containing NUL bytes round-trip" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    // 256-byte blob where every byte is zero.
    const data: [256]u8 = .{0} ** 256;
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = &data },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, "s", .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 256), page.events[0].data.len);
    try testing.expectEqualSlices(u8, &data, page.events[0].data);
}

test "security: data blob with mixed NUL and non-NUL bytes" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var data: [512]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = if (i % 3 == 0) 0 else @intCast((i * 37) & 0xff);
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = &data },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, "s", .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqualSlices(u8, &data, page.events[0].data);
}

test "security: stream id accepts UTF-8" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const id = "订单-1234-αβγ-🍕";
    const r = try esdb.appendToStream(c, a, id, .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, id, .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 1), page.events.len);
    try testing.expectEqualStrings(id, page.events[0].stream_id);
}

test "security: very long stream id (1 KB) round-trips" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var id_buf: [1024]u8 = undefined;
    for (&id_buf, 0..) |*b, i| b.* = @intCast(('a' + (i % 26)));
    const r = try esdb.appendToStream(c, a, &id_buf, .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, &id_buf, .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 1), page.events.len);
    try testing.expectEqual(@as(usize, 1024), page.events[0].stream_id.len);
}

test "security: large payload (1 MB) round-trips" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const big = try a.alloc(u8, 1024 * 1024);
    defer a.free(big);
    for (big, 0..) |*b, i| b.* = @intCast((i * 1103515245 + 12345) & 0xff);

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = big },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, "s", .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqual(big.len, page.events[0].data.len);
    try testing.expectEqualSlices(u8, big, page.events[0].data);
}
