//! Unit tests for `readStream` and `readAll`.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "readStream: from start forward returns all events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
        .{ .event_type = "C", .data = "3" },
    });
    defer esdb.freeEvents(a, res.events);

    const page = try esdb.readStream(c, a, "s", .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 3), page.events.len);
    try testing.expect(page.is_end_of_stream);
    try testing.expectEqualStrings("A", page.events[0].event_type);
    try testing.expectEqualStrings("C", page.events[2].event_type);
}

test "readStream: backward returns events in reverse" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
        .{ .event_type = "C", .data = "3" },
    });
    defer esdb.freeEvents(a, res.events);

    const page = try esdb.readStream(c, a, "s", .{ .direction = .backward });
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 3), page.events.len);
    try testing.expectEqualStrings("C", page.events[0].event_type);
    try testing.expectEqualStrings("A", page.events[2].event_type);
}

test "readStream: from end forward on empty stream yields no events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const page = try esdb.readStream(c, a, "missing", .{ .from = .{ .end = {} } });
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 0), page.events.len);
}

test "readStream: limit caps the page" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
        .{ .event_type = "C", .data = "3" },
    });
    defer esdb.freeEvents(a, res.events);
    const page = try esdb.readStream(c, a, "s", .{ .limit = 2 });
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
    try testing.expect(!page.is_end_of_stream);
}

test "readStream: from revision skips earlier events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
        .{ .event_type = "C", .data = "3" },
    });
    defer esdb.freeEvents(a, res.events);
    const page = try esdb.readStream(c, a, "s", .{ .from = .{ .revision = 1 } });
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
    try testing.expectEqualStrings("B", page.events[0].event_type);
    try testing.expectEqualStrings("C", page.events[1].event_type);
}

test "readAll: from start across multiple streams" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r1 = try esdb.appendToStream(c, a, "a", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
    });
    defer esdb.freeEvents(a, r1.events);
    const r2 = try esdb.appendToStream(c, a, "b", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "B", .data = "2" },
    });
    defer esdb.freeEvents(a, r2.events);

    const page = try esdb.readAll(c, a, .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
    try testing.expectEqualStrings("a", page.events[0].stream_id);
    try testing.expectEqualStrings("b", page.events[1].stream_id);
}

test "readAll: from position resumes after a checkpoint" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r1 = try esdb.appendToStream(c, a, "a", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
    });
    defer esdb.freeEvents(a, r1.events);
    const r2 = try esdb.appendToStream(c, a, "b", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "B", .data = "2" },
    });
    defer esdb.freeEvents(a, r2.events);

    const checkpoint = esdb.Position{ .commit = 1, .prepare = 1 };
    const page = try esdb.readAll(c, a, .{ .from = .{ .position = checkpoint } });
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 1), page.events.len);
    try testing.expectEqualStrings("b", page.events[0].stream_id);
}

test "readStream: empty data blob is preserved" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    });
    defer esdb.freeEvents(a, res.events);
    const page = try esdb.readStream(c, a, "s", .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 0), page.events[0].data.len);
}

test "readStream: revision numbers are 0-based and contiguous" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "A", .data = "1" },
        .{ .event_type = "B", .data = "2" },
        .{ .event_type = "C", .data = "3" },
    });
    defer esdb.freeEvents(a, res.events);
    const page = try esdb.readStream(c, a, "s", .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(u64, 0), page.events[0].revision);
    try testing.expectEqual(@as(u64, 1), page.events[1].revision);
    try testing.expectEqual(@as(u64, 2), page.events[2].revision);
}
