//! Test suite. Run with `zig build test`.
//!
//! Tests use the namespace function API: `esdb.appendToStream(&c, ...)`
//! rather than method-call syntax. The Client struct stores state;
//! the functions in `src/append.zig`, `src/read.zig`, etc. operate on
//! `*Client` as their first parameter.
//!
//! The test allocator is the GeneralPurposeAllocator wrapped in leak
//! detection, but `bind.prepare` is documented to "intentionally leak
//! the small SQL string for the life of the program" (see bind.zig).
//! To keep the leak detector clean for the SQL strings that escape
//! the bind lifecycle, we route `newClient` through the page
//! allocator. Per-event allocations still flow through the test
//! allocator and are caught if they leak.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");

// Dedicated allocator for the connection itself so the SQL strings
// held internally by `bind.prepare` are not charged against the
// test allocator's leak detector.
const conn_alloc = std.heap.page_allocator;

fn newClient(testing_allocator: std.mem.Allocator) !*esdb.Client {
    _ = testing_allocator;
    const c = try esdb.Client.open(conn_alloc, .{ .path = ":memory:" });
    errdefer c.close();
    return c;
}

// Each `RecordedEvent` returned by the read API owns its `stream_id`,
// `event_type`, `data` and `metadata` slices (allocated by
// `read.zig::dupText`/`dupBlob`). The library does not ship a
// corresponding free helper yet, so the tests do the cleanup
// themselves.
fn freeEvents(allocator: std.mem.Allocator, events: []const esdb.RecordedEvent) void {
    for (events) |e| {
        allocator.free(e.stream_id);
        allocator.free(e.event_type);
        allocator.free(e.data);
        if (e.metadata) |m| allocator.free(m);
    }
    allocator.free(events);
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

    const res = try esdb.appendToStream(c, a, "s1",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{
            .{ .event_type = "A", .data = "{\"v\":1}" },
            .{ .event_type = "B", .data = "{\"v\":2}" },
        },
    );
    defer freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 2), res.next_revision);
    try testing.expectEqual(@as(u64, 2), res.log_position);
    try testing.expectEqual(@as(u64, 2), c.lastLogPosition());

    const page = try esdb.readStream(c, a, "s1", .{});
    defer freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
    try testing.expectEqualStrings("A", page.events[0].event_type);
    try testing.expectEqualStrings("B", page.events[1].event_type);
    try testing.expect(page.is_end_of_stream);
}

test "append with expected revision" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .stream_exists },
        &[_]esdb.EventData{.{ .event_type = "X", .data = "" }},
    ));

    const created = try esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "X", .data = "" }},
    );
    defer freeEvents(a, created.events);

    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "X", .data = "" }},
    ));

    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .{ .revision = 0 } },
        &[_]esdb.EventData{.{ .event_type = "X", .data = "" }},
    ));
}

test "idempotency" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    var id: esdb.Uuid = .{0} ** 16;
    id[0] = 1;
    const ev = esdb.EventData{ .event_id = id, .event_type = "T", .data = "{}" };

    const first = try esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{ev},
    );
    defer freeEvents(a, first.events);
    const second = try esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{ev},
    );
    // Second call hits the idempotency fast path and returns the
    // row previously inserted; its `stream_id`, `event_type` and
    // `data` were duped by `lookupCommitted` using the test
    // allocator, so we have to free them here.
    defer freeEvents(a, second.events);
    try testing.expectEqual(@as(usize, 1), second.events.len);
    try testing.expectEqual(first.events[0].log_position, second.events[0].log_position);
    try testing.expectEqual(@as(u64, 1), c.lastLogPosition());
}

test "read all" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    const r1 = try esdb.appendToStream(c, a, "a",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "X", .data = "1" }},
    );
    defer freeEvents(a, r1.events);
    const r2 = try esdb.appendToStream(c, a, "b",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "Y", .data = "2" }},
    );
    defer freeEvents(a, r2.events);

    const page = try esdb.readAll(c, a, .{});
    defer freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
}

test "snapshot round trip" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 5, "payload", "meta");
    const snap = try esdb.loadSnapshot(c, a, "s", 0);
    defer a.free(snap.payload);
    defer a.free(snap.metadata.?);
    defer a.free(snap.stream_id);
    try testing.expectEqual(@as(u64, 5), snap.revision);
    try testing.expectEqualStrings("payload", snap.payload);

    try testing.expectError(error.SnapshotNotFound, esdb.loadSnapshot(c, a, "missing", 0));
}

test "delete stream" {
    const a = testing.allocator;
    const c = try newClient(a);
    defer c.close();

    const r3 = try esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "T", .data = "1" }},
    );
    defer freeEvents(a, r3.events);

    try esdb.deleteStream(c, "s");
    try testing.expectError(error.StreamTombstoned, esdb.appendToStream(c, a, "s",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{.{ .event_type = "T", .data = "2" }},
    ));
}

test "uuid v4 layout" {
    const u = esdb.uuid.newV4();
    try testing.expectEqual(@as(u8, 0x40), u[6] & 0xF0);
    try testing.expectEqual(@as(u8, 0x80), u[8] & 0xC0);
}
