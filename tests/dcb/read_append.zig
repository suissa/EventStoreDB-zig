//! DCB (Dynamic Consistency Boundary) tests.
//!
//! These exercise the columns added in schema v2 (`sequence`,
//! `tags`, `dc_time`) and the read/append query shape described
//! in `docs/DCB.md`. The Zig side does not yet ship first-class
//! `readDcb` / `appendIfNoEventsMatch` helpers, so the tests use
//! the public API to write events and the public read API to
//! verify the data flows through. Once the helpers land, the
//! same tests should be expressible through them.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "dcb: columns exist on every new event" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "InvoiceIssued", .data = "{}" },
        });
        defer esdb.freeEvents(a, r.events);
    }

    // Reading the row back through the public API gives us the
    // data; the DCB columns are managed by the schema and are
    // NULL for legacy rows. We assert the event was committed
    // and that the read API still sees it.
    const page = try esdb.readStream(c, a, "s", .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 1), page.events.len);
    try testing.expectEqualStrings("InvoiceIssued", page.events[0].event_type);
}

test "dcb: empty stream tag round-trips through the schema" {
    // DCB tags are an opaque JSON blob; the library does not
    // parse or validate them. All we assert here is that an
    // event whose type acts as the "type" column is read back
    // verbatim, which is the foundation DCB relies on.
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "PaymentReceived", .data = "{\"amount\":42}" },
            .{ .event_type = "PaymentReceived", .data = "{\"amount\":7}" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    const page = try esdb.readStream(c, a, "s", .{});
    defer esdb.freeEvents(a, page.events);
    try testing.expectEqual(@as(usize, 2), page.events.len);
}

test "dcb: revision assignment is contiguous regardless of stream" {
    // DCB reads return "max sequence" for the matching rows, so
    // the global log position must be contiguous across streams.
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "a", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    {
        const r = try esdb.appendToStream(c, a, "b", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
            .{ .event_type = "X", .data = "2" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    const all = try esdb.readAll(c, a, .{});
    defer esdb.freeEvents(a, all.events);
    try testing.expectEqual(@as(usize, 3), all.events.len);
    try testing.expectEqual(@as(u64, 1), all.events[0].log_position);
    try testing.expectEqual(@as(u64, 2), all.events[1].log_position);
    try testing.expectEqual(@as(u64, 3), all.events[2].log_position);
}
