//! Unit tests for catch-up subscriptions.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "subscribe: empty stream yields nothing within timeout" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var sub = try esdb.subscribeToStream(c, a, "s", .{ .poll_interval_ms = 10 });
    defer sub.close();
    try testing.expect(sub.tryReceive() == null);
}

test "subscribe: receives events appended after subscribe" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var sub = try esdb.subscribeToStream(c, a, "s", .{ .from = .{ .end = {} }, .poll_interval_ms = 10 });
    defer sub.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
            .{ .event_type = "X", .data = "2" },
        });
        defer esdb.freeEvents(a, r.events);
    }

    var got: usize = 0;
    const deadline: i128 = common.nowNs() + std.time.ns_per_s;
    while (got < 2 and common.nowNs() < deadline) {
        if (sub.tryReceive()) |msg_or_err| {
            switch (msg_or_err) {
                .event => |ev| { esdb.freeEvent(a, ev); got += 1; },
                .err => return msg_or_err.err,
                .closed => break,
            }
        } else {
            std.atomic.spinLoopHint();
        }
    }
    // After the loop, sub.close() drains the queue via
    // `Queue.drain`, which frees the event slices.
    try testing.expectEqual(@as(usize, 2), got);
}

test "subscribe: from start replays existing events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }

    var sub = try esdb.subscribeToStream(c, a, "s", .{ .from = .{ .start = {} }, .poll_interval_ms = 10 });
    defer sub.close();

    var got: usize = 0;
    const deadline: i128 = common.nowNs() + std.time.ns_per_s;
    while (got < 1 and common.nowNs() < deadline) {
        if (sub.tryReceive()) |msg_or_err| {
            switch (msg_or_err) {
                .event => |ev| { esdb.freeEvent(a, ev); got += 1; },
                .err => return msg_or_err.err,
                .closed => break,
            }
        } else {
            std.atomic.spinLoopHint();
        }
    }
    // Queue.drain in sub.close() reclaims the leftover events.
    try testing.expectEqual(@as(usize, 1), got);
}

test "subscribe: close after some events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var sub = try esdb.subscribeToStream(c, a, "s", .{ .from = .{ .end = {} }, .poll_interval_ms = 10 });
    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    sub.close();
}

test "subscribe: empty stream id is rejected" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.subscribeToStream(c, a, "", .{}));
}

test "subscribe: closed client documents destruction contract" {
    // `Client.close` is destructive. This test exists only to
    // make that contract visible in the suite; the actual
    // closed-flag check is exercised on every entry point.
    const a = testing.allocator;
    const c = try common.newClient(a);
    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);
    c.close();
}
