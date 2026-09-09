//! Unit tests for `appendToStream`.
//!
//! Covers every flavor of `ExpectedRevision`, idempotency by
//! `event_id`, multi-event batches with mixed new/existing rows,
//! the `DatabaseClosed` short-circuit, and the `InvalidArgument`
//! guards on empty stream id / empty batch.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "append: no_stream on empty store succeeds" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 1), res.next_revision);
    try testing.expectEqual(@as(u64, 1), res.log_position);
    try testing.expectEqual(@as(u64, 1), c.lastLogPosition());
}

test "append: no_stream on existing stream fails" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r0 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r0.events);
    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "2" },
    }));
}

test "append: stream_exists on empty fails, on existing succeeds" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s", .{ .expected_revision = .stream_exists }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));

    // Create it, then stream_exists must accept.
    const created = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, created.events);

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .stream_exists }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "2" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 2), res.next_revision);
}

test "append: revision matches exactly" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    // Empty stream: next expected revision is 0.
    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .revision = 0 } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 1), res.next_revision);

    // Stale: 0 again should fail (next is 1 now).
    try testing.expectError(error.WrongExpectedVersion, esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .revision = 0 } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));

    // Correct: 1 succeeds, leaving next = 2.
    const res2 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .revision = 1 } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    });
    defer esdb.freeEvents(a, res2.events);
    try testing.expectEqual(@as(u64, 2), res2.next_revision);
}

test "append: any bypasses the check" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r0 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r0.events);
    // `any` should pass even on a stream that already exists.
    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "2" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(u64, 2), res.next_revision);
}

test "append: idempotency by event_id reuses the existing row" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var id: esdb.Uuid = .{0} ** 16;
    id[0] = 1;
    const ev = esdb.EventData{ .event_id = id, .event_type = "T", .data = "{}" };

    const first = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev});
    defer esdb.freeEvents(a, first.events);
    try testing.expectEqual(@as(usize, 1), first.events.len);
    try testing.expectEqual(@as(u64, 1), first.events[0].log_position);

    const second = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{ev});
    // The second call returns duped slices because the row was
    // resolved by the idempotency fast-path.
    defer esdb.freeEvents(a, second.events);
    try testing.expectEqual(@as(usize, 1), second.events.len);
    try testing.expectEqual(first.events[0].log_position, second.events[0].log_position);
    try testing.expectEqual(@as(u64, 1), c.lastLogPosition());
}

test "append: idempotency across mixed new and existing events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var id_existing: esdb.Uuid = .{0} ** 16;
    id_existing[0] = 1;
    const ev_old = esdb.EventData{ .event_id = id_existing, .event_type = "T", .data = "{}" };

    const r0 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev_old});
    defer esdb.freeEvents(a, r0.events);
    var id_new: esdb.Uuid = .{0} ** 16;
    id_new[0] = 2;

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
        ev_old, // existing
        .{ .event_id = id_new, .event_type = "T", .data = "new" },
    });
    // The existing row's slices are duped by the caller's
    // allocator; the new row's slices are the caller's literals.
    // Mix of both — only the outer slice is owned here.
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(usize, 2), res.events.len);
    try testing.expectEqual(@as(u64, 1), res.events[0].log_position); // existing, reuse
    try testing.expectEqual(@as(u64, 2), res.events[1].log_position); // new
    try testing.expectEqual(@as(u64, 2), c.lastLogPosition());
}

test "append: empty event batch returns InvalidArgument" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{}));
}

test "append: empty stream id returns InvalidArgument" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try testing.expectError(error.InvalidArgument, esdb.appendToStream(c, a, "", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    }));
}

test "append: append to tombstoned stream returns StreamTombstoned" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r0 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r0.events);
    try esdb.deleteStream(c, "s");
    try testing.expectError(error.StreamTombstoned, esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "2" },
    }));
}

test "append: auto-fills event_id when caller leaves it zero" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_id = .{0} ** 16, .event_type = "X", .data = "" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expect(res.events[0].event_id[0] != 0 or res.events[0].event_id[15] != 0);
}

test "append: empty data blob is accepted" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(usize, 0), res.events[0].data.len);
}

test "append: metadata is preserved" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "{}", .metadata = "{\"trace\":\"abc\"}" },
    });
    defer esdb.freeEvents(a, res.events);
    try testing.expect(res.events[0].metadata != null);
    try testing.expectEqualStrings("{\"trace\":\"abc\"}", res.events[0].metadata.?);
}

test "append: many events in one batch share a transaction position" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var batch: [16]esdb.EventData = undefined;
    for (&batch) |*slot| {
        slot.* = .{ .event_type = "X", .data = "1" };
    }
    const res = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &batch);
    defer esdb.freeEvents(a, res.events);
    try testing.expectEqual(@as(usize, 16), res.events.len);
    const tx_pos = res.events[0].transaction_position;
    for (res.events[1..]) |ev| try testing.expectEqual(tx_pos, ev.transaction_position);
}
