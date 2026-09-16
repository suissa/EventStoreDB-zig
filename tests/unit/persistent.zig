//! Unit tests for the persistent-subscription lifecycle.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "persistent: create then delete group" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false);
    try esdb.deletePersistentSubscription(c, "g1", "s");
}

test "persistent: creating twice without overwrite fails" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false);
    try testing.expectError(error.PersistentSubscriptionExists, esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false));
}

test "persistent: overwrite=true replaces the config" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "1" },
    });
    defer esdb.freeEvents(a, r.events);
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1", .max_retries = 3 }, .{}, false);
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1", .max_retries = 99 }, .{}, true);
}

test "persistent: empty stream or group name is rejected" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try testing.expectError(error.InvalidArgument, esdb.createPersistentSubscription(c, a, "", "g", .{ .group_name = "g" }, .{}, false));
    try testing.expectError(error.InvalidArgument, esdb.createPersistentSubscription(c, a, "s", "", .{ .group_name = "" }, .{}, false));
}

test "persistent: delete missing group is a no-op" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();
    try esdb.deletePersistentSubscription(c, "missing", "missing");
}

test "persistent: connected worker has owned lifetime and client close is safe" {
    const a = testing.allocator;
    const c = try common.newClient(a);

    const r = try esdb.appendToStream(c, a, "owned-worker", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "Created", .data = "{}" },
    });
    defer esdb.freeEvents(a, r.events);

    try esdb.createPersistentSubscription(c, a, "owned-worker", "workers", .{ .group_name = "workers" }, .{}, false);
    var sub = try esdb.connectPersistentSubscription(c, a, "owned-worker", "workers");
    c.close();
    sub.close();
}

test "persistent: out-of-order ACK never crosses an incomplete gap" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "checkpoint", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "X", .data = "0" },
        .{ .event_type = "X", .data = "1" },
        .{ .event_type = "X", .data = "2" },
    });
    defer esdb.freeEvents(a, r.events);

    try esdb.createPersistentSubscription(c, a, "checkpoint", "g", .{ .group_name = "g", .from = .{ .start = {} } }, .{}, false);
    var sub = try esdb.connectPersistentSubscription(c, a, "checkpoint", "g");
    defer sub.close();

    var ids: [3]esdb.Uuid = undefined;
    var received: usize = 0;
    const deadline: i128 = common.nowNs() + 2 * std.time.ns_per_s;
    while (received < ids.len and common.nowNs() < deadline) {
        if (sub.tryReceive()) |item| {
            switch (item) {
                .message => |msg| {
                    ids[received] = msg.event.event_id;
                    try testing.expectEqual(@as(u64, @intCast(received)), msg.event.revision);
                    received += 1;
                    esdb.freeEvent(a, msg.event);
                },
                .err => return item.err,
                .closed => break,
            }
        } else std.atomic.spinLoopHint();
    }
    try testing.expectEqual(@as(usize, 3), received);

    try sub.ack(ids[2]);
    try testing.expectEqual(@as(u64, 0), sub.last_position);

    try sub.ack(ids[0]);
    try testing.expectEqual(@as(u64, 1), sub.last_position);

    try sub.ack(ids[1]);
    try testing.expectEqual(@as(u64, 3), sub.last_position);
}
