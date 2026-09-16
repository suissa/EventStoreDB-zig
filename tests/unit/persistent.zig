//! Unit tests for the persistent-subscription lifecycle.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "persistent: create then delete group" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false);
    try esdb.deletePersistentSubscription(c, "g1", "s");
}

test "persistent: creating twice without overwrite fails" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
    try esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false);
    try testing.expectError(error.PersistentSubscriptionExists, esdb.createPersistentSubscription(c, a, "s", "g1", .{ .group_name = "g1" }, .{}, false));
}

test "persistent: overwrite=true replaces the config" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer esdb.freeEvents(a, r.events);
    }
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

    {
        const r = try esdb.appendToStream(c, a, "owned-worker", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "Created", .data = "{}" },
        });
        defer esdb.freeEvents(a, r.events);
    }

    try esdb.createPersistentSubscription(c, a, "owned-worker", "workers", .{ .group_name = "workers" }, .{}, false);
    var sub = try esdb.connectPersistentSubscription(c, a, "owned-worker", "workers");

    // Client.close() must signal the worker and wait until it has
    // stopped before destroying SQLite/Client memory. The returned
    // subscription can then release its queue and owned strings.
    c.close();
    sub.close();
}
