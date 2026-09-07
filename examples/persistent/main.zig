// Persistent subscription: create a group, connect a consumer
// that acks every message, then produce a few events.
//
//   zig build examples
//   ./zig-out/examples/persistent

const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main(_: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}){.init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = try esdb.Client.open(allocator, .{ .path = ":memory:" });
    defer client.close();

    const stream_id = "audit-log";
    const group_name = "shippers";

    try client.createPersistentSubscription(allocator, stream_id, group_name, .{
        .group_name = group_name,
        .from = .start,
    }, .{}, false);

    var ps = try client.connectPersistentSubscription(allocator, stream_id, group_name);
    defer ps.close();

    // Consumer goroutine.
    const consumer = try std.Thread.spawn(.{}, consumerLoop, .{ &ps });
    defer consumer.join();

    // Produce a few events.
    var i: u8 = 0;
    while (i < 3) : (i += 1) {
        var exp: esdb.ExpectedRevision = .no_stream;
        if (i > 0) exp = .stream_exists;
        _ = try client.appendToStream(allocator, stream_id, .{ .expected_revision = exp }, &[_]esdb.EventData{
            .{ .event_type = "AuditEvent", .data = "{\"i\":1}" },
        });
        std.time.sleep(50 * std.time.ns_per_ms);
    }

    std.time.sleep(200 * std.time.ns_per_ms);
    std.debug.print("done\n", .{});
}

fn consumerLoop(ps: *esdb.PersistentSubscription) void {
    while (true) {
        const next = ps.receive() orelse return;
        switch (next) {
            .message => |m| {
                std.debug.print("delivered: rev={d} type={s} data={s}\n", .{
                    m.event.revision,
                    m.event.event_type,
                    m.event.data,
                });
                std.time.sleep(10 * std.time.ns_per_ms);
                ps.ack(m.event.event_id) catch {};
            },
            .err => |e| {
                std.debug.print("error: {any}\n", .{e});
                return;
            },
            .closed => return,
        }
    }
}

