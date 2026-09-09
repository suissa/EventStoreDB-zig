// Minimal happy-path: open a memory store, append a few
// events, read them back, print stats.
//
//   zig build examples
//   ./zig-out/examples/basic
//
// Note: operations on the client are namespace functions in
// the `esdb` module, not methods. Use `esdb.appendToStream(c, ...)`.

const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main(init: std.process.Init) !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    _ = init;

    var client = try esdb.Client.open(allocator, .{ .path = ":memory:" });
    defer client.close();

    const stream_id = "orders-1";

    const result = try esdb.appendToStream(allocator, client, stream_id,
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{
            .{ .event_type = "OrderCreated", .data = "{\"id\":\"1\",\"total\":99.90}" },
            .{ .event_type = "OrderItemAdded", .data = "{\"sku\":\"A\",\"qty\":2}" },
            .{ .event_type = "OrderShipped", .data = "{\"carrier\":\"DHL\"}" },
        },
    );
    defer allocator.free(result.events);

    const w = std.io.getStdOut().writer();
    try w.print("created stream; next revision: {d}, log pos: {d}\n", .{ result.next_revision, result.log_position });

    const page = try esdb.readStream(allocator, client, stream_id, .{});
    defer allocator.free(page.events);
    for (page.events) |ev| {
        try w.print("  rev={d}  pos={d}  type={s:<16}  data={s}\n", .{
            ev.revision,
            ev.log_position,
            ev.event_type,
            ev.data,
        });
    }

    const stats = try client.stats();
    try w.print("stats: streams={d} events={d} size={d} bytes\n", .{ stats.stream_count, stats.event_count, stats.db_size_bytes });
}
