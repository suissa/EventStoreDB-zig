// Snapshot pattern: build an aggregate by loading the latest
// snapshot and replaying only events above it.
//
//   zig build examples
//   ./zig-out/examples/snapshots

const std = @import("std");
const esdb = @import("eventstoredb");

const Counter = struct {
    stream_id: []const u8,
    total: i64 = 0,
    rev: u64 = 0,
};

pub fn main(_: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}){.init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = try esdb.Client.open(allocator, .{ .path = ":memory:" });
    defer client.close();

    const stream_id = "counter-1";
    var counter = Counter{ .stream_id = stream_id };

    // Append 5 "increment" events.
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        var exp: esdb.ExpectedRevision = .no_stream;
        if (i > 0) exp = .stream_exists;
        _ = try client.appendToStream(allocator, stream_id, .{ .expected_revision = exp }, &[_]esdb.EventData{
            .{ .event_type = "Inc", .data = "{\"by\":1}" },
        });
    }

    try load(&counter, client, allocator, 0);
    const w = std.io.getStdOut().writer();
    try w.print("cold start: total={d} at rev={d}\n", .{ counter.total, counter.rev });

    // Save a snapshot.
    const snap_payload = try std.fmt.allocPrint(allocator, "{{\"total\":{d}}}", .{counter.total});
    defer allocator.free(snap_payload);
    try client.saveSnapshot(stream_id, counter.rev, snap_payload, null);

    // Append 3 more.
    var j: u8 = 0;
    while (j < 3) : (j += 1) {
        _ = try client.appendToStream(allocator, stream_id, .{ .expected_revision = .stream_exists }, &[_]esdb.EventData{
            .{ .event_type = "Inc", .data = "{\"by\":1}" },
        });
    }

    var warm = Counter{ .stream_id = stream_id };
    try load(&warm, client, allocator, 0);
    try w.print("warm start: total={d} at rev={d} (replayed {d} events from {d})\n", .{
        warm.total,
        warm.rev,
        @as(i64, 3),
        @as(i64, warm.rev) - 3,
    });
}

fn load(c: *Counter, client: *esdb.Client, allocator: std.mem.Allocator, from: u64) !void {
    _ = from;
    if (client.loadSnapshot(allocator, c.stream_id, 0)) |snap| {
        defer allocator.free(snap.payload);
        if (snap.metadata) |m| defer allocator.free(m);
        c.rev = snap.revision;
    } else |err| {
        if (err != error.SnapshotNotFound) return err;
    }

    const page = try client.readStream(allocator, c.stream_id, .{
        .from = .{ .revision = c.rev },
        .limit = 1000,
    });
    defer allocator.free(page.events);
    for (page.events) |ev| {
        c.rev = ev.revision;
        if (std.mem.eql(u8, ev.event_type, "Inc")) {
            c.total += 1;
        }
    }
}

