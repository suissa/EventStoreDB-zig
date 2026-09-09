//! Stress test: writers and readers sharing the same in-memory
//! store. The Client serializes appends via its internal spinlock,
//! so this verifies (a) no torn writes, (b) the catch-up
//! subscription actually wakes up.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

const writer_threads: u32 = 4;
const events_per_writer: u32 = 250;

test "stress: 4 writers in parallel leave the log consistent" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    // Pre-subscribe so we can prove no events are dropped.
    var sub = try esdb.subscribeToAll(c, a, .{ .from = .{ .end = {} }, .poll_interval_ms = 5 });
    defer sub.close();

    const started = common.nowNs();
    var threads: [writer_threads]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        t.* = try std.Thread.spawn(.{}, writerFn, .{ c, a, @as(u32, @intCast(i)), events_per_writer });
    }
    for (threads) |t| t.join();
    const write_elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("stress-4x250", writer_threads * events_per_writer, write_elapsed);

    // The log must contain exactly writer_threads * events_per_writer events.
    try testing.expectEqual(@as(u64, writer_threads * events_per_writer), c.lastLogPosition());

    // Drain the subscription to prove the waker actually fires
    // under contention. We don't care about ordering here.
    var got: usize = 0;
    const deadline: i128 = common.nowNs() + 2 * std.time.ns_per_s;
    while (got < writer_threads * events_per_writer and common.nowNs() < deadline) {
        if (sub.tryReceive()) |msg_or_err| {
            switch (msg_or_err) {
                .event => got += 1,
                .err => return msg_or_err.err,
                .closed => break,
            }
        } else {
            std.atomic.spinLoopHint();
        }
    }
    try testing.expectEqual(@as(usize, writer_threads * events_per_writer), got);
}

fn writerFn(c: *esdb.Client, a: std.mem.Allocator, worker_id: u32, n: u32) void {
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "w-{d}", .{worker_id}) catch unreachable;
        const ev = esdb.EventData{ .event_type = "X", .data = "1" };
        if (esdb.appendToStream(c, a, name, .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{ev})) |r| {
            a.free(r.events);
        } else |_| return;
    }
}
