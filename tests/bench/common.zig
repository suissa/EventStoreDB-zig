//! Shared helpers for the benchmark test modules. Kept in each
//! subdirectory because Zig 0.16 test modules do not import
//! across subdirectory boundaries without extra build glue.

const std = @import("std");
const esdb = @import("eventstoredb");

pub const conn_alloc = std.heap.page_allocator;

pub fn newClient(_: std.mem.Allocator) !*esdb.Client {
    const c = try esdb.Client.open(conn_alloc, .{ .path = ":memory:" });
    errdefer c.close();
    return c;
}

pub fn freeEvents(allocator: std.mem.Allocator, events: []const esdb.RecordedEvent) void {
    for (events) |e| {
        allocator.free(e.stream_id);
        allocator.free(e.event_type);
        allocator.free(e.data);
        if (e.metadata) |m| allocator.free(m);
    }
    allocator.free(events);
}

pub fn logBench(comptime label: []const u8, count: usize, elapsed_ns: u128) void {
    const per_op_ns: u128 = if (count == 0) 0 else elapsed_ns / count;
    std.debug.print(
        "[bench] {s}: {d} ops in {d} ns ({d} ns/op)\n",
        .{ label, count, elapsed_ns, per_op_ns },
    );
}

pub fn nowNs() i128 {
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);
    return @intCast(ts.nanoseconds);
}
