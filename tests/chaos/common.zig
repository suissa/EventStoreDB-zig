//! Shared helpers for the chaos test modules. Kept in each
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

pub fn freeSnapshots(allocator: std.mem.Allocator, snaps: []const esdb.Snapshot) void {
    for (snaps) |s| {
        allocator.free(s.stream_id);
        allocator.free(s.payload);
        if (s.metadata) |m| allocator.free(m);
    }
    allocator.free(snaps);
}

pub fn freeStreamInfo(allocator: std.mem.Allocator, infos: []const esdb.StreamInfo) void {
    for (infos) |i| allocator.free(i.stream_id);
    allocator.free(infos);
}

pub fn freeProjectionState(allocator: std.mem.Allocator, p: esdb.ProjectionState) void {
    allocator.free(p.name);
    if (p.state) |s| allocator.free(s);
}

pub fn nowNs() i128 {
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);
    return @intCast(ts.nanoseconds);
}

pub fn sleepMs(ms: u32) void {
    std.Thread.sleep(ms * std.time.ns_per_ms);
}

pub fn logBench(comptime label: []const u8, n: u32, elapsed_ns: u64) void {
    const per = if (n == 0) 0 else elapsed_ns / n;
    std.debug.print("[bench] {s}: {d} ops in {d} ns ({d} ns/op)\n", .{ label, n, elapsed_ns, per });
}

pub fn uuidFromSeed(seed: u64, out: *[16]u8) void {
    var s = seed;
    for (out, 0..) |*b, i| {
        s = s *% 6364136223846793005 +% 1442695040888963407;
        b.* = @truncate(@as(u8, @intCast(s >> (i % 8 * 8))));
    }
}
