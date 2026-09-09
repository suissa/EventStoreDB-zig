//! Shared helpers for every `tests/<dir>/*.zig` module.
//! Each test module imports this file via `@import("common.zig")`
//! — the build system does not need a separate module entry because
//! the test root is the file itself and `common.zig` is a sibling
//! under the same `tests/<dir>/` directory.

const std = @import("std");
const esdb = @import("eventstoredb");

/// Used for the `Client` itself: its connection owns SQL strings
/// (dupe'd by `bind.prepare`) that the library intentionally
/// retains for the lifetime of the program. Routing them through
/// `page_allocator` keeps the test allocator's leak detector clean
/// without losing the safety net for everything else.
pub const conn_alloc = std.heap.page_allocator;

pub fn newClient(_: std.mem.Allocator) !*esdb.Client {
    const c = try esdb.Client.open(conn_alloc, .{ .path = ":memory:" });
    errdefer c.close();
    return c;
}

/// Free a `[]const RecordedEvent` returned by the public read API.
/// Each row owns its `stream_id`, `event_type`, `data` and optional
/// `metadata` slices (allocated via `read.zig::dupText`/`dupBlob`
/// with the caller's allocator). The library does not yet ship a
/// `freeEvents` helper.
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

/// Make a 16-byte UUID from any 8-byte seed. Useful for tests
/// that need stable, unique event IDs without going through the
/// CSPRNG.
pub fn uuidFromSeed(seed: u64) esdb.Uuid {
    var out: esdb.Uuid = undefined;
    const bytes = std.mem.asBytes(&seed);
    @memcpy(out[0..8], bytes);
    @memcpy(out[8..16], bytes);
    return out;
}

/// Print a benchmark result on stderr. Used by `tests/bench/`.
pub fn logBench(comptime label: []const u8, count: usize, elapsed_ns: u128) void {
    const per_op_ns: u128 = if (count == 0) 0 else elapsed_ns / count;
    std.debug.print(
        "[bench] {s}: {d} ops in {d} ns ({d} ns/op)\n",
        .{ label, count, elapsed_ns, per_op_ns },
    );
}

/// Monotonic clock in nanoseconds, suitable for deadlines.
/// Zig 0.16 removed `std.time.nanoTimestamp`; this is the
/// replacement we use everywhere a relative timestamp is needed.
pub fn nowNs() i128 {
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);
    return @intCast(ts.nanoseconds);
}

/// Spin-wait sleep in milliseconds. Replaces `std.time.sleep`.
pub fn sleepMs(ms: u32) void {
    const slice_ns: u64 = std.time.ns_per_ms;
    const deadline_ns: u64 = @as(u64, ms) * slice_ns;
    var elapsed: u64 = 0;
    while (elapsed < deadline_ns) : (elapsed += slice_ns) {
        std.atomic.spinLoopHint();
    }
}
