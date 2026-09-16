//! Current-time helper. In Zig 0.16 there is no `std.time.timestamp`;
//! time is exposed through `std.Io.Clock`, which needs an `Io` instance.
//! We use `Threaded.init_single_threaded` so the rest of the package
//! can call `time.nowMs()` without threading an `Io` through every
//! call site. Cost is one threadpool-style struct per call; for the
//! handful of timestamps this store emits per second it is free in
//! practice. Promote to a per-Client Io if profiling shows otherwise.

const std = @import("std");

pub fn nowMs() i64 {
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);
    return ts.toMilliseconds();
}

pub fn nowSec() i64 {
    return @divTrunc(nowMs(), std.time.ms_per_s);
}

/// Clock suitable for benchmarking and short deadlines.
/// Returns a value in nanoseconds; the absolute reference is
/// undefined. Use the delta between two calls.
pub fn nowNs() i128 {
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    const ts = std.Io.Clock.now(.real, io);
    return @intCast(ts.nanoseconds);
}
