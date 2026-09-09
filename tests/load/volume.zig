//! Load test: 10 000 events across 1 000 streams, then a single
//! catch-all read.
//!
//! Run with `zig build test-load-volume`. Use `--test-filter` to
//! pick one. The test reports throughput on stderr.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

const stream_count: u32 = 100;
const per_stream: u32 = 10;

test "load: 1000 streams x 10 events" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const started = common.nowNs();

    var i: u32 = 0;
    while (i < stream_count) : (i += 1) {
        var name_buf: [16]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "s-{d:0>4}", .{i}) catch unreachable;
        var ev: esdb.EventData = .{ .event_type = "E", .data = "{\"i\":0}" };
        const r = try esdb.appendToStream(c, a, name, .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev});
        defer esdb.freeEvents(a, r.events);
        // Now append (per_stream - 1) more.
        var j: u32 = 1;
        while (j < per_stream) : (j += 1) {
            var data_buf: [32]u8 = undefined;
            const data = std.fmt.bufPrint(&data_buf, "{{\"i\":{d}}}", .{j}) catch unreachable;
            ev = .{ .event_type = "E", .data = data };
            const r2 = try esdb.appendToStream(c, a, name, .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{ev});
            defer esdb.freeEvents(a, r2.events);
        }
    }

    const write_elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("append-volume", stream_count * per_stream, write_elapsed);

    const read_started = common.nowNs();
    const page = try esdb.readAll(c, a, .{ .limit = 0 });
    defer esdb.freeEvents(a, page.events);
    const read_elapsed: u64 = @intCast(common.nowNs() - read_started);
    common.logBench("readAll-volume", page.events.len, read_elapsed);

    try testing.expectEqual(@as(usize, stream_count * per_stream), page.events.len);
    try testing.expectEqual(@as(u64, stream_count * per_stream), c.lastLogPosition());
}
