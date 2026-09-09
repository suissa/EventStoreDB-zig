//! Micro-benchmarks. Each test runs a fixed amount of work and
//! prints the throughput on stderr. They are plain `test` blocks
//! so the standard `zig build test` runner picks them up; the
//! `common.logBench` helper prints timing after the assertions.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

const n_append: u32 = 2_000;
const n_read: u32 = 5_000;

test "bench: append throughput" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const started = common.nowNs();
    var i: u32 = 0;
    while (i < n_append) : (i += 1) {
        const r = try esdb.appendToStream(c, a, "bench", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        esdb.freeEvents(a, r.events);
    }
    const elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("append-1K", n_append, elapsed);
}

test "bench: readStream throughput" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var i: u32 = 0;
    while (i < 1_000) : (i += 1) {
        const r = try esdb.appendToStream(c, a, "bench", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        esdb.freeEvents(a, r.events);
    }

    const started = common.nowNs();
    var k: u32 = 0;
    while (k < n_read) : (k += 1) {
        const page = try esdb.readStream(c, a, "bench", .{ .limit = 1 });
        esdb.freeEvents(a, page.events);
    }
    const elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("readStream-1K", n_read, elapsed);
}

test "bench: readAll throughput" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var i: u32 = 0;
    while (i < 1_000) : (i += 1) {
        const r = try esdb.appendToStream(c, a, "bench", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        esdb.freeEvents(a, r.events);
    }

    const started = common.nowNs();
    var k: u32 = 0;
    while (k < n_read) : (k += 1) {
        const page = try esdb.readAll(c, a, .{});
        esdb.freeEvents(a, page.events);
    }
    const elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("readAll-1K", n_read, elapsed);
}

test "bench: idempotent append (no new row)" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    var id: esdb.Uuid = .{0} ** 16;
    id[0] = 1;
    const ev = esdb.EventData{ .event_id = id, .event_type = "T", .data = "{}" };
    const r0 = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{ev});
    defer esdb.freeEvents(a, r0.events);

    const started = common.nowNs();
    var i: u32 = 0;
    while (i < n_append) : (i += 1) {
        const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .{ .any = {} } }, &[_]esdb.EventData{ev});
        esdb.freeEvents(a, r.events);
    }
    const elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("idempotent-append-1K", n_append, elapsed);
}

test "bench: snapshot save + load" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    try esdb.saveSnapshot(c, "s", 1, "payload", "meta");
    const started = common.nowNs();
    var i: u32 = 0;
    while (i < 1_000) : (i += 1) {
        try esdb.saveSnapshot(c, "s", i + 1, "payload", "meta");
        const snap = try esdb.loadSnapshot(c, a, "s", 0);
        a.free(snap.payload);
        a.free(snap.stream_id);
        if (snap.metadata) |m| a.free(m);
    }
    const elapsed: u64 = @intCast(common.nowNs() - started);
    common.logBench("snapshot-save+load-1K", 1_000, elapsed);
}
