//! Shared helpers for the DCB test modules. Kept in each
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
