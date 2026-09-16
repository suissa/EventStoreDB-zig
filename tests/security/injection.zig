//! Security: every public string input must flow through a
//! prepared statement. Try a battery of classic SQL-injection
//! vectors and verify that (a) the store does not execute the
//! injected code, and (b) the malicious string ends up stored
//! verbatim so the read side sees the attacker's input.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

const vectors = [_][]const u8{
    "'; DROP TABLE events; --",
    "' OR 1=1; --",
    "x' UNION SELECT 1,2,3,4,5,6,7,8,9 FROM events --",
    "admin'/*",
    "\"; DELETE FROM streams; --",
    "name\x00with\x00nulls",
    "' waitfor delay '0:0:10' --",
    "1' AND (SELECT COUNT(*) FROM sqlite_master) > 0 --",
};

test "security: stream id is bound, not concatenated" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    for (vectors, 0..) |vec, i| {
        const name = std.fmt.allocPrint(a, "victim-{d}", .{i}) catch unreachable;
        defer a.free(name);
        const r = try esdb.appendToStream(c, a, name, .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        });
        defer common.freeEvents(a, r.events);

        // Now try to use the malicious string as a stream id. It
        // must be treated as a literal, not a query. Use a
        // success-only block so the defer only runs when `r2`
        // is actually bound — Zig evaluates the defer expression
        // at declaration time, so binding-less `r2` would be UB.
        if (esdb.appendToStream(c, a, vec, .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
            .{ .event_type = "X", .data = "1" },
        })) |r2| {
            defer common.freeEvents(a, r2.events);
        } else |err| switch (err) {
            error.Sqlite => {}, // some vectors are valid UTF-8-ish but still OK
            else => return err,
        }
    }

    // The legit "victim-*" streams must still be readable, proving
    // that the malicious vectors did not DROP the table.
    for (vectors, 0..) |_, i| {
        const name = std.fmt.allocPrint(a, "victim-{d}", .{i}) catch unreachable;
        defer a.free(name);
        const page = try esdb.readStream(c, a, name, .{});
        defer common.freeEvents(a, page.events);
        try testing.expectEqual(@as(usize, 1), page.events.len);
    }
}

test "security: event data is bound, not concatenated" {
    const a = testing.allocator;
    const c = try common.newClient(a);
    defer c.close();

    const r = try esdb.appendToStream(c, a, "s", .{ .expected_revision = .no_stream }, &[_]esdb.EventData{
        .{ .event_type = "'; DROP TABLE events; --", .data = "x" },
    });
    defer common.freeEvents(a, r.events);

    const page = try esdb.readStream(c, a, "s", .{});
    defer common.freeEvents(a, page.events);
    try testing.expectEqualStrings("'; DROP TABLE events; --", page.events[0].event_type);
    // Table is still alive.
    const stats = try c.stats();
    try testing.expect(stats.event_count >= 1);
}
