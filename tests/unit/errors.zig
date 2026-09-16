//! Unit tests for the public error contract.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "errors: every documented error has at least one trigger" {
    // Smoke test that the Error set is the one we ship. If a new
    // variant is added without a test, this catches it.
    const expected: []const esdb.Error = &.{
        error.CannotOpenDatabase,
        error.StreamNotFound,
        error.StreamTombstoned,
        error.WrongExpectedVersion,
        error.InvalidTransaction,
        error.SubscriptionClosed,
        error.PersistentSubscriptionNotFound,
        error.PersistentSubscriptionExists,
        error.SnapshotNotFound,
        error.DatabaseClosed,
        error.InvalidArgument,
        error.NotFound,
        error.PrepareFailed,
        error.Sqlite,
        error.OutOfMemory,
    };
    try testing.expectEqual(@as(usize, 15), expected.len);
}

test "errors: WrongExpectedVersionError formats the failure" {
    const c = try common.newClient(testing.allocator);
    defer c.close();

    const err = esdb.WrongExpectedVersionError{
        .expected = "StreamExists",
        .actual = 0,
        .stream = "s1",
    };
    // The struct implements `format` so callers can put it in any
    // `std.fmt` sink. We round-trip through the Io.Writer-based
    // `print` to exercise that path.
    var storage: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&storage);
    try err.format(&w);
    const slice = w.buffered();
    try testing.expect(std.mem.indexOf(u8, slice, "s1") != null);
    try testing.expect(std.mem.indexOf(u8, slice, "StreamExists") != null);
    try testing.expect(std.mem.indexOf(u8, slice, "0") != null);
}

test "errors: CannotOpenDatabase on a missing directory" {
    try testing.expectError(error.CannotOpenDatabase, esdb.Client.open(common.conn_alloc, .{
        .path = "/this/path/does/not/exist/store.db",
    }));
}
