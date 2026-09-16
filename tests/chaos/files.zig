//! Chaos test: file-system failures (invalid paths, directories
//! that don't exist). SQLite 3.47 is permissive about paths with
//! NUL bytes, so we only exercise the cases it actually rejects.

const std = @import("std");
const testing = std.testing;
const esdb = @import("eventstoredb");
const common = @import("common.zig");

test "chaos: open on a non-existent directory fails" {
    try testing.expectError(error.CannotOpenDatabase, esdb.Client.open(common.conn_alloc, .{
        .path = "/this/dir/does/not/exist/db.sqlite",
    }));
}

test "chaos: open on a directory that exists but is not a file fails" {
    // `.` is a directory, not a SQLite file. SQLite rejects
    // opening it (SQLITE_CANTOPEN_ISDIR or similar).
    try testing.expectError(error.CannotOpenDatabase, esdb.Client.open(common.conn_alloc, .{
        .path = ".",
    }));
}

test "chaos: open with an empty path falls back to in-memory semantics or errors" {
    // SQLite treats "" as a temp database; either the Client opens
    // it or it errors. Both are acceptable. We just want to make
    // sure neither path crashes.
    if (esdb.Client.open(common.conn_alloc, .{ .path = "" })) |c| {
        c.close();
    } else |_| {
        // Expected: CannotOpenDatabase
    }
}
