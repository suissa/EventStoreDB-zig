//! Stream and `$all` reads (forward and backward).

const std = @import("std");
const c = @import("c.zig").c;
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const bind = @import("bind.zig");
const Client = @import("client.zig").Client;

/// Read a page of events from a single stream.
pub fn readStream(
    self: *Client,
    allocator: std.mem.Allocator,
    stream_id: []const u8,
    opts: types.ReadOptions,
) (errors_mod.Error || std.mem.Allocator.Error)!types.ReadResult {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    if (stream_id.len == 0) return error.InvalidArgument;

    const limit: u32 = if (opts.limit == 0) self.max_batch_size else opts.limit;
    if (try isTombstoned(self.conn, stream_id)) return error.StreamTombstoned;

    const from_rev = try resolveFromRevision(self, stream_id, switch (opts.from) {
        .start => if (opts.direction == .backward) .start_backward else .start,
        else => opts.from,
    });

    const sql = if (opts.direction == .backward)
        \\SELECT event_id, stream_id, event_number, log_position, transaction_position,
        \\       event_type, data, metadata
        \\FROM events
        \\WHERE stream_id = ? AND event_number <= ?
        \\ORDER BY event_number DESC
        \\LIMIT ?
    else
        \\SELECT event_id, stream_id, event_number, log_position, transaction_position,
        \\       event_type, data, metadata
        \\FROM events
        \\WHERE stream_id = ? AND event_number >= ?
        \\ORDER BY event_number ASC
        \\LIMIT ?
    ;

    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    _ = bind.bindI64(stmt, 2, from_rev);
    _ = bind.bindI64(stmt, 3, @intCast(@as(i64, @intCast(limit))));

    return collectPage(allocator, stmt, limit, opts.direction == .backward, true);
}

/// Read a page of events from the global all-stream log.
pub fn readAll(
    self: *Client,
    allocator: std.mem.Allocator,
    opts: types.ReadOptions,
) (errors_mod.Error || std.mem.Allocator.Error)!types.ReadResult {
    if (self.closed.load(.seq_cst)) return error.DatabaseClosed;
    const limit: u32 = if (opts.limit == 0) self.max_batch_size else opts.limit;
    const from_pos = resolveFromPosition(opts.from, self.lastLogPosition());

    // A `Position{commit=N, prepare=N}` is the position *of* the
    // next unread event. To start reading *at* the next event
    // (i.e. skip everything strictly before it) we use
    // `log_position > N`. The exception is `.start_of_log`,
    // which is `{commit=0, prepare=0}` and is the sentinel for
    // "from the beginning", so we use `>= 0`.
    const min_pos: i64 = if (from_pos.commit == 0 and from_pos.prepare == 0) 0 else @intCast(from_pos.commit);
    const use_strict: bool = from_pos.commit != 0 or from_pos.prepare != 0;

    const sql_strict =
        \\SELECT event_id, stream_id, event_number, log_position, transaction_position,
        \\       event_type, data, metadata
        \\FROM events
        \\WHERE log_position > ?
        \\ORDER BY log_position ASC
        \\LIMIT ?
    ;
    const sql_loose =
        \\SELECT event_id, stream_id, event_number, log_position, transaction_position,
        \\       event_type, data, metadata
        \\FROM events
        \\WHERE log_position >= ?
        \\ORDER BY log_position ASC
        \\LIMIT ?
    ;
    const sql: []const u8 = if (use_strict) sql_strict else sql_loose;

    const stmt = try bind.prepare(self.conn.db, self.conn.allocator, sql);
    defer bind.finalize(self.conn.allocator, stmt);
    _ = bind.bindI64(stmt, 1, min_pos);
    _ = bind.bindI64(stmt, 2, @intCast(@as(i64, @intCast(limit))));

    return collectPage(allocator, stmt, limit, opts.direction == .backward, false);
}

fn collectPage(
    allocator: std.mem.Allocator,
    stmt: *c.sqlite3_stmt,
    limit: u32,
    backward: bool,
    has_stream: bool,
) (errors_mod.Error || std.mem.Allocator.Error)!types.ReadResult {
    var events = try std.ArrayListAligned(types.RecordedEvent, null).initCapacity(allocator, 16);
    defer events.deinit(allocator);

    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        try events.append(allocator, try scanEvent(allocator, stmt));
    }

    if (events.items.len == 0) {
        return .{
            .events = &[_]types.RecordedEvent{},
            .next_revision = 0,
            .next_position = .{},
            .is_end_of_stream = true,
        };
    }

    const last = events.items[events.items.len - 1];
    const is_end = events.items.len < @as(usize, limit);

    var next_rev: u64 = 0;
    var next_pos: types.Position = .{};
    if (backward) {
        next_rev = if (last.revision > 0) last.revision - 1 else 0;
        next_pos = .{ .commit = if (last.log_position > 0) last.log_position - 1 else 0 };
    } else {
        next_rev = last.revision + 1;
        next_pos = .{ .commit = last.log_position + 1, .prepare = last.log_position + 1 };
    }
    _ = has_stream;

    return .{
        .events = try events.toOwnedSlice(allocator),
        .next_revision = next_rev,
        .next_position = next_pos,
        .is_end_of_stream = is_end,
    };
}

fn scanEvent(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt) (errors_mod.Error || std.mem.Allocator.Error)!types.RecordedEvent {
    const ev_id: types.Uuid = blk: {
        var out: types.Uuid = undefined;
        const len = c.sqlite3_column_bytes(stmt, 0);
        if (len != 16) return error.Sqlite;
        const src: [*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, 0));
        @memcpy(&out, src[0..16]);
        break :blk out;
    };
    const stream_id = try dupText(allocator, stmt, 1);
    const event_type = try dupText(allocator, stmt, 5);
    const data = try dupBlob(allocator, stmt, 6);
    const metadata: ?[]const u8 = if (c.sqlite3_column_type(stmt, 7) == c.SQLITE_NULL) null else try dupBlob(allocator, stmt, 7);
    return .{
        .event_id = ev_id,
        .stream_id = stream_id,
        .event_type = event_type,
        .data = data,
        .metadata = metadata,
        .revision = @intCast(c.sqlite3_column_int64(stmt, 2)),
        .log_position = @intCast(c.sqlite3_column_int64(stmt, 3)),
        .transaction_position = @intCast(c.sqlite3_column_int64(stmt, 4)),
    };
}

fn dupText(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) std.mem.Allocator.Error![]const u8 {
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
    if (len == 0) return &[_]u8{};
    const ptr_opt: ?[*]const u8 = @ptrCast(c.sqlite3_column_text(stmt, col));
    const ptr = ptr_opt orelse return &[_]u8{};
    return allocator.dupe(u8, ptr[0..len]);
}

fn dupBlob(allocator: std.mem.Allocator, stmt: *c.sqlite3_stmt, col: c_int) std.mem.Allocator.Error![]const u8 {
    const len: usize = @intCast(c.sqlite3_column_bytes(stmt, col));
    if (len == 0) return &[_]u8{};
    const ptr_opt: ?[*]const u8 = @ptrCast(c.sqlite3_column_blob(stmt, col));
    const ptr = ptr_opt orelse return &[_]u8{};
    return allocator.dupe(u8, ptr[0..len]);
}

fn isTombstoned(conn: *schema_mod.Connection, stream_id: []const u8) !bool {
    const stmt = try bind.prepare(conn.db, conn.allocator, "SELECT deleted_at FROM streams WHERE stream_id = ?");
    defer bind.finalize(conn.allocator, stmt);
    _ = bind.bindText(stmt, 1, stream_id);
    if (c.sqlite3_step(stmt) != c.SQLITE_ROW) return false;
    return c.sqlite3_column_type(stmt, 0) != c.SQLITE_NULL;
}

fn resolveFromRevision(self: *Client, stream_id: []const u8, from: types.From) !i64 {
    _ = stream_id;
    switch (from) {
        .start => return 0,
        // For backward reads, "from start" means "from the end of
        // the log" — i.e. no upper bound. Use a large positive
        // value that exceeds any real revision.
        .start_backward => return std.math.maxInt(i64),
        .end => {
            const v = @import("client.zig").scalarI64(
                self.conn,
                "SELECT revision FROM streams WHERE stream_id = ?",
            ) catch |err| switch (err) {
                // Stream does not exist: there are no events
                // past the end, so a read from end returns no
                // rows. We return `0` so the SQL `event_number <= 0`
                // matches no event with a 0-based index, and the
                // `is_end_of_stream` short-circuit fires.
                error.NotFound => return 0,
                else => return err,
            };
            return v + 1;
        },
        .revision => |r| return @intCast(r),
        .position => return 0,
    }
}

fn resolveFromPosition(from: types.From, last_log: u64) types.Position {
    return switch (from) {
        .start, .start_backward, .revision => .start_of_log,
        .position => |p| p,
        .end => .{ .commit = last_log + 1, .prepare = last_log + 1 },
    };
}
