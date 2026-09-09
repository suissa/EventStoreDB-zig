//! Storage layer: opens the SQLite database, applies pragmas,
//! runs migrations, and exposes a tiny statement-prepare/exec
//! helper used by the rest of the package.

const std = @import("std");
const c = @import("c.zig").c;
const errors = @import("errors.zig");
const bind = @import("bind.zig");
const time_mod = @import("time.zig");

pub const Connection = struct {
    db: *c.sqlite3,
    busy_timeout_ms: u32,
    allocator: std.mem.Allocator,

    /// Open a SQLite database. The path can be ":memory:" for
    /// an in-memory database, or any SQLite-acceptable path.
    pub fn open(allocator: std.mem.Allocator, path: []const u8, busy_timeout_ms: u32) errors.Error!*Connection {
        // Reject paths that embed a NUL byte: SQLite would silently truncate
        // at the NUL via strlen, so we treat the path as invalid up front.
        if (std.mem.indexOfScalar(u8, path, 0) != null) {
            return errors.Error.CannotOpenDatabase;
        }
        var db: ?*c.sqlite3 = null;
        // SQLite expects a nul-terminated path.
        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);

        const flags: c_int = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_URI;
        const rc = c.sqlite3_open_v2(path_z.ptr, &db, flags, null);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return errors.Error.CannotOpenDatabase;
        }
        if (db == null) return errors.Error.CannotOpenDatabase;

        const conn = try allocator.create(Connection);
        conn.* = .{
            .db = db.?,
            .busy_timeout_ms = busy_timeout_ms,
            .allocator = allocator,
        };

        try exec(conn, "PRAGMA journal_mode = WAL");
        try exec(conn, "PRAGMA synchronous = NORMAL");
        try exec(conn, "PRAGMA temp_store = MEMORY");
        try exec(conn, "PRAGMA foreign_keys = OFF");

        var buf: [64]u8 = undefined;
        const pragma = std.fmt.bufPrint(&buf, "PRAGMA busy_timeout = {d}", .{busy_timeout_ms}) catch return errors.Error.Sqlite;
        try exec(conn, pragma);

        try applyMigrations(conn);
        return conn;
    }

    pub fn close(self: *Connection) void {
        _ = c.sqlite3_close(self.db);
        self.allocator.destroy(self);
    }

    /// Execute a SQL statement that has no parameters and no
    /// result rows.
    pub fn exec(self: *Connection, sql: []const u8) errors.Error!void {
        const sql_z = self.allocator.dupeZ(u8, sql) catch return errors.Error.OutOfMemory;
        defer self.allocator.free(sql_z);
        const rc = c.sqlite3_exec(self.db, sql_z.ptr, null, null, null);
        if (rc != c.SQLITE_OK) return errors.Error.Sqlite;
    }

    /// Run a SQL scalar query (one row, one column) and return
    /// its i64 value. Returns `error.NotFound` for zero rows.
    pub fn queryScalarI64(self: *Connection, sql: []const u8) errors.Error!i64 {
        const stmt = try bind.prepare(self.db, self.allocator, sql);
        defer bind.finalize(self.allocator, stmt);
        const rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_ROW) return errors.Error.NotFound;
        return c.sqlite3_column_int64(stmt, 0);
    }

    /// Format and execute a SQL string. Used only for trivial
    /// queries (the schema_info insert) where compile-time
    /// formatting is convenient and there are no bindings.
    pub fn execFmt(self: *Connection, comptime fmt: []const u8, args: anytype) errors.Error!void {
        var buf: [4096]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, fmt, args) catch return errors.Error.Sqlite;
        try self.exec(sql);
    }
};

const schemaVersion: u32 = 2;

const schemaSQL =
    \\CREATE TABLE IF NOT EXISTS streams (
    \\  stream_id        TEXT PRIMARY KEY,
    \\  stream_type      INTEGER NOT NULL DEFAULT 0,
    \\  revision         INTEGER NOT NULL DEFAULT -1,
    \\  max_count        INTEGER NOT NULL DEFAULT 0,
    \\  truncate_before  INTEGER NOT NULL DEFAULT 0,
    \\  custom_metadata  BLOB,
    \\  created_at       INTEGER NOT NULL,
    \\  updated_at       INTEGER NOT NULL,
    \\  deleted_at       INTEGER
    \\);
    \\CREATE INDEX IF NOT EXISTS idx_streams_type ON streams(stream_type);
    \\
    \\CREATE TABLE IF NOT EXISTS events (
    \\  event_id              BLOB NOT NULL,
    \\  stream_id             TEXT NOT NULL,
    \\  event_number          INTEGER NOT NULL,
    \\  log_position          INTEGER NOT NULL UNIQUE,
    \\  transaction_position  INTEGER NOT NULL,
    \\  event_type            TEXT NOT NULL,
    \\  data                  BLOB NOT NULL,
    \\  metadata              BLOB,
    \\  created_at            INTEGER NOT NULL,
    \\  -- DCB (Dynamic Consistency Boundary) columns.
    \\  -- Nullable for legacy rows written before schema v2.
    \\  -- `sequence` is the global, gap-free, append-only position
    \\  -- used by the DCB read/append algorithm. `tags` is a JSON
    \\  -- array of {key,value} pairs as described in the DCB spec;
    \\  -- it is filterable with JSON_EXTRACT(tags, '$.<key>').
    \\  -- `dc_time` is the wall-clock unix-ms used by DCB reads
    \\  -- (created_at stays as the EventStoreDB-style timestamp).
    \\  sequence              INTEGER,
    \\  tags                  TEXT,
    \\  dc_time               INTEGER,
    \\  PRIMARY KEY (stream_id, event_number)
    \\);
    \\CREATE INDEX IF NOT EXISTS idx_events_log_pos    ON events(log_position);
    \\CREATE INDEX IF NOT EXISTS idx_events_tx_pos     ON events(transaction_position);
    \\CREATE INDEX IF NOT EXISTS idx_events_type       ON events(event_type);
    \\CREATE INDEX IF NOT EXISTS idx_events_stream_rev ON events(stream_id, event_number);
    \\-- DCB indexes (added in v2). Partial unique index keeps
    \\-- legacy NULL sequence rows from clashing while still
    \\-- enforcing uniqueness for new DCB writes.
    \\CREATE UNIQUE INDEX IF NOT EXISTS idx_events_sequence
    \\  ON events(sequence) WHERE sequence IS NOT NULL;
    \\CREATE INDEX IF NOT EXISTS idx_events_tags ON events(tags);
    \\
    \\CREATE TABLE IF NOT EXISTS committed_event_ids (
    \\  event_id      BLOB PRIMARY KEY,
    \\  log_position  INTEGER NOT NULL,
    \\  stream_id     TEXT NOT NULL,
    \\  event_number  INTEGER NOT NULL,
    \\  committed_at  INTEGER NOT NULL
    \\);
    \\CREATE INDEX IF NOT EXISTS idx_committed_id_pos ON committed_event_ids(log_position);
    \\
    \\CREATE TABLE IF NOT EXISTS persistent_subscriptions (
    \\  group_name     TEXT NOT NULL,
    \\  stream_id      TEXT NOT NULL,
    \\  start_from     INTEGER NOT NULL,
    \\  last_position  INTEGER NOT NULL,
    \\  revision       INTEGER NOT NULL,
    \\  config         BLOB NOT NULL,
    \\  status         TEXT NOT NULL DEFAULT 'Live',
    \\  created_at     INTEGER NOT NULL,
    \\  updated_at     INTEGER NOT NULL,
    \\  PRIMARY KEY (group_name, stream_id)
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS persistent_acks (
    \\  group_name     TEXT NOT NULL,
    \\  stream_id      TEXT NOT NULL,
    \\  event_id       BLOB NOT NULL,
    \\  log_position   INTEGER NOT NULL,
    \\  retry_count    INTEGER NOT NULL DEFAULT 0,
    \\  parked         INTEGER NOT NULL DEFAULT 0,
    \\  enqueued_at    INTEGER NOT NULL,
    \\  PRIMARY KEY (group_name, stream_id, event_id)
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS snapshots (
    \\  stream_id     TEXT NOT NULL,
    \\  revision      INTEGER NOT NULL,
    \\  payload       BLOB NOT NULL,
    \\  metadata      BLOB,
    \\  created_at    INTEGER NOT NULL,
    \\  PRIMARY KEY (stream_id, revision)
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS projection_checkpoints (
    \\  projection_name         TEXT PRIMARY KEY,
    \\  last_processed_position INTEGER NOT NULL,
    \\  state                   BLOB,
    \\  updated_at              INTEGER NOT NULL
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS schema_info (
    \\  version    INTEGER PRIMARY KEY,
    \\  applied_at INTEGER NOT NULL
    \\);
;

/// Apply forward-only migrations until the on-disk schema
/// version matches `schemaVersion`. Re-runs are no-ops.
fn applyMigrations(conn: *Connection) errors.Error!void {
    try conn.exec(schemaSQL);

    const current = try conn.queryScalarI64("SELECT COALESCE(MAX(version), 0) FROM schema_info");
    if (current >= @as(i64, @intCast(schemaVersion))) return;

    var v: u32 = @intCast(current + 1);
    while (v <= schemaVersion) : (v += 1) {
        try conn.exec("SAVEPOINT mig");
        switch (v) {
            1 => {
                // v1: base DDL only. Already covered by the
                // idempotent schemaSQL above; nothing extra
                // to run inside the savepoint.
            },
            2 => {
                // v2: DCB (Dynamic Consistency Boundary)
                // columns. ALTER TABLE ADD COLUMN is not
                // idempotent in SQLite, so we probe via
                // PRAGMA table_info before issuing it.
                try addColumnIfMissing(conn, "events", "sequence", "INTEGER");
                try addColumnIfMissing(conn, "events", "tags", "TEXT");
                try addColumnIfMissing(conn, "events", "dc_time", "INTEGER");
                // Idempotent index DDL is safe to re-run.
                try conn.exec(
                    \\CREATE UNIQUE INDEX IF NOT EXISTS idx_events_sequence
                    \\  ON events(sequence) WHERE sequence IS NOT NULL;
                );
                try conn.exec("CREATE INDEX IF NOT EXISTS idx_events_tags ON events(tags);");
            },
            else => {
                // Future migrations branch here.
            },
        }
        try conn.exec("RELEASE mig");
        try conn.execFmt("INSERT INTO schema_info(version, applied_at) VALUES ({d}, {d})", .{ v, time_mod.nowSec() });
    }
}

/// SQLite has no IF NOT EXISTS for ADD COLUMN, so probe
/// `PRAGMA table_info(<table>)` and only ALTER when the
/// column is absent. Safe to call repeatedly.
fn addColumnIfMissing(conn: *Connection, table: []const u8, column: []const u8, col_type: []const u8) errors.Error!void {
    var buf: [256]u8 = undefined;
    const pragma_sql = std.fmt.bufPrint(&buf, "PRAGMA table_info({s})", .{table}) catch return errors.Error.Sqlite;
    const stmt = try bind.prepare(conn.db, conn.allocator, pragma_sql);
    defer bind.finalize(conn.allocator, stmt);

    var found = false;
    while (true) {
        const rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_DONE) break;
        if (rc != c.SQLITE_ROW) return errors.Error.Sqlite;
        const name = c.sqlite3_column_text(stmt, 1);
        if (name == null) continue;
        const name_slice = std.mem.span(name);
        if (std.mem.eql(u8, name_slice, column)) {
            found = true;
            break;
        }
    }
    if (found) return;

    var alter_buf: [256]u8 = undefined;
    const alter_sql = std.fmt.bufPrint(&alter_buf, "ALTER TABLE {s} ADD COLUMN {s} {s}", .{ table, column, col_type }) catch return errors.Error.Sqlite;
    try conn.exec(alter_sql);
}

/// Current unix timestamp in milliseconds.
pub fn nowMs() i64 {
    return time_mod.nowMs();
}

