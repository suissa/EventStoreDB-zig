// Quick one-shot: create a demo.db populated with the event store schema
// and a handful of events so the analyzer has something interesting to look at.
const std = @import("std");

const c = @cImport(@cInclude("sqlite3.h"));

const schema_v2 =
    \\CREATE TABLE IF NOT EXISTS streams (
    \\  stream_id TEXT PRIMARY KEY,
    \\  stream_revision INTEGER NOT NULL DEFAULT 0,
    \\  created_at INTEGER NOT NULL,
    \\  updated_at INTEGER NOT NULL
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS events (
    \\  event_id TEXT PRIMARY KEY,
    \\  stream_id TEXT NOT NULL,
    \\  event_number INTEGER NOT NULL,
    \\  event_type TEXT NOT NULL,
    \\  data BLOB NOT NULL,
    \\  metadata BLOB,
    \\  log_position INTEGER NOT NULL,
    \\  created_at INTEGER NOT NULL,
    \\  sequence INTEGER,
    \\  tags TEXT,
    \\  dc_time INTEGER,
    \\  UNIQUE(stream_id, event_number)
    \\);
    \\
    \\CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_id, event_number);
    \\CREATE INDEX IF NOT EXISTS idx_events_position ON events(log_position);
    \\
    \\CREATE TABLE IF NOT EXISTS committed_event_ids (
    \\  event_id TEXT PRIMARY KEY,
    \\  committed_at INTEGER NOT NULL
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS persistent_subscriptions (
    \\  group_name TEXT NOT NULL,
    \\  stream_id TEXT NOT NULL,
    \\  checkpoint INTEGER NOT NULL DEFAULT 0,
    \\  created_at INTEGER NOT NULL,
    \\  PRIMARY KEY(group_name, stream_id)
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS persistent_acks (
    \\  group_name TEXT NOT NULL,
    \\  stream_id TEXT NOT NULL,
    \\  event_id TEXT NOT NULL,
    \\  acked_at INTEGER NOT NULL
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS snapshots (
    \\  stream_id TEXT NOT NULL,
    \\  stream_revision INTEGER NOT NULL,
    \\  payload BLOB NOT NULL,
    \\  created_at INTEGER NOT NULL,
    \\  PRIMARY KEY(stream_id, stream_revision)
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS projection_checkpoints (
    \\  projection_name TEXT PRIMARY KEY,
    \\  position INTEGER NOT NULL DEFAULT 0,
    \\  state BLOB
    \\);
    \\
    \\CREATE TABLE IF NOT EXISTS schema_info (
    \\  id INTEGER PRIMARY KEY CHECK (id = 1),
    \\  version INTEGER NOT NULL,
    \\  applied_at INTEGER NOT NULL
    \\);
;

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var iter = try std.process.Args.Iterator.initAllocator(init.args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    const path = iter.next() orelse "demo.db";

    // Open the DB
    var db: ?*c.sqlite3 = null;
    const rc = c.sqlite3_open(path.ptr, &db);
    if (rc != c.SQLITE_OK) {
        std.debug.print("cannot open {s}\n", .{path});
        std.process.exit(1);
    }
    defer _ = c.sqlite3_close(db.?);
    const d = db.?;

    // Apply the v2 schema.
    if (c.sqlite3_exec(d, schema_v2, null, null, null) != c.SQLITE_OK) {
        std.debug.print("schema apply failed: {s}\n", .{c.sqlite3_errmsg(d)});
        std.process.exit(1);
    }

    // Insert a few events so the per-table counts are non-zero.
    const inserts =
        \\INSERT INTO streams(stream_id, stream_revision, created_at, updated_at) VALUES
        \\  ('order-1', 3, 1700000000000, 1700000003000),
        \\  ('order-2', 1, 1700000010000, 1700000010000),
        \\  ('invoice-99', 0, 1700000020000, 1700000020000);
    ;
    _ = c.sqlite3_exec(d, inserts, null, null, null);

    const events_insert =
        \\INSERT INTO events(event_id, stream_id, event_number, event_type, data, log_position, created_at, sequence, tags, dc_time) VALUES
        \\  ('e1', 'order-1', 0, 'OrderCreated',   X'7B2274797065223A226F72646572227D', 1, 1700000000000, 1, '["a","b"]', 1700000000000),
        \\  ('e2', 'order-1', 1, 'ItemAdded',      X'7B2274797065223A226974656D227D', 2, 1700000001000, 2, '["a"]',     1700000001000),
        \\  ('e3', 'order-1', 2, 'OrderShipped',   X'7B2274797065223A2273686970227D', 3, 1700000002000, 3, NULL,        1700000002000),
        \\  ('e4', 'order-2', 0, 'OrderCreated',   X'7B2274797065223A226F72646572227D', 4, 1700000010000, 4, '["vip"]',   1700000010000);
    ;
    _ = c.sqlite3_exec(d, events_insert, null, null, null);

    std.debug.print("demo DB created at {s}\n", .{path});
}
