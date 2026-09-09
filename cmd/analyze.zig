//! SQLite analyzer — a single-file CLI that exercises every
//! diagnostic surface the C API exposes. Intended for poking at
//! a running event store to answer questions like: which version
//! am I on, what pragmas are in effect, how full is the file, what
//! does the planner do with my queries, and is the database
//! consistent.
//!
//! Usage:
//!   analyze [path]
//!   analyze               # opens :memory:
//!   analyze ./store.db    # opens a file
//!
//! The output is human-readable plain text. Nothing writes to the
//! database; everything is read-only or diagnostic.

const std = @import("std");
const c = @cImport(@cInclude("sqlite3.h"));

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var iter = try std.process.Args.Iterator.initAllocator(init.args, allocator);
    defer iter.deinit();
    _ = iter.skip(); // skip argv[0]
    const path = iter.next() orelse ":memory:";

    var db: ?*c.sqlite3 = null;
    const flags: c_int = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_URI;
    const rc = c.sqlite3_open_v2(path.ptr, &db, flags, null);
    if (rc != c.SQLITE_OK) {
        std.debug.print("error: cannot open '{s}': {s}\n", .{ path, c.sqlite3_errmsg(db) });
        if (db) |d| _ = c.sqlite3_close(d);
        std.process.exit(1);
    }
    defer _ = c.sqlite3_close(db.?);

    try printBanner(allocator, db.?, path);
    try printCompileOptions(allocator, db.?);
    try printPragmas(allocator, db.?);
    try printAttachedDatabases(allocator, db.?);
    try printSqliteMaster(allocator, db.?);
    try printPerTable(allocator, db.?);
    try printFileSize(allocator, db.?);
    try printQueryPlans(allocator, db.?);
    try printFunctionList(allocator, db.?);
    try printJsonDemos(allocator, db.?);
    try printMathDemos(allocator, db.?);
    try printDateDemos(allocator, db.?);
    try printWindowAndCte(allocator, db.?);
    try printIntegrity(allocator, db.?);
    try printEventStoreReport(allocator, db.?);
}

fn printBanner(_: std.mem.Allocator, db: *c.sqlite3, path: []const u8) !void {
    std.debug.print("\n=== eventstoredb-zig SQLite analyzer ===\n", .{});
    std.debug.print("path: {s}\n", .{path});
    if (c.sqlite3_db_filename(db, "??")) |p| {
        const resolved = std.mem.span(p);
        if (!std.mem.eql(u8, resolved, "??")) std.debug.print("sqlite3_db_filename: {s}\n", .{resolved});
    }
    if (c.sqlite3_db_name(db, 0)) |n| std.debug.print("default schema: {s}\n", .{std.mem.span(n)});
    std.debug.print("\n", .{});
}

fn printCompileOptions(_: std.mem.Allocator, _: *c.sqlite3) !void {
    std.debug.print("--- compile-time info ---\n", .{});
    std.debug.print("sqlite_version:        {s}\n", .{c.sqlite3_libversion()});
    std.debug.print("sqlite_source_id:      {s}\n", .{c.sqlite3_sourceid()});
    std.debug.print("sqlite_version_number: {d}\n", .{c.sqlite3_libversion_number()});
    // SQLITE_COMPILEOPTION_GETOPTIONS = -1
    {
        std.debug.print("compile_options (iterated):\n", .{});
        var i: c_int = 0;
        while (true) : (i += 1) {
            const opt = c.sqlite3_compileoption_get(i);
            if (opt == null) break;
            const opt_slice = std.mem.span(opt.? orelse break);
            std.debug.print("  [{d:3}] {s}\n", .{ i, opt_slice });
        }
    }
    std.debug.print("compile_options(SQLITE_ENABLE_FTS5): {d}\n", .{c.sqlite3_compileoption_used("SQLITE_ENABLE_FTS5")});
    std.debug.print("compile_options(SQLITE_OMIT_WINDOWFUNC): {d}\n", .{c.sqlite3_compileoption_used("SQLITE_OMIT_WINDOWFUNC")});
    std.debug.print("compile_options(SQLITE_ENABLE_JSON1): {d}\n", .{c.sqlite3_compileoption_used("SQLITE_ENABLE_JSON1")});
    std.debug.print("\n", .{});
}

const pragma_rows = [_][]const u8{
    "application_id",
    "user_version",
    "schema_version",
    "journal_mode",
    "synchronous",
    "foreign_keys",
    "auto_vacuum",
    "incremental_vacuum",
    "temp_store",
    "page_size",
    "page_count",
    "cache_size",
    "busy_timeout",
    "locking_mode",
    "wal_autocheckpoint",
    "secure_delete",
    "integrity_check",
    "quick_check",
    "compile_options",
};

fn printPragmas(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- pragmas ---\n", .{});
    var stmt: ?*c.sqlite3_stmt = null;
    for (pragma_rows) |name| {
        defer finalize(allocator, stmt);
        var sql_buf: [256]u8 = undefined;
        const sql_str = std.fmt.bufPrint(&sql_buf, "PRAGMA {s}", .{name}) catch unreachable;
        const sql_z = try allocator.dupeZ(u8, sql_str);
        defer allocator.free(sql_z);
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) continue;
        if (c.sqlite3_step(stmt) != c.SQLITE_ROW) continue;
        const val = c.sqlite3_column_text(stmt, 0);
        const txt = if (val) |v| std.mem.span(v) else "(null)";
        std.debug.print("  {s:24} = {s}\n", .{ name, txt });
    }
    std.debug.print("\n", .{});
}

fn printAttachedDatabases(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- attached databases ---\n", .{});
    var stmt: ?*c.sqlite3_stmt = null;
    const sql_z = try allocator.dupeZ(u8, "PRAGMA database_list");
    defer allocator.free(sql_z);
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) return;
    defer finalize(allocator, stmt);
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const seq = c.sqlite3_column_int(stmt, 0);
        const name = colText(stmt, 1);
        const file = colText(stmt, 2);
        std.debug.print("  [{d}] name={s} file={s}\n", .{ seq, name, file });
    }
    std.debug.print("\n", .{});
}

fn colText(stmt: ?*c.sqlite3_stmt, col: c_int) []const u8 {
    const val = c.sqlite3_column_text(stmt, col);
    return if (val) |v| std.mem.span(v) else "";
}

fn printSqliteMaster(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- sqlite_master overview ---\n", .{});
    // Counts by kind.
    {
        const sql_z = try allocator.dupeZ(u8,
            \\SELECT type, COUNT(*) FROM sqlite_master GROUP BY type ORDER BY type
        );
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            std.debug.print("  counts by kind:\n", .{});
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("    {s:10} = {d}\n", .{ colText(stmt, 0), c.sqlite3_column_int(stmt, 1) });
            }
        }
    }
    // Full list ordered by kind, name.
    {
        const sql_z = try allocator.dupeZ(u8,
            \\SELECT type, name, tbl_name, sql FROM sqlite_master
            \\ORDER BY type, name
        );
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            std.debug.print("  full list:\n", .{});
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const kind = colText(stmt, 0);
                const name = colText(stmt, 1);
                const tbl = colText(stmt, 2);
                std.debug.print("    [{s}] {s} (table={s})\n", .{ kind, name, tbl });
            }
        }
    }
    // json_group_array of all DDL.
    {
        const sql_z = try allocator.dupeZ(u8,
            \\SELECT json_group_array(json_object(
            \\  'type', type, 'name', name, 'tbl_name', tbl_name, 'sql', sql
            \\)) FROM sqlite_master
        );
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const j = colText(stmt, 0);
                std.debug.print("  json_group_array length: {d} bytes\n", .{j.len});
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printPerTable(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- per-table info ---\n", .{});

    var stmt: ?*c.sqlite3_stmt = null;
    const sql_z = try allocator.dupeZ(u8,
        \\SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
        \\ORDER BY name
    );
    defer allocator.free(sql_z);
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) return;
    defer finalize(allocator, stmt);

    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const name = colText(stmt, 0);
        std.debug.print("\n  table: {s}\n", .{name});
        try dumpTableColumns(allocator, db, name);
        try dumpTableIndexes(allocator, db, name);
        try dumpTableCount(allocator, db, name);
    }
    std.debug.print("\n", .{});
}

fn dumpTableColumns(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !void {
        var tibuf: [256]u8 = undefined;
    const tistr = std.fmt.bufPrint(&tibuf, "PRAGMA table_info({s})", .{name}) catch unreachable;
    const sql_z = try allocator.dupeZ(u8, tistr);
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) return;
    defer finalize(allocator, stmt);
    std.debug.print("    columns:\n", .{});
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const cid = c.sqlite3_column_int(stmt, 0);
        const col_name = colText(stmt, 1);
        const col_type = colText(stmt, 2);
        const notnull = c.sqlite3_column_int(stmt, 3) != 0;
        const pk = c.sqlite3_column_int(stmt, 5);
        std.debug.print("      [{d}] {s:24} {s}{s}{s}\n", .{
            cid,
            col_name,
            col_type,
            if (pk > 0) " PRIMARY KEY" else "",
            if (notnull) " NOT NULL" else "",
        });
    }
}

fn dumpTableIndexes(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !void {
        var ilbuf: [256]u8 = undefined;
    const ilstr = std.fmt.bufPrint(&ilbuf, "PRAGMA index_list({s})", .{name}) catch unreachable;
    const sql_z = try allocator.dupeZ(u8, ilstr);
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) return;
    defer finalize(allocator, stmt);
    std.debug.print("    indexes:\n", .{});
    while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const seq = c.sqlite3_column_int(stmt, 0);
        const idx_name = colText(stmt, 1);
        const unique = c.sqlite3_column_int(stmt, 2) != 0;
        const origin = colText(stmt, 3);
        const partial = c.sqlite3_column_int(stmt, 4) != 0;
        std.debug.print("      [{d}] {s:32} unique={any} origin={s} partial={any}\n", .{
            seq, idx_name, unique, origin, partial,
        });
    }
}

fn dumpTableCount(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !void {
        var ccbuf: [256]u8 = undefined;
    const ccstr = std.fmt.bufPrint(&ccbuf, "SELECT COUNT(*) FROM {s}", .{name}) catch unreachable;
    const sql_z = try allocator.dupeZ(u8, ccstr);
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) != c.SQLITE_OK) return;
    defer finalize(allocator, stmt);
    if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
        const n = c.sqlite3_column_int64(stmt, 0);
        std.debug.print("    row_count: {d}\n", .{n});
    }
}

fn printFileSize(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- file metrics ---\n", .{});

    inline for (.{ "page_count", "page_size", "freelist_count", "max_page_count" }) |pr| {
        const sql_z = try allocator.dupeZ(u8, "PRAGMA " ++ pr);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const v = c.sqlite3_column_int64(stmt, 0);
                std.debug.print("  {s:16} = {d}\n", .{ pr, v });
            }
        }
    }

    const sql_z = try allocator.dupeZ(u8,
        \\SELECT CAST(page_count AS INTEGER) * page_size AS bytes_used
        \\FROM pragma_page_count(), pragma_page_size()
    );
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
        defer finalize(allocator, stmt);
        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const v = c.sqlite3_column_int64(stmt, 0);
            std.debug.print("  bytes_used       = {d}\n", .{v});
        }
    }
    std.debug.print("\n", .{});
}

const queries_to_explain = [_][]const u8{
    "SELECT event_id, stream_id, event_type FROM events WHERE stream_id = 's1' ORDER BY event_number",
    "SELECT * FROM events WHERE log_position >= ?",
    "SELECT stream_id, COUNT(*) FROM events GROUP BY stream_id HAVING COUNT(*) > 1",
    "SELECT * FROM events WHERE event_type = 'X' AND stream_id = 's'",
};

fn printQueryPlans(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- EXPLAIN QUERY PLAN ---\n", .{});
    for (queries_to_explain) |q| {
        std.debug.print("\n  Q: {s}\n", .{q});
        var epbuf: [512]u8 = undefined;
        const epstr = std.fmt.bufPrint(&epbuf, "EXPLAIN QUERY PLAN {s}", .{q}) catch unreachable;
        const sql_z = try allocator.dupeZ(u8, epstr);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const id = c.sqlite3_column_int(stmt, 0);
                const parent = c.sqlite3_column_int(stmt, 1);
                const notused = c.sqlite3_column_int(stmt, 2);
                std.debug.print("    id={d} parent={d} notused={d} detail={s}\n", .{
                    id, parent, notused, colText(stmt, 3),
                });
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printFunctionList(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- registered functions (first 30) ---\n", .{});
    const sql_z = try allocator.dupeZ(u8,
        \\SELECT name, builtin, type, enc FROM pragma_function_list ORDER BY name LIMIT 30
    );
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
        defer finalize(allocator, stmt);
        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            std.debug.print("  {s:24} builtin={any} type={s} enc={s}\n", .{
                colText(stmt, 0),
                c.sqlite3_column_int(stmt, 1) != 0,
                colText(stmt, 2),
                colText(stmt, 3),
            });
        }
    }
    std.debug.print("\n", .{});
}

fn printJsonDemos(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- json1 demos ---\n", .{});
    const demos = [_][]const u8{
        \\SELECT json_object('a', 1, 'b', 'two', 'nested', json_object('x', 10, 'y', 20))
        ,
        \\SELECT json_extract('{"a":1,"b":{"c":3}}', '$.b.c')
        ,
        \\SELECT json_array_length('[1,2,3,4,5]')
        ,
        \\SELECT json_group_array(value) FROM json_each('["a","b","c"]')
        ,
    };
    for (demos) |q| {
        std.debug.print("  Q: {s}\n", .{q});
        const sql_z = try allocator.dupeZ(u8, q);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("    -> {s}\n", .{colText(stmt, 0)});
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printMathDemos(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- math demos ---\n", .{});
    const demos = [_][]const u8{
        "SELECT 2+2, 2*3, 7%3, 10/3, CAST(10 AS REAL)/3",
        "SELECT abs(-5), max(1,2,3), min(1,2,3), round(3.7), random()",
        "SELECT pi(), pow(2, 10), sqrt(2), exp(1), log(2.71828), sin(0)",
        "SELECT unicode('A'), char(65), hex(255), quote('it''s')",
    };
    for (demos) |q| {
        std.debug.print("  Q: {s}\n", .{q});
        const sql_z = try allocator.dupeZ(u8, q);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("    -> {s}\n", .{colText(stmt, 0)});
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printDateDemos(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- date/time demos ---\n", .{});
    const demos = [_][]const u8{
        "SELECT date('now'), time('now'), datetime('now'), julianday('now'), unixepoch('now')",
        "SELECT date('now', '+1 day'), date('now', '-1 month'), date('now', 'start of month')",
        "SELECT strftime('%Y-%m-%d %H:%M:%S', 'now'), strftime('%W', 'now')",
    };
    for (demos) |q| {
        std.debug.print("  Q: {s}\n", .{q});
        const sql_z = try allocator.dupeZ(u8, q);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("    -> {s}\n", .{colText(stmt, 0)});
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printWindowAndCte(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- window functions & CTEs ---\n", .{});
    // CTE that returns a small synthetic table.
    const cte_sql =
        \\WITH RECURSIVE cnt(x) AS (
        \\  SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5
        \\)
        \\SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum
        \\FROM cnt
    ;
    std.debug.print("  Q: {s}\n", .{cte_sql});
    const sql_z = try allocator.dupeZ(u8, cte_sql);
    defer allocator.free(sql_z);
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
        defer finalize(allocator, stmt);
        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            std.debug.print("    x={d} cum={s}\n", .{ c.sqlite3_column_int(stmt, 0), colText(stmt, 1) });
        }
    }
    std.debug.print("\n", .{});
}

fn printIntegrity(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- integrity checks ---\n", .{});
    const checks = [_][]const u8{
        "PRAGMA integrity_check",
        "PRAGMA quick_check",
        "PRAGMA foreign_key_check",
    };
    for (checks) |c_sql| {
        std.debug.print("  {s}: ", .{c_sql});
        const sql_z = try allocator.dupeZ(u8, c_sql);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            // First row of integrity_check is the overall result.
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("{s}\n", .{colText(stmt, 0)});
                // Subsequent rows are per-failure detail; surface
                // the first few.
                var n: u8 = 0;
                while (c.sqlite3_step(stmt) == c.SQLITE_ROW) : (n += 1) {
                    if (n > 4) break;
                    std.debug.print("      {s}\n", .{colText(stmt, 0)});
                }
            }
        }
    }
    std.debug.print("\n", .{});
}

fn printEventStoreReport(allocator: std.mem.Allocator, db: *c.sqlite3) !void {
        std.debug.print("--- event store domain report ---\n", .{});

    const expected_tables = [_][]const u8{
        "streams", "events", "committed_event_ids",
        "persistent_subscriptions", "persistent_acks",
        "snapshots", "projection_checkpoints", "schema_info",
    };
    for (expected_tables) |t| {
        var etbuf: [256]u8 = undefined;
        const etstr = std.fmt.bufPrint(&etbuf, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{s}'", .{t}) catch unreachable;
        const sql_z = try allocator.dupeZ(u8, etstr);
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const n = c.sqlite3_column_int(stmt, 0);
                const mark = if (n == 1) "OK" else "MISSING";
                std.debug.print("  table {s:28} = {s}\n", .{ t, mark });
            }
        }
    }
    // DCB-specific check: are the v2 columns present?
    {
        const sql_z = try allocator.dupeZ(u8,
            \\SELECT COUNT(*) FROM pragma_table_info('events')
            \\WHERE name IN ('sequence', 'tags', 'dc_time')
        );
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                const n = c.sqlite3_column_int(stmt, 0);
                const mark = if (n == 3) "OK (v2)" else if (n == 0) "v1 schema" else "PARTIAL";
                std.debug.print("  DCB columns (sequence, tags, dc_time): {s}\n", .{mark});
            }
        }
    }
    // Per-table row counts in one round-trip with a CTE.
    {
        const sql_z = try allocator.dupeZ(u8,
            \\SELECT 'streams', COUNT(*) FROM streams
            \\UNION ALL SELECT 'events', COUNT(*) FROM events
            \\UNION ALL SELECT 'persistent_subscriptions', COUNT(*) FROM persistent_subscriptions
            \\UNION ALL SELECT 'snapshots', COUNT(*) FROM snapshots
            \\UNION ALL SELECT 'projection_checkpoints', COUNT(*) FROM projection_checkpoints
        );
        defer allocator.free(sql_z);
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql_z.ptr, -1, &stmt, null) == c.SQLITE_OK) {
            defer finalize(allocator, stmt);
            std.debug.print("  row counts (one round-trip):\n", .{});
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                std.debug.print("    {s:28} = {d}\n", .{ colText(stmt, 0), c.sqlite3_column_int64(stmt, 1) });
            }
        }
    }
    std.debug.print("\n=== end of report ===\n", .{});
}

fn finalize(allocator: std.mem.Allocator, stmt: ?*c.sqlite3_stmt) void {
    if (stmt) |s| _ = c.sqlite3_finalize(s);
    _ = allocator;
}
