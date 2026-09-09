// eventstoredb-zig CLI.
//
//   stats --data ./eventstore.db
//   tail  --data ./eventstore.db [--stream id]
//
// `stats` prints a JSON snapshot of the store.
// `tail` streams live events (Ctrl-C to stop).
//
// Single-connection design: this CLI is intentionally one-shot.
// `stats` opens one Client, runs one query, prints, exits.
// `tail` opens one Client, runs one subscription, drains until
// SIGINT, exits. There is no listener and no fan-out — the store
// itself is shared via the SQLite file, not via a network socket.
// If you need concurrent consumers, run multiple instances or
// embed `eventstoredb-zig` as a library in your own process.
// (The HTTP server is exposed as a library API; embed it in
//  your own Zig program to serve over the network.)

const std = @import("std");
const esdb = @import("eventstoredb");

const usage =
    \\eventstoredb-zig — embedded EventStoreDB over SQLite
    \\
    \\Usage:
    \\  eventstoredb-zig <command> [flags]
    \\
    \\Commands:
    \\  stats [--data PATH]
    \\  tail  [--data PATH] [--stream id]
    \\  version
    \\  help
    \\
    \\Run "eventstoredb-zig <command> -h" for command-specific flags.
;

pub fn main(init: std.process.Init) !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = init.io;

    var args_arena = std.heap.ArenaAllocator.init(allocator);
    defer args_arena.deinit();
    const aa = args_arena.allocator();

    var argv = try aa.alloc([]const u8, 0);
    var it = try init.minimal.args.iterateAllocator(allocator);
    defer it.deinit();
    while (it.next()) |arg| {
        argv = try aa.realloc(argv, argv.len + 1);
        argv[argv.len - 1] = try allocator.dupeZ(u8, arg);
    }

    if (argv.len < 2) {
        try std.Io.File.writeStreamingAll(.stdout(), io, usage);
        return;
    }

    const cmd = argv[1];
    if (std.mem.eql(u8, cmd, "stats")) {
        try runStats(allocator, io, argv[2..]);
    } else if (std.mem.eql(u8, cmd, "tail")) {
        try runTail(allocator, io, argv[2..]);
    } else if (std.mem.eql(u8, cmd, "version") or std.mem.eql(u8, cmd, "-v")) {
        try std.Io.File.writeStreamingAll(.stdout(), io, "eventstoredb-zig 0.1.0\n");
    } else if (std.mem.eql(u8, cmd, "help") or std.mem.eql(u8, cmd, "-h")) {
        try std.Io.File.writeStreamingAll(.stdout(), io, usage);
    } else {
        std.log.err("unknown command: {s}\n", .{cmd});
        try std.Io.File.writeStreamingAll(.stderr(), io, usage);
        std.process.exit(2);
    }
}

fn runStats(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const data = parseDataPath(args) orelse "./eventstore.db";
    var client = try esdb.Client.open(allocator, .{ .path = data });
    defer client.close();

    const stats = try client.stats();
    const out = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "stream_count": {d},
        \\  "event_count": {d},
        \\  "tombstoned_streams": {d},
        \\  "persistent_groups": {d},
        \\  "snapshots": {d},
        \\  "db_size_bytes": {d},
        \\  "last_log_position": {d}
        \\}}
    , .{
        stats.stream_count,
        stats.event_count,
        stats.tombstoned_streams,
        stats.persistent_groups,
        stats.snapshots,
        stats.db_size_bytes,
        stats.last_log_position,
    });
    defer allocator.free(out);
    try std.Io.File.writeStreamingAll(.stdout(), io, out);
}

fn runTail(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const data = parseDataPath(args) orelse "./eventstore.db";
    const stream = parseStreamArg(args);

    var client = try esdb.Client.open(allocator, .{ .path = data });
    defer client.close();

    var sub = if (stream) |s|
        try esdb.subscribeToStream(client, allocator, s, .{ .from = .start, .buffer_size = 64 })
    else
        try esdb.subscribeToAll(client, allocator, .{ .from = .start, .buffer_size = 64 });
    defer sub.close();

    while (sub.receive()) |next| {
        switch (next) {
            .event => |ev| {
                const line = try std.fmt.allocPrint(allocator, "{{\"stream_id\":\"{s}\",\"revision\":{d},\"log_position\":{d},\"event_type\":\"{s}\",\"data\":\"{s}\"}}\n", .{ ev.stream_id, ev.revision, ev.log_position, ev.event_type, ev.data });
                defer allocator.free(line);
                try std.Io.File.writeStreamingAll(.stdout(), io, line);
            },
            .err => |e| {
                const err_msg = try std.fmt.allocPrint(allocator, "error: {any}\n", .{e});
                defer allocator.free(err_msg);
                try std.Io.File.writeStreamingAll(.stdout(), io, err_msg);
            },
            .closed => break,
        }
    }
}

fn parseDataPath(args: []const []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data")) return args[i + 1];
    }
    return null;
}

fn parseStreamArg(args: []const []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--stream")) return args[i + 1];
    }
    return null;
}
