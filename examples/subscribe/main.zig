// Subscribe example: produce N demo events with -produce, then
// in another shell tail the stream with the default mode.
//
//   zig build examples
//   ./zig-out/examples/subscribe -data ./demo.db -stream demo -produce
//   ./zig-out/examples/subscribe -data ./demo.db -stream demo

const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    var data_path: []const u8 = "./demo.db";
    var stream_id: []const u8 = "demo";
    var produce = false;
    var from_rev: u64 = 0;

    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "-data")) {
            i += 1;
            data_path = argv[i];
        } else if (std.mem.eql(u8, a, "-stream")) {
            i += 1;
            stream_id = argv[i];
        } else if (std.mem.eql(u8, a, "-produce")) {
            produce = true;
        } else if (std.mem.eql(u8, a, "-from")) {
            i += 1;
            from_rev = try std.fmt.parseInt(u64, argv[i], 10);
        }
    }

    var client = try esdb.Client.open(allocator, .{ .path = data_path });
    defer client.close();

    if (produce) {
        var j: u8 = 0;
        while (j < 5) : (j += 1) {
            var exp: esdb.ExpectedRevision = .no_stream;
            if (j > 0) exp = .stream_exists;
            const data = try std.fmt.allocPrint(allocator, "{{\"i\":{d}}}", .{j});
            defer allocator.free(data);
            _ = try client.appendToStream(allocator, stream_id, .{ .expected_revision = exp }, &[_]esdb.EventData{
                .{ .event_type = "DemoEvent", .data = data },
            });
            std.time.sleep(200 * std.time.ns_per_ms);
        }
        try std.Io.File.writeStreamingAll(.stdout(), io, "produced 5 events\n");
        return;
    }

    var sub = try client.subscribeToStream(allocator, stream_id, .{
        .from = .{ .revision = from_rev },
        .buffer_size = 64,
    });
    defer sub.close();

    const head = try std.fmt.allocPrint(allocator, "subscribed to {s} (from rev {d})\n", .{ stream_id, from_rev });
    defer allocator.free(head);
    try std.Io.File.writeStreamingAll(.stdout(), io, head);

    while (sub.receive()) |next| {
        switch (next) {
            .event => |ev| {
                const line = try std.fmt.allocPrint(allocator, "  rev={d}  pos={d}  type={s:<12}  data={s}\n", .{ ev.revision, ev.log_position, ev.event_type, ev.data });
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


