// Minimal HTTP/1.1 server for the CLI. Handles only the routes
// needed by the README; everything else returns 404. We parse
// the request line by hand to keep the binary small.

const std = @import("std");
const esdb = @import("eventstoredb");

pub const Server = struct {
    allocator: std.mem.Allocator,
    client: *esdb.Client,

    pub fn init(allocator: std.mem.Allocator, client: *esdb.Client) Server {
        return .{ .allocator = allocator, .client = client };
    }

    pub fn deinit(_: *Server) void {}
};

pub const ConnectionCtx = struct {
    client: *esdb.Client,
    allocator: std.mem.Allocator,
};

pub fn handleConn(ctx: *ConnectionCtx, stream: std.net.Stream) void {
    defer stream.close();
    defer ctx.allocator.destroy(ctx);

    var buf: [4096]u8 = undefined;
    const n = stream.read(&buf) catch return;
    const request = buf[0..n];

    // Parse the first line.
    var line_end: usize = 0;
    while (line_end < request.len and request[line_end] != '\n') : (line_end += 1) {}
    const first_line = std.mem.trimRight(u8, request[0..line_end], " \r");
    var parts = std.mem.splitScalar(u8, first_line, ' ');
    const method = parts.next() orelse return;
    const path = parts.next() orelse return;
    _ = parts.next();

    var response_buf: [65536]u8 = undefined;
    var w = std.io.fixedBufferStream(&response_buf);
    const writer = w.writer();

    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/healthz")) {
        writer.writeAll("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 16\r\n\r\n{\"status\":\"ok\"}") catch return;
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/stats")) {
        const stats = ctx.client.stats() catch {
            writer.writeAll("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n") catch return;
            stream.writeAll(w.getWritten()) catch return;
            return;
        };
        const body = std.fmt.bufPrint(
            &response_buf[2048..],
            "{{\"stream_count\":{d},\"event_count\":{d},\"tombstoned_streams\":{d},\"persistent_groups\":{d},\"snapshots\":{d},\"db_size_bytes\":{d},\"last_log_position\":{d}}}",
            .{ stats.stream_count, stats.event_count, stats.tombstoned_streams, stats.persistent_groups, stats.snapshots, stats.db_size_bytes, stats.last_log_position },
        ) catch return;
        const header = std.fmt.bufPrint(
            &response_buf[0..2048],
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\n\r\n",
            .{body.len},
        ) catch return;
        writer.writeAll(header) catch return;
        writer.writeAll(body) catch return;
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/streams/")) {
        const rest = path["/streams/".len..];
        handleStream(ctx, writer, rest, request) catch return;
    } else {
        writer.writeAll("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n") catch return;
    }

    stream.writeAll(w.getWritten()) catch return;
}

fn handleStream(ctx: *ConnectionCtx, writer: anytype, rest: []const u8, full_request: []const u8) !void {
    _ = full_request;
    if (std.mem.eql(u8, rest, "")) {
        const list = ctx.client.listStreams(ctx.allocator, 1000, 0) catch {
            try writer.writeAll("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
            return;
        };
        defer ctx.allocator.free(list);
        try writer.writeAll("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n[");
        for (list, 0..) |s, i| {
            if (i > 0) try writer.writeAll(",");
            try writer.print("{{\"stream_id\":\"{s}\",\"revision\":{d}}}", .{ s.stream_id, s.revision });
        }
        try writer.writeAll("]");
    }
}
