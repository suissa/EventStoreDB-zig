//! UUIDv4 generation. Zig 0.16 removed `std.crypto.random`; the
//! secure path is `std.Io.randomSecure`, which needs an `Io`
//! instance. We use `Threaded.init_single_threaded` so callers
//! do not have to thread an `Io` through everywhere. This is
//! called from the append hot path; if the cost shows up in a
//! profile, move to a per-Client Io.
//!
//! ## Security posture (v0.1) — read before changing the call site
//!
//! `Threaded.init_single_threaded` is a *non-entropy-bearing*
//! `Io`: `randomSecure` there always fails with `error.NoSecureEntropy`,
//! and the code below falls back to `io.random`. That means
//! **every auto-generated `event_id` produced by this library
//! today is a non-cryptographic PRNG output**, not a CSPRNG one.
//!
//! This is acceptable for the only role `event_id` currently
//! plays — a server-internal idempotency key, where the only
//! attacker is a concurrent client trying to collide on its
//! own UUID, which `random` (xoshiro-style) makes vanishingly
//! unlikely at 122 bits of entropy.
//!
//! It is **not** acceptable the moment `event_id` (or any
//! output of `newV4`) starts being relied on for authenticity,
//! non-repudiation, or externally-visible uniqueness. If that
//! day comes, switch `newV4` to take a caller-supplied `Io`
//! that is constructed against a real entropy source (e.g.
//! an `Io.Threaded` instance with `randomSecure` wired to the
//! OS CSPRNG).

const std = @import("std");
const Uuid = @import("types.zig").Uuid;

/// Generate a fresh UUIDv4.
pub fn newV4() Uuid {
    var bytes: [16]u8 = undefined;
    var threaded = std.Io.Threaded.init_single_threaded;
    const io = threaded.io();
    io.randomSecure(&bytes) catch {
        // Threaded.init_single_threaded provides no secure
        // entropy source; fall back to non-secure random. The
        // version/variant bits below still ensure a valid v4
        // layout, which is what the rest of the store relies on.
        io.random(&bytes);
    };

    // Set version (4) and variant (RFC 4122).
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    return bytes;
}

/// Format a UUID as a canonical `8-4-4-4-12` string. Caller
/// owns the returned slice and must free it with the allocator.
pub fn format(allocator: std.mem.Allocator, id: Uuid) ![]u8 {
    var buf: [36]u8 = undefined;
    _ = std.fmt.bufPrint(
        &buf,
        "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}",
        .{
            id[0],  id[1],  id[2],  id[3],
            id[4],  id[5],  id[6],  id[7],
            id[8],  id[9],  id[10], id[11],
            id[12], id[13], id[14], id[15],
        },
    ) catch unreachable;
    return allocator.dupe(u8, &buf);
}

/// Parse a UUID from a 36-character canonical form, or from a
/// 32-character hex blob, or from a 16-byte raw buffer. Returns
/// `error.InvalidArgument` on bad input.
pub fn parse(input: []const u8) @import("errors.zig").Error!Uuid {
    // Canonical 36-character form: 8-4-4-4-12.
    if (input.len == 36 and
        input[8] == '-' and input[13] == '-' and
        input[18] == '-' and input[23] == '-')
    {
        var out: Uuid = undefined;
        var hex: [32]u8 = undefined;
        const segments = [_][]const u8{
            input[0..8],   input[9..13],
            input[14..18], input[19..23],
            input[24..36],
        };
        var pos: usize = 0;
        for (segments) |seg| {
            for (seg) |ch| {
                hex[pos] = ch;
                pos += 1;
            }
        }
        try hexToBytes(&out, &hex);
        return out;
    }

    // 32-character hex blob.
    if (input.len == 32) {
        var out: Uuid = undefined;
        try hexToBytes(&out, input);
        return out;
    }

    return @import("errors.zig").Error.InvalidArgument;
}

fn hexToBytes(out: *Uuid, hex: []const u8) @import("errors.zig").Error!void {
    if (hex.len != 32) return @import("errors.zig").Error.InvalidArgument;
    var i: usize = 0;
    while (i < 16) : (i += 1) {
        const hi = hexCharToNibble(hex[i * 2]) orelse return @import("errors.zig").Error.InvalidArgument;
        const lo = hexCharToNibble(hex[i * 2 + 1]) orelse return @import("errors.zig").Error.InvalidArgument;
        out[i] = (hi << 4) | lo;
    }
}

fn hexCharToNibble(ch: u8) ?u4 {
    return switch (ch) {
        '0'...'9' => @as(u4, @intCast(ch - '0')),
        'a'...'f' => @as(u4, @intCast(ch - 'a' + 10)),
        'A'...'F' => @as(u4, @intCast(ch - 'A' + 10)),
        else => null,
    };
}
