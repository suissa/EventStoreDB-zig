# eventstoredb-zig

> **EventStoreDB-compatible event store over SQLite — embedded, single-binary, zero-ops, written in Zig.**

Zig port of the same idea behind `eventstoredb-sqlite`: implements the
EventStoreDB programming model (streams, events, expected revision,
persistent subscriptions, snapshots, projections, tombstones) on top of a
single SQLite file. Designed as a **drop-in for local development, edge
deployments, agents and tests** — anywhere you would reach for
EventStoreDB but don't want to operate a separate server.

> ⚠️ **v0.1 — work in progress.** This is a fresh rewrite in Zig; the
> surface is intentionally close to the Go version but a few
> niceties (filter subscriptions, multi-stream transactions) are
> not yet implemented. See the inline TODO comments and the
> compatibility matrix below.

---

## Why Zig

- **Single static binary** — no runtime, no glibc mismatch, no `cgo` pain.
- **No external SQLite dependency** — the amalgamation is vendored and
  statically linked.
- **Manual memory management** makes subscription lifetimes obvious and
  zero-cost.
- **Build cross-compiles** to Windows / Linux / macOS from any host
  (try `zig build -Dtarget=x86_64-linux-gnu`).

---

## Quick start

```zig
const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var client = try esdb.Client.open(allocator, .{
        .path = ":memory:",
    });
    defer client.close();

    const result = try client.appendToStream(
        allocator,
        "orders-1",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{
            .{ .event_type = "OrderCreated", .data = "{\"id\":\"1\"}" },
            .{ .event_type = "OrderItemAdded", .data = "{\"sku\":\"A\"}" },
        },
    );
    std.debug.print("next revision: {d}\n", .{result.next_revision});

    const page = try client.readStream(
        allocator,
        "orders-1",
        .{ .from = .start, .direction = .forward, .limit = 100 },
    );
    for (page.events) |ev| {
        std.debug.print("  rev={d} type={s}\n", .{ ev.revision, ev.event_type });
    }
}
```

## Build

```bash
zig build              # compiles the library, CLI and examples
zig build test         # runs the test suite
zig build run          # runs the CLI server on :2113 with ./eventstore.db
zig build examples     # builds all example binaries into zig-out/examples/
```

The first build compiles the vendored SQLite amalgamation (`vendor/sqlite/c/sqlite3.c`,
~9 MB). Subsequent builds reuse the cache.

## CLI

```bash
# Start a local server
zig build run -- --data ./data/eventstore.db --listen :2113

# Inspect
./zig-out/bin/eventstoredb-zig stats --data ./data/eventstore.db

# Tail events live (Ctrl-C to stop)
./zig-out/bin/eventstoredb-zig tail --data ./data/eventstore.db --stream orders-1
```

## HTTP API

| Method | Path | Body | Description |
| --- | --- | --- | --- |
| GET | `/healthz` | — | health check |
| GET | `/stats` | — | store statistics (JSON) |
| GET | `/streams` | — | list streams (capped) |
| GET | `/streams/{id}/info` | — | per-stream metadata |
| POST | `/streams/{id}/append` | `{"expectedRevision":"no_stream","events":[{...}]}` | append events |
| GET | `/streams/{id}/read?from=0&limit=10` | — | read a page of events |
| DELETE | `/streams/{id}` | — | soft-delete a stream |
| GET | `/subscribe/all` | — | newline-delimited JSON stream of all events |
| GET | `/subscribe/stream/{id}?from=0` | — | newline-delimited JSON stream of one stream |
| POST | `/persistent/{stream}/{group}/create` | `{"from":0}` | create a group |
| GET | `/persistent/{stream}/{group}/connect` | — | connect to a group (NDJSON) |
| DELETE | `/persistent/{stream}/{group}` | — | delete a group |
| POST | `/snapshots/{stream}` | `{"revision":42,"payload":{...}}` | save snapshot |
| GET | `/snapshots/{stream}?revision=0` | — | load snapshot |

`expectedRevision` accepts `any`, `no_stream`, `stream_exists`, or a
numeric revision.

## Architecture

```
   ┌─────────────────────────────────────────────┐
   │  Your Zig app                               │
   │  ┌──────────────────────────────────────┐   │
   │  │  eventstoredb.Client                 │   │
   │  │   ├─ Open / Close / Stats            │   │
   │  │   ├─ Append / Read / Subscribe       │   │
   │  │   ├─ Persistent Subscriptions        │   │
   │  │   └─ Snapshots / Meta                │   │
   │  └────────────┬─────────────────────────┘   │
   │               │  cImport → sqlite3            │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  vendored SQLite amalgamation        │   │
   │  └────────────┬─────────────────────────┘   │
   │               │                              │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  SQLite file (WAL, mmap, NORMAL sync)│   │
   │  │   ├─ streams                          │   │
   │  │   ├─ events (heart)                  │   │
   │  │   ├─ committed_event_ids (idempotency)│   │
   │  │   ├─ persistent_subscriptions        │   │
   │  │   ├─ persistent_acks                 │   │
   │  │   ├─ snapshots                       │   │
   │  │   └─ schema_info                     │   │
   │  └──────────────────────────────────────┘   │
   └─────────────────────────────────────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full schema,
concurrency story, and the per-operation SQL.

## Compatibility

The Zig API is intentionally close to the Go API in
`eventstoredb-sqlite`. The HTTP server is the same shape. See
[docs/EVENTSTOREDB-COMPATIBILITY.md](docs/EVENTSTOREDB-COMPATIBILITY.md)
for the full matrix.

## Layout

```
eventstoredb-zig/
├── build.zig, build.zig.zon        — package manifest
├── vendor/sqlite/                  — vendored amalgamation + sub-build
├── src/                            — public library (this is what users import)
│   ├── root.zig
│   ├── types.zig
│   ├── errors.zig
│   ├── schema.zig
│   ├── client.zig
│   ├── append.zig
│   ├── read.zig
│   ├── subscribe.zig
│   ├── persistent.zig
│   ├── snapshot.zig
│   ├── meta.zig
│   ├── waker.zig
│   └── uuid.zig
├── cmd/server.zig                  — CLI binary (serve, stats, tail)
├── examples/                       — basic, subscribe, persistent, snapshots
├── tests/                          — Zig test suite (run with `zig build test`)
└── docs/
    ├── ARCHITECTURE.md
    └── EVENTSTOREDB-COMPATIBILITY.md
```

## Development

```bash
zig build           # build everything
zig build test      # run the test suite
zig fmt src/        # format the source tree
zig build run       # build and run the CLI server
```

## License

MIT for our code. The vendored SQLite amalgamation is in the
public domain. See [LICENSE](LICENSE) for details.
