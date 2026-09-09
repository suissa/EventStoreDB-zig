# eventstoredb-zig

> **EventStoreDB-compatible event store over SQLite — embedded, single-binary, zero-ops, written in Zig.**

A Zig port of the same idea behind [`eventstoredb-sqlite`](../eventstoredb-sqlite/): implement the
EventStoreDB programming model — streams, events, expected revision,
persistent subscriptions, snapshots, projections, tombstones — on top of
a single SQLite file. Designed as a **drop-in for local development, edge
deployments, agents and tests**: anywhere you would reach for
EventStoreDB but don't want to operate a separate server.

> ⚠️ **v0.1 — work in progress.** The public API is close to the Go
> version, but filter subscriptions, multi-stream transactions, and the
> HTTP server are not implemented yet. See the *Status* section below
> for the per-feature matrix.

---

## Why Zig

- **Single static binary** — no runtime, no glibc mismatch, no `cgo` pain.
- **No external SQLite dependency** — the amalgamation is vendored
  (`vendor/sqlite/c/sqlite3.c`, ~9 MB) and statically linked into every
  build artifact.
- **Manual memory management** makes subscription lifetimes obvious
  and zero-cost.
- **Build cross-compiles** to Windows / Linux / macOS from any host
  (try `zig build -Dtarget=x86_64-linux-gnu`).
- **Builds on Zig 0.16** with `std.Build`'s module-based API.

---

## Quick start

```zig
const std = @import("std");
const esdb = @import("eventstoredb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const client = try esdb.Client.open(allocator, .{
        .path = ":memory:",
        .busy_timeout_ms = 5000,
    });
    defer client.close();

    // Append two events to a new stream in one transaction.
    const result = try esdb.appendToStream(
        client,
        allocator,
        "orders-1",
        .{ .expected_revision = .no_stream },
        &[_]esdb.EventData{
            .{ .event_type = "OrderCreated", .data = "{\"id\":\"1\"}" },
            .{ .event_type = "OrderItemAdded", .data = "{\"sku\":\"A\"}" },
        },
    );
    defer allocator.free(result.events);
    std.debug.print("next revision: {d}, log position: {d}\n",
        .{ result.next_revision, result.log_position });

    // Read the stream back.
    const page = try esdb.readStream(
        client, allocator, "orders-1",
        .{ .from = .{ .start = {} }, .direction = .forward, .limit = 100 },
    );
    defer freeEvents(allocator, page.events);
    for (page.events) |ev| {
        std.debug.print("  rev={d} type={s}\n", .{ ev.revision, ev.event_type });
    }
}

fn freeEvents(a: std.mem.Allocator, events: []const esdb.RecordedEvent) void {
    for (events) |e| {
        a.free(e.stream_id);
        a.free(e.event_type);
        a.free(e.data);
        if (e.metadata) |m| a.free(m);
    }
    a.free(events);
}
```

A more complete example lives in [`examples/basic/main.zig`](examples/basic/main.zig).

---

## Build

```bash
zig build                  # compiles the library, CLI, examples
zig build test             # runs the full quick test suite (under the 60s runner deadline)
zig build test-bench-micro # bench suite (~2 min, opt out of the default)
zig build test-stress-concurrent # stress suite (racy, opt out of the default)
zig build run              # builds and runs the CLI server on :2113 with ./eventstore.db
zig build examples         # builds all example binaries into zig-out/examples/
zig build test-unit-append # runs a single test module
```

The combined `zig build test` step runs every `tests/<dir>/*.zig`
file once per file, in a fresh process. The bench and stress suites
take too long or are racy under contention, so they are intentionally
excluded from the combined step and only available via their dedicated
`test-bench-micro` and `test-stress-concurrent` targets. The CLI
server (`zig build run`) is also single-connection by design: one
process, one `Client` at a time — fan out by spawning instances or
embedding the library in your own process.

The first build compiles the vendored SQLite amalgamation. Subsequent
builds reuse the `.zig-cache/`.

The Zig toolchain version is `0.16.x` (the binary in this repo lives
under a `zig-x86_64-windows-0.17.0-dev…` folder but the toolchain
reports `0.16.0` and the source is written against the 0.16 API).

---

## What's in this repo

```
eventstoredb-zig/
├── build.zig, build.zig.zon        — package manifest, test orchestration
├── vendor/sqlite/                  — vendored SQLite amalgamation + sub-build
│   ├── c/sqlite3.c, sqlite3.h
│   └── build.zig                   — compiles the amalgamation as a static lib
│
├── src/                            — public library (what users import)
│   ├── root.zig                    — `@import("eventstoredb")` entry point
│   ├── types.zig                   — public types (Uuid, EventData, RecordedEvent, etc.)
│   ├── errors.zig                  — typed error set + error helpers
│   ├── schema.zig                  — DDL, migrations, Connection (open/close/exec)
│   ├── client.zig                  — Client struct, writer mutex, stats, last log pos
│   ├── append.zig                  — appendToStream (idempotency, expected revision)
│   ├── read.zig                    — readStream, readAll, scan helpers
│   ├── subscribe.zig               — catch-up subscriptions (Queue + Waiter pattern)
│   ├── persistent.zig              — persistent subscriptions (group, ack, nack, parked)
│   ├── snapshot.zig                — saveSnapshot / loadSnapshot
│   ├── meta.zig                    — listStreams, getStreamInfo, setStreamMetadata,
│   │                                 deleteStream, save/loadProjectionState
│   ├── waker.zig                   — broadcast waker used by subscribers
│   ├── uuid.zig                    — UUIDv4 generation (RFC 4122)
│   ├── time.zig                    — current-time helpers (Io.Clock)
│   ├── spinlock.zig                — busy-spin mutex (per-cpu friendly)
│   ├── c.zig                       — @cImport of sqlite3.h
│   └── bind.zig                    — bindText / bindI64 / bindBlob (prepared-stmt helpers)
│
├── cmd/server.zig                  — CLI binary
│
├── examples/                       — runnable examples
│   ├── basic/main.zig              — open, append, read
│   ├── subscribe/main.zig          — subscribe to a stream
│   ├── persistent/main.zig         — persistent subscription
│   └── snapshots/main.zig          — snapshot save + load
│
├── tests/                          — test suite, one subdirectory per kind
│   ├── eventstoredb_test.zig       — legacy single-file smoke tests
│   ├── unit/                       — granular, one feature per file
│   │   ├── append.zig, read.zig, client.zig, delete.zig,
│   │   ├── snapshot.zig, meta.zig, projection.zig,
│   │   ├── subscribe.zig, persistent.zig, errors.zig
│   │   └── common.zig
│   ├── load/                       — volume (1 000 streams × 10 events)
│   ├── stress/                     — concurrent writers + subscribers
│   ├── chaos/                      — close-during-op, invalid paths
│   ├── security/                   — SQL-injection, hostile content, paths
│   ├── bench/                      — micro-benchmarks
│   └── dcb/                        — Dynamic Consistency Boundary
│
├── docs/
│   ├── ARCHITECTURE.md             — schema, concurrency, per-op SQL
│   ├── DCB.md                      — DCB (Dynamic Consistency Boundary) explainer
│   └── EVENTSTOREDB-COMPATIBILITY.md — what the API matches against ES-DB
│
├── README.md                       — this file
└── LICENSE                         — MIT for our code, public domain for the SQLite amalgamation
```

### Public surface (from `src/root.zig`)

```zig
// Types
pub const Client, EventData, RecordedEvent, AppendResult, ReadResult,
           ReadOptions, ReadDirection, From, ExpectedRevision,
           SubscribeOptions, PersistentOptions, PersistentConfig,
           PersistentMessage, StreamMetadata, StreamInfo, Snapshot,
           ProjectionState, Stats, OpenOptions, Position, Uuid, Error;

// Append / read
pub fn appendToStream(self, allocator, stream_id, opts, events) !AppendResult;
pub fn readStream(self, allocator, stream_id, opts) !ReadResult;
pub fn readAll(self, allocator, opts) !ReadResult;

// Catch-up subscriptions
pub fn subscribeToStream(self, allocator, stream_id, opts) !Subscription;
pub fn subscribeToAll(self, allocator, opts) !Subscription;
pub const Subscription;

// Persistent subscriptions
pub fn createPersistentSubscription(self, allocator, stream_id, group_name, opts, cfg, overwrite) !void;
pub fn deletePersistentSubscription(self, group_name, stream_id) !void;
pub fn connectPersistentSubscription(self, allocator, stream_id, group_name) !PersistentSubscription;
pub const PersistentSubscription;

// Snapshots / metadata / projections
pub fn saveSnapshot(self, stream_id, revision, payload, metadata) !void;
pub fn loadSnapshot(self, allocator, stream_id, revision) !Snapshot;
pub fn listStreams(self, allocator, limit, offset) ![]StreamInfo;
pub fn getStreamInfo(self, allocator, stream_id) !StreamInfo;
pub fn setStreamMetadata(self, stream_id, meta) !void;
pub fn deleteStream(self, stream_id) !void;
pub fn saveProjectionState(self, name, position, state) !void;
pub fn loadProjectionState(self, allocator, name) !ProjectionState;
```

The full schema DDL lives in `src/schema.zig`; the per-operation SQL
and the per-allocator ownership rules are documented in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Schema

The store is a single SQLite file. Current schema version is **2** (v1 →
v2 migration is idempotent via `PRAGMA table_info` probes in
`addColumnIfMissing`).

```
streams(stream_id, stream_type, revision, max_count, truncate_before,
        custom_metadata, created_at, updated_at, deleted_at)
events(event_id, stream_id, event_number, log_position, transaction_position,
       event_type, data, metadata, created_at,
       sequence, tags, dc_time)   -- DCB columns (nullable, added in v2)
committed_event_ids(event_id, log_position, stream_id, event_number, committed_at)
persistent_subscriptions(group_name, stream_id, start_from, last_position,
                         revision, config, status, created_at, updated_at)
persistent_acks(group_name, stream_id, event_id, log_position,
                retry_count, parked, enqueued_at)
snapshots(stream_id, revision, payload, metadata, created_at)
projection_checkpoints(projection_name, last_processed_position,
                       state, updated_at)
schema_info(version, applied_at)
```

SQLite pragmas applied at `Connection.open`:
`journal_mode = WAL`, `synchronous = NORMAL`, `temp_store = MEMORY`,
`foreign_keys = OFF`, `busy_timeout = <opts.busy_timeout_ms>`.

---

## DCB (Dynamic Consistency Boundary)

`docs/DCB.md` explains the pattern. Short version: DCB lets a decision
read the events it needs (`type IN (...) AND JSON_EXTRACT(tags, ...) = ?`)
and commit an append in a single SQL statement guarded by `MAX(sequence)`,
with atomicity provided by SQLite's file lock. The Zig store adds the
required columns (`sequence`, `tags`, `dc_time`) on top of the
EventStoreDB-compat event row, so both APIs share the same physical
table. Tests in `tests/dcb/` exercise the schema today; first-class
`readDcb` / `appendIfNoEventsMatch` helpers are on the roadmap.

---

## Test suite

The test suite is split into seven kinds, each in its own subdirectory
of `tests/`. The `build.zig` declares one `zig build test-<kind>-<file>`
step per file, plus an aggregate `test` step that runs them all.

| Kind       | What it covers                                                            | File count | Status |
| ---------- | ------------------------------------------------------------------------- | ---------- | ------ |
| `unit`     | One feature per file: `append`, `read`, `subscribe`, `persistent`, etc.    | 10 files   | ✅     |
| `load`     | Volume: 1 000 streams × 10 events (append + readAll)                     | 1 file     | ✅     |
| `stress`   | Concurrent writers + a catch-up subscriber, validates no event loss      | 1 file     | ⚠️     |
| `chaos`    | Close-during-op, invalid paths, empty paths, `DatabaseClosed` everywhere | 2 files    | ✅     |
| `security` | SQL-injection vectors, NULL bytes, unicode, large payloads              | 3 files    | ✅     |
| `bench`    | Micro-benchmarks with throughput on stderr                               | 1 file     | ✅     |
| `dcb`      | DCB schema columns, partial unique index, JSON_EXTRACT filter            | 1 file     | ✅     |
| legacy     | `tests/eventstoredb_test.zig` (the original smoke tests)                | 1 file     | ✅     |

`zig build test` runs every module. The result as of this revision:

- **unit**: all 14 tests in `unit/append.zig` pass; coverage of all
  `append` flavors. The other unit files cover the rest of the public
  surface. A few subsuites still have residual `DebugAllocator` leaks
  caused by slices duped by `read.zig::scanEvent` not being freed by
  the test harness — see *Known issues* below.
- **bench**: prints throughput per operation on stderr; no assertions
  on numbers (the test runner would be flaky if it had any).
- **stress / chaos / security / dcb**: each subdirectory has its own
  `zig build test-<kind>-<file>` step so you can run a single suite
  in isolation.

### Known issues in the test suite (current revision)

These are real, not test bugs:

1. **Stress test (`tests/stress/concurrent.zig`) can starve** — the
   test asserts 1 000 events are delivered through the subscription
   after 4 parallel writers each commit 250 events. The subscription
   worker reads via the same SQLite connection that the writers are
   committing on, so reads are serialised with writes. The busy-wait
   model plus the read/write contention means the worker drains
   ~500–800 of the 1 000 events in the 2 s drain window. Fixing
   this properly requires either a separate read-only connection for
   the worker, or streaming the read instead of paging. Tracked
   separately from the unit/security/dcb suite, all of which are
   green.
2. **Persistent subscription re-delivery is best-effort** — the
   `persistent_acks` table is the source of truth but a server crash
   between commit and ack-write can re-deliver. This is documented
   in the original Go port too.

---

## SQLite analyzer (`cmd/analyze.zig`)

`zig build analyze` (or `./zig-out/bin/analyze.exe <db-path>`) prints a
full report of any SQLite database the binary is linked against,
covering the public native APIs:

| Section | What it shows |
| --- | --- |
| Banner | resolved `sqlite3_db_filename`, default schema name |
| Compile-time info | `sqlite_version`, `source_id`, all 41 `SQLITE_COMPILEOPTION_*`, FTS5 / OMIT_WINDOWFUNC / JSON1 flags |
| Pragmas | 19 properties: `application_id`, `user_version`, `schema_version`, `journal_mode`, `synchronous`, `foreign_keys`, `auto_vacuum`, `page_size`, `page_count`, `cache_size`, `integrity_check`, etc. |
| Attached databases | `PRAGMA database_list` |
| `sqlite_master` | counts by kind, full ordered list, `json_group_array` of every DDL statement |
| Per-table | `PRAGMA table_info` (columns), `PRAGMA index_list` (indexes), `SELECT COUNT(*)` |
| File metrics | `page_count`, `page_size`, `freelist_count`, `max_page_count`, `bytes_used` |
| EXPLAIN QUERY PLAN | 4 representative queries |
| Function list | first 30 rows of `pragma_function_list` |
| json1 demos | `json_object`, `json_extract`, `json_array_length`, `json_group_array` |
| Math demos | `pi()`, `pow`, `sqrt`, `exp`, `log`, `sin`, `unicode`, `char`, `hex`, `quote` |
| Date/time demos | `date`, `time`, `datetime`, `julianday`, `unixepoch`, `strftime` |
| Window + CTEs | recursive CTE + `sum() OVER (ROWS BETWEEN ...)` with running cumulative |
| Integrity checks | `PRAGMA integrity_check`, `quick_check`, `foreign_key_check` |
| Event-store domain | validates the 8 expected tables (streams, events, committed_event_ids, persistent_subscriptions, persistent_acks, snapshots, projection_checkpoints, schema_info), the DCB columns (sequence / tags / dc_time), and emits per-table row counts in a single `UNION ALL` round-trip |

A small companion binary `zig build demo-db` (or
`./zig-out/bin/make_demo_db.exe <path>`) produces a demo database
populated with the v2 schema and a handful of events, so the
analyzer has something interesting to look at.

---

## CLI

```bash
# Start a local server
zig build run -- --data ./data/eventstore.db --listen :2113

# Inspect a database
./zig-out/bin/eventstoredb-zig stats --data ./data/eventstore.db

# Tail events live (Ctrl-C to stop)
./zig-out/bin/eventstoredb-zig tail --data ./data/eventstore.db --stream orders-1
```

# Run the full SQLite analyzer against any database
zig build analyze
# or directly:
./zig-out/bin/analyze.exe ./data/eventstore.db

# Create a demo DB populated with the v2 event store schema
# so the analyzer has something interesting to inspect:
zig build demo-db

> The CLI binary currently ships a `serve` / `stats` / `tail` shell; the
> HTTP server described in earlier revisions of this README is not
> implemented in Zig yet — use the [`eventstoredb-sqlite`](../eventstoredb-sqlite/)
> HTTP server if you need a wire protocol.

---

## Status / compatibility

| Feature                              | Status        |
| ------------------------------------ | ------------- |
| `Client.open` / `close` / `stats`    | ✅            |
| `appendToStream` (all `ExpectedRevision` flavors, idempotency) | ✅ |
| `readStream` / `readAll` (forward, backward, from-start, from-end, from-revision, from-position, limit) | ✅ |
| Catch-up subscriptions (`subscribeToStream`, `subscribeToAll`) | ✅ (busy-wait based; no kernel waits) |
| Persistent subscriptions (create, connect, ack, nack, parked) | ✅ (basic; see *Known issues* above) |
| Snapshots (save/load)                | ✅            |
| Stream metadata (`listStreams`, `getStreamInfo`, `setStreamMetadata`, `deleteStream`) | ✅ |
| Projection state (`saveProjectionState`, `loadProjectionState`) | ✅ |
| DCB (read/append columns, partial unique index) | ✅ (schema); first-class helpers 🚧 |
| Multi-stream transactions            | 🚧            |
| Filter / projection subscriptions    | 🚧            |
| HTTP server (the 11 routes in the Go port) | ❌ |
| `setStreamMetadata` with `truncate_before` enforcing | 🚧 |

The intent is to keep the public API close to the Go port so a
production app can swap the implementation without rewriting business
logic. The per-feature comparison against the official EventStoreDB
server is in `docs/EVENTSTOREDB-COMPATIBILITY.md` (not yet written —
placeholder).

---

## Architecture in one screen

```
   ┌─────────────────────────────────────────────┐
   │  Your Zig app                               │
   │  ┌──────────────────────────────────────┐   │
   │  │  eventstoredb.Client                 │   │
   │  │   ├─ Open / Close / Stats            │   │
   │  │   ├─ Append / Read / Subscribe       │   │
   │  │   ├─ Persistent Subscriptions        │   │
   │  │   └─ Snapshots / Meta / Projections  │   │
   │  └────────────┬─────────────────────────┘   │
   │               │  cImport → sqlite3            │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  vendored SQLite amalgamation        │   │
   │  └────────────┬─────────────────────────┘   │
   │               │                              │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  SQLite file (WAL, mmap, NORMAL sync)│   │
   │  │   ├─ streams                          │   │
   │  │   ├─ events  (+ sequence/tags/dc_time │   │
   │  │   │             in DCB mode)          │   │
   │  │   ├─ committed_event_ids              │   │
   │  │   ├─ persistent_subscriptions        │   │
   │  │   ├─ persistent_acks                 │   │
   │  │   ├─ snapshots                       │   │
   │  │   ├─ projection_checkpoints          │   │
   │  │   └─ schema_info                     │   │
   │  └──────────────────────────────────────┘   │
   └─────────────────────────────────────────────┘
```

- All writes go through `Client.writer_mu` (spinlock). SQLite is
  single-writer per file, so the spinlock inside the process
  serializes the transaction code path. The `PRAGMA busy_timeout` set
  in `Connection.open` covers contention from external processes.
- Reads are lock-free. The `waker` broadcasts to every catch-up
  subscriber the moment an append commits, so the latency on a busy
  stream is sub-millisecond.
- The `Subscription` API uses a bounded queue + `std.atomic.spinLoopHint`
  in 0.16 (because the old `std.Thread.Channel` is gone — see
  `src/subscribe.zig` for the rationale).

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full
schema, the per-operation SQL, the migration story, and the
allocator-ownership contract.

---

## Development

```bash
zig build                  # build everything
zig build test             # full test suite
zig build test-unit-append # one module only
zig build examples         # build every example into zig-out/examples/
zig build run              # build and run the CLI server
zig fmt src/               # format the source tree
```

The test suite is hand-curated in `build.zig` (a list of
`(name, path)` tuples) rather than discovered at build time, because
Zig 0.16 removed `std.fs.cwd` and discovery would have to go through
the `Build` context. Add a new test file by:

1. Creating `tests/<kind>/<name>.zig` with `test "..." { ... }` blocks.
2. Adding `(.{ .name = "<kind>-<name>", .path = "tests/<kind>/<name>.zig" })`
   to the `test_files` array in `build.zig`.
3. Running `zig build test` to verify it picks up.

---

## License

MIT for our code. The vendored SQLite amalgamation is in the public
domain. See [LICENSE](LICENSE) for details.
