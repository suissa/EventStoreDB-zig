# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`eventstoredb-zig` is an EventStoreDB-compatible event store built on top of a single vendored SQLite file, written in Zig 0.16.x. It's a Zig port of a Go sibling project (`eventstoredb-sqlite`) — streams, events, expected-revision checks, catch-up and persistent subscriptions, snapshots, and stream metadata all live in one `.db` file with no separate server process. There is no HTTP/gRPC server yet; the CLI only exposes `stats`/`tail`/`serve` (a single-connection shell).

## Build, test, run

```bash
zig build                         # build the library, CLI, examples
zig build test                    # run the full quick test suite (aggregate step)
zig build test-unit-append        # run one named test module (see below for the full list)
zig build test-stress-concurrent  # racy contention suite — excluded from `test`, run explicitly
zig build test-bench-micro        # micro-benchmarks (~2 min) — excluded from `test`, run explicitly
zig build examples                # build example binaries into zig-out/examples/
zig build run                     # build and run the CLI server (:2113, ./eventstore.db)
zig build analyze                 # run the SQLite analyzer against a db (./zig-out/bin/analyze.exe <path>)
zig build demo-db                 # populate a demo database for the analyzer to inspect
zig fmt src/                      # format changed .zig files — do this before finishing any edit
```

Requires Zig `0.16.x`. Test files are hand-registered as `(name, path)` tuples in the `test_files` array in [build.zig](build.zig) (not auto-discovered, because 0.16 removed `std.fs.cwd`-based discovery) — add a new suite by creating `tests/<kind>/<name>.zig` with `test "..." {}` blocks and appending an entry there; `zig build test-<kind>-<name>` picks it up.

Full list of individually-runnable test steps (`zig build <step>`): `test-unit-append`, `test-unit-read`, `test-unit-client`, `test-unit-delete`, `test-unit-snapshot`, `test-unit-meta`, `test-unit-projection`, `test-unit-subscribe`, `test-unit-persistent`, `test-unit-errors`, `test-load-volume`, `test-stress-concurrent`, `test-chaos-close`, `test-chaos-files`, `test-security-injection`, `test-security-unicode`, `test-security-path`, `test-bench-micro`, `test-dcb-read-append`.

The combined `zig build test` step runs every `tests/<dir>/*.zig` file once per file in a fresh process, but deliberately excludes the bench and stress suites (too slow / racy) — run those two individually when touching performance or concurrency code. Cross-compilation sanity check: `zig build -Dtarget=x86_64-linux-gnu`.

## Architecture

**Single-writer-per-process model.** `Client.writer_mu` (a spinlock in [src/spinlock.zig](src/spinlock.zig)) serializes every `appendToStream` call within the process before it opens a SQLite transaction, so same-process races never hit `SQLITE_BUSY`. Cross-process contention is covered by `PRAGMA busy_timeout`. Reads are lock-free (`journal_mode = WAL`). This is why the CLI server is single-connection by design — fan out by spawning separate processes or embedding the library directly.

**Module layout in `src/`** (each is one concern, all wired together through `Client`):
- `root.zig` — the `@import("eventstoredb")` public entry point; re-exports types and top-level functions.
- `schema.zig` — DDL, migrations (`addColumnIfMissing` probes `PRAGMA table_info` for idempotent v1→v2 upgrades), `Connection` open/close/exec.
- `client.zig` — `Client` struct, the writer spinlock, stats, last log position.
- `append.zig` / `read.zig` — the two core operations: idempotency via `committed_event_ids`, `ExpectedRevision` checks, forward/backward/from-revision/from-position reads.
- `subscribe.zig` — catch-up subscriptions: bounded queue + `std.atomic.spinLoopHint` (Zig 0.16 removed `std.Thread.Channel`, see inline rationale). Delivery loop: drain a page → push to queue → advance cursor → loop immediately if the page was full, else wait on the broadcast `waker.zig` or poll.
- `persistent.zig` — consumer-group subscriptions backed by `persistent_subscriptions` (config + cursor) and `persistent_acks` (in-flight/parked queue); checkpoint advances on `Ack`.
- `snapshot.zig`, `meta.zig` — snapshot save/load; `listStreams`/`getStreamInfo`/`setStreamMetadata`/`deleteStream` (soft-delete via `deleted_at`, tombstoned streams reject further appends/reads); projection checkpoint save/load.
- `waker.zig` — broadcasts to every catch-up subscriber the moment an append commits.
- `bind.zig` — the only sanctioned way to bind values into prepared statements (`bindText`/`bindI64`/`bindBlob`); never interpolate user-controlled values into SQL.
- `uuid.zig`, `time.zig`, `errors.zig`, `types.zig`, `c.zig` — UUIDv4, clock helpers, typed error set, public types, the `@cImport` of `sqlite3.h`.

**Schema** (current version 2, DDL is canonical in `src/schema.zig`): `streams`, `events` (PK `(stream_id, event_number)`; `log_position` is a dense global order; DCB columns `sequence`/`tags`/`dc_time` are nullable v2 additions), `committed_event_ids` (idempotency), `persistent_subscriptions` + `persistent_acks`, `snapshots`, `projection_checkpoints`, `schema_info`.

**DCB (Dynamic Consistency Boundary)**: the `events` table doubles as the DCB event log — the same physical row serves both the EventStoreDB-compat API and DCB-style reads/appends guarded by `MAX(sequence)`. See [docs/DCB.md](docs/DCB.md). Schema support exists and is exercised by `tests/dcb/`; first-class `readDcb`/`appendIfNoEventsMatch` helpers are not yet implemented.

**Vendored SQLite**: the amalgamation lives at `vendor/sqlite/c/sqlite3.c` (~9 MB) with its own `build.zig` sub-build compiling it as a static lib. Do not regenerate or hand-edit it casually.

Full per-operation SQL and the allocator-ownership contract (who frees what) are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); read it before touching `append.zig`/`read.zig`/`subscribe.zig` memory handling.

## Known, load-bearing issues (not bugs to silently "fix")

- `tests/stress/concurrent.zig` can legitimately starve: the subscription worker reads over the same SQLite connection the writers commit on, so under contention it may drain only 500–800 of 1000 expected events in the drain window. This is a documented limitation, not flaky-test noise — a real fix needs a separate read connection or streaming reads.
- Persistent subscription redelivery is best-effort: a crash between commit and ack-write can redeliver a message. `persistent_acks` is the source of truth.
- A few unit subsuites have residual `DebugAllocator` leaks from `read.zig::scanEvent`-duped slices not freed by the test harness (not a production leak).

## Conventions

- `zig fmt` on every changed `.zig` file; four-space indent, `snake_case` functions/files/locals, `PascalCase` types.
- All SQL parameterized through `src/bind.zig` — never interpolate user-controlled values (database paths and event payloads are untrusted input; see `tests/security/`).
- Explicit allocator ownership and typed error sets throughout — match the existing free/dupe patterns rather than introducing new allocation strategies.
- Don't commit generated databases, WAL/SHM files, or debug artifacts (the working tree currently has some stray `demo.db`/`ok*`/`.tmp` files checked in — don't add to that, and don't casually `git add -A`).
- No detailed commit-message convention; use short imperative subjects. PRs should call out behavior/compatibility impact, tests run, and any schema/migration changes.
