# Architecture

This document describes the internal design of `eventstoredb-zig`:
how events are stored, how concurrency is handled, and how the
subscription engine works.

> **Status:** v0.1. Work in progress; see [EVENTSTOREDB-COMPATIBILITY.md](./EVENTSTOREDB-COMPATIBILITY.md) for the API surface.

---

## High-level shape

```
   ┌──────────────────────────────────────────────┐
   │  Your Zig app                               │
   │  ┌──────────────────────────────────────┐   │
   │  │  eventstoredb.Client                 │   │
   │  │   ├─ Open / Close / Stats            │   │
   │  │   ├─ Append / Read / Subscribe       │   │
   │  │   ├─ Persistent Subscriptions        │   │
   │  │   └─ Snapshots / Meta                │   │
   │  └────────────┬─────────────────────────┘   │
   │               │  database/sql (via cImport)  │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  vendored SQLite amalgamation        │   │
   │  │   compiled as a static lib,           │   │
   │  │   linked into the library/CLI.        │   │
   │  └────────────┬─────────────────────────┘   │
   │               │                              │
   │  ┌────────────▼─────────────────────────┐   │
   │  │  SQLite file (single .db + .db-wal)   │   │
   │  │   ├─ streams                          │   │
   │  │   ├─ events (heart)                  │   │
   │  │   ├─ committed_event_ids (idempotency)│   │
   │  │   ├─ persistent_subscriptions        │   │
   │  │   ├─ persistent_acks                 │   │
   │  │   ├─ snapshots                       │   │
   │  │   └─ schema_info                     │   │
   │  └──────────────────────────────────────┘   │
   └──────────────────────────────────────────────┘
```

The library is a single Go module. There is no daemon process.
The in-process `Client` is the daemon. The CLI exists to expose
`stats` and a `tail` for live event streaming.

---

## Storage model

The schema is identical to the Go port (`eventstoredb-sqlite`).
See the inline comments in [`src/schema.zig`](../src/schema.zig)
for the canonical DDL.

Salient tables:

### `events`

```
event_id              BLOB NOT NULL              -- 16 bytes UUID
stream_id             TEXT NOT NULL
event_number          INTEGER NOT NULL           -- 0-based per-stream
log_position          INTEGER NOT NULL UNIQUE   -- global commit order
transaction_position  INTEGER NOT NULL
event_type            TEXT NOT NULL
data                  BLOB NOT NULL
metadata              BLOB
created_at            INTEGER NOT NULL           -- unix epoch ms
PRIMARY KEY (stream_id, event_number)
```

- `PRIMARY KEY (stream_id, event_number)` matches the B-tree layout
  used by EventStoreDB for the per-stream log. Range scans by
  revision are O(log n).
- `log_position` is 1-based; the first event appended to an empty
  store gets position 1. This matches `MAX(log_position) + 1` on
  the next insert, which gives a dense, gap-free global ordering.
- `transaction_position` groups events written in the same Append
  call. Multi-stream transactions (planned for v0.2) will share
  the same `transaction_position`.
- `event_type` is indexed for projection lookups.

### `committed_event_ids`

A separate table for idempotency. The PK lookup is O(1) so a
retrying append that reuses an `event_id` pays a single index
probe instead of scanning `events`.

### `streams`

Per-stream metadata. The `deleted_at` column implements
tombstoning: appends and reads fail with `StreamTombstoned`
once a stream is soft-deleted, but the events remain on disk
so a restorer can rebuild state if needed.

### `persistent_subscriptions` + `persistent_acks`

Two tables per consumer group:

- `persistent_subscriptions` carries the configuration, the
  per-group cursor, and a `status` enum (`Live`, `Paused`).
- `persistent_acks` is the in-flight queue. A row exists for
  each event that has been delivered to a consumer and is
  waiting on `Ack`/`Nack`. Parked messages have `parked = 1`.

### `snapshots` and `projection_checkpoints`

- `snapshots` is the per-stream aggregate snapshot store,
  keyed by `(stream_id, revision)`. Multiple revisions are
  retained; callers garbage-collect in a background job.
- `projection_checkpoints` makes user-defined projections
  restartable: a `last_processed_position` row per projection
  name.

---

## Concurrency model

SQLite is single-writer per file. The library embraces this:

1. **Process-level spinlock** (`Client.writer_mu`): every
   `appendToStream` call acquires it before opening its
   transaction. This serializes appends within the process, so
   we never get `SQLITE_BUSY` from a same-process race.
2. **PRAGMA journal_mode = WAL**: readers never block on
   writers and vice versa.
3. **PRAGMA synchronous = NORMAL**: trades a small durability
   window (last transaction may roll back on a power loss) for
   ~10x write throughput. Acceptable for an append-only store.
4. **PRAGMA busy_timeout = 5000ms**: covers the cross-process
   `SQLITE_BUSY` case.

### Subscription delivery

`SubscribeToStream` and `SubscribeToAll` are catch-up
subscriptions:

1. Read a page of events from the cursor position.
2. Push them onto the buffered channel.
3. Update the cursor.
4. If we drained a full page, immediately loop (more may be
   available). Otherwise wait for the broadcast waker
   (signalled at the end of every `AppendToStream`) or the
   poll interval.

This gives sub-poll-interval latency on a busy stream and
falls back to polling on an idle one.

### Persistent subscription delivery

`ConnectPersistentSubscription` adds:

- A per-group checkpoint (`last_position` on
  `persistent_subscriptions`).
- An in-flight table (`persistent_acks`).

The engine advances the checkpoint on `Ack`. On restart, it
resumes from the last checkpoint and re-delivers any parked
or unacked messages.

---

## Compatibility with EventStoreDB

The Zig API is intentionally close to the Go port. The CLI
covers the basic operational commands (`stats`, `tail`).
A full HTTP+JSON server is on the roadmap for v0.2.

See [EVENTSTOREDB-COMPATIBILITY.md](./EVENTSTOREDB-COMPATIBILITY.md)
for the current API surface.

---

## Performance budget

Single-process, local SSD, default pragmas, vendored SQLite
amalgamation:

- **Append**: 20–50k events/sec for small payloads.
- **Read by stream**: 100k events/sec.
- **Subscribe latency**: < 10 ms p99 under sustained load.

These are not stress-tested. Run your own benchmark before
committing to a workload.

---

## Failure modes

| Failure | What happens |
| --- | --- |
| Process crash mid-append | WAL rolls back the partial transaction; the next open sees a consistent state. |
| Disk full | `appendToStream` returns the SQLite I/O error. The Client stays open. |
| Concurrent process opening same `.db` | SQLite acquires the write lock. Readers see WAL-mode data; writers wait. |
| Subscriber buffer full | The send blocks. The producer's append is unaffected. |

---

## Why Zig

- **Single static binary** — no runtime, no glibc mismatch, no
  cgo-equivalent pain.
- **No external SQLite dependency** — the amalgamation is
  vendored and statically linked.
- **Manual memory management** makes subscription lifetimes
  obvious and zero-cost.
- **Build cross-compiles** to Windows / Linux / macOS from any
  host (`zig build -Dtarget=...`).

---

## Where to extend

- **gRPC server**: implement the
  `event_store.client.streams.Streams` and
  `event_store.client.persistent_subscriptions.PersistentSubscriptions`
  services from the official proto. Reuse `Client` as the
  storage layer.
- **Server-side projections**: a JS engine (V8 / goja /
  bun.sh) running user-defined projection scripts that
  consume `$all` and write to side tables. The
  `projection_checkpoints` table already supports this.
- **Multi-stream transactions**: extend `appendToStream` to
  take a list of `(streamID, expectedRevision, events)` tuples
  and commit them in a single transaction.
- **Encryption**: switch to SQLCipher by adding the
  appropriate build flag and PRAGMA key. The schema and API
  do not change.
