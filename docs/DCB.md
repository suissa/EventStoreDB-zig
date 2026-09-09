# DCB — Dynamic Consistency Boundary

This document explains what DCB is, how it differs from the stream-based
event-store model we already ship, and exactly how the Zig event store
extends the four-column reference schema proposed by Maxime Gosselin in
[*Implementing a DCB-Compliant Event Store in SQLite*][gosselin] (Feb 2026).

The authoritative spec is at <https://dcb.events/specification/>; this file
is implementation-facing and assumes you have either read the spec, the
article, or both.

[gosselin]: https://maximegosselin.com/posts/implementing-a-dcb-compliant-event-store-in-sqlite/

## 1. What problem DCB solves

A traditional event store groups events into **streams** (`order-123`,
`invoice-42`) and enforces per-stream ordering with an *expected revision*
check on append. That model is great when one aggregate is one stream, but
it forces every business invariant to live inside a single stream. The moment
a rule spans multiple aggregates — "this cart can only be checked out if no
fraud block exists for the buyer *and* the inventory reservation has not
expired" — the stream model has nothing to say. You either re-aggregate
everything into one stream (and lose parallelism) or you sprinkle compensating
logic across services.

DCB (Dynamic Consistency Boundary) flips the unit of consistency from
"stream" to **"the set of events a decision depends on right now."** That set
is expressed as a query (`failIfEventsMatch: Query`); the append succeeds
only if no event matching the query has been written since the caller last
read. The boundary is dynamic because the caller picks it per use case.

Concretely, the operational rules are:

- **Decisions are reads.** A handler reads the events it needs (filtered by
  type + tags), receives the position of the *last* event in that slice, and
  decides based on that slice.
- **Appends are guarded.** When the handler appends, it passes the same
  query plus `after: <lastPosition>`. The store commits only if no matching
  event has been appended after that position since the read.
- **No canonical event names.** Because the boundary is *the query*, not a
  stream id, you do not negotiate a global event-naming schema. Every
  append is "I have not seen anything matching this query since position N."

That last point is what makes DCB attractive in polyglot systems: the
producer and the consumer do not need to agree on a name; they agree on a
filter.

## 2. The reference schema

The article proposes the minimum schema for a DCB event store:

| column     | required | type      | role                                              |
| ---------- | -------- | --------- | ------------------------------------------------- |
| `sequence` | yes      | INTEGER   | global, gap-free, append-only PK                  |
| `type`     | yes      | TEXT      | event type (e.g. `InvoiceIssued`)                 |
| `tags`     | yes      | TEXT      | JSON array of `{key, value}` pairs                |
| `data`     | yes      | TEXT/BLOB | event payload                                     |
| `uid`      | no       | TEXT      | stable external id (idempotency on the producer)  |
| `metadata` | no       | TEXT      | correlation/causation ids, free-form              |
| `time`     | no       | INTEGER   | wall-clock timestamp (ms since epoch)             |

A read is a SQL `WHERE type IN (...) AND JSON_EXTRACT(tags, '$.<key>') = ?`
plus `sequence >= ?` so the caller can resume from a position. The position
returned to the caller is the **max sequence of the matching rows** — that
is the value they will pass back as `after` on the next append.

## 3. The reference append algorithm

The whole point of DCB is collapsed into one `INSERT … SELECT` with a guard
in the `WHERE` clause:

```sql
INSERT INTO events (type, tags, data, time, sequence)
SELECT ?, ?, ?, ?, COALESCE(MAX(sequence), 0) + 1
FROM events
WHERE ? = (SELECT MAX(sequence) FROM events WHERE <query>);

-- where the placeholder ? is the "after" position the caller saw on read.
```

The `SELECT MAX(sequence) FROM events WHERE <query>` is the freshness check.
The outer `? =` makes the row count zero (and therefore the `INSERT`
no-ops) the moment any matching event has been written after the caller's
position. The atomicity is provided by the **database's own write lock** —
in SQLite that is the file lock; in Postgres it is `SERIALIZABLE` (or
`SELECT … FOR UPDATE` in `READ COMMITTED`); in MySQL you need
`SERIALIZABLE` or row locks via `SELECT … FOR UPDATE` plus a retry loop.

The article is explicit: this pattern is **not portable to MySQL or
Postgres without help**. SQLite's global write lock is what makes the
single-statement check-and-insert linearizable. Anywhere else, you have to
add explicit locking or run the whole transaction at `SERIALIZABLE`.

## 4. How our store extends the reference schema

Our store already ships a stream/aggregate model (EventStoreDB
compatibility: `event_id`, `stream_id`, `event_number`, `log_position`,
`transaction_position`, `event_type`, `data`, `metadata`, `created_at`).
Rather than maintain two stores, we extend the existing `events` table
with the DCB columns so both APIs share the same physical rows.

Schema v2 (see `src/schema.zig`):

```sql
CREATE TABLE IF NOT EXISTS events (
  -- EventStoreDB-compat columns
  event_id              BLOB NOT NULL,
  stream_id             TEXT NOT NULL,
  event_number          INTEGER NOT NULL,
  log_position          INTEGER NOT NULL UNIQUE,
  transaction_position  INTEGER NOT NULL,
  event_type            TEXT NOT NULL,
  data                  BLOB NOT NULL,
  metadata              BLOB,
  created_at            INTEGER NOT NULL,
  -- DCB columns (added in v2; nullable for legacy rows)
  sequence              INTEGER,        -- global, gap-free, append-only
  tags                  TEXT,           -- JSON array of {key,value}
  dc_time               INTEGER,        -- unix-ms for DCB reads
  PRIMARY KEY (stream_id, event_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_sequence
  ON events(sequence) WHERE sequence IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_tags ON events(tags);
```

Mapping the article's columns onto ours:

| DCB reference | Our column        | Why                                                                 |
| ------------- | ----------------- | ------------------------------------------------------------------- |
| `sequence`    | `sequence`        | 1:1. Partial unique index keeps legacy NULL rows out of conflicts.  |
| `type`        | `event_type`      | We already had it; reused verbatim.                                 |
| `tags`        | `tags`            | Stored as TEXT, JSON array, filterable with `JSON_EXTRACT`.        |
| `data`        | `data`            | Same column, we keep it as BLOB so the EventStoreDB API is happy.   |
| `uid`         | `event_id`        | EventStoreDB already requires a 16-byte unique id; we reuse it.     |
| `metadata`    | `metadata`        | We store BLOB for ES-DB compat; a JSON variant can live in the BLOB.|
| `time`        | `dc_time`         | Distinct from `created_at` so DCB callers can opt out of clock.    |

### Why nullable, and not `NOT NULL`

The v1 schema shipped before DCB existed. Backfilling `sequence` for every
existing row would require a deterministic global ordering, which we have
(`log_position`), but it is a one-shot rewrite that is cheaper to defer
until there is an actual reason to do it. The partial unique index
(`WHERE sequence IS NOT NULL`) lets new DCB writes enforce uniqueness while
leaving legacy rows out of the way. New writes that go through the DCB
API will always populate `sequence`, `tags`, and `dc_time`.

### What we add beyond the reference

- **NoSQL-style `tags`**: instead of forcing callers to declare a fixed
  schema, `tags` is a free-form JSON array. Producers and consumers agree
  on the keys they care about; the rest is ignored. This is the article's
  design.
- **A wall-clock `dc_time` distinct from `created_at`**: `created_at` is
  the EventStoreDB-style server-side commit timestamp; `dc_time` is the
  business timestamp the producer wants the read to filter on. They can
  diverge and that is fine.
- **Reuse of `event_id` as the DCB `uid`**: the spec treats `uid` as
  optional; we already have a 16-byte UUID per event, so we use it for
  idempotency (`INSERT … WHERE NOT EXISTS (SELECT 1 FROM events WHERE
  event_id = ?)` is a no-op on retry).
- **Shared physical table**: the stream API and the DCB API both read and
  write the same rows. A stream-append can later be re-read as a DCB
  slice, and vice versa, as long as the writer populated the DCB columns.

## 5. The DCB read query in our store

A DCB read against our schema is the article's read, with the column
names swapped:

```sql
SELECT sequence, event_type, tags, data, metadata, dc_time
FROM events
WHERE sequence >= ?
  AND event_type IN (?, ?, ?)
  AND JSON_EXTRACT(tags, '$.<key>') = ?
  AND sequence IS NOT NULL
ORDER BY sequence ASC
LIMIT ?;
```

The `sequence IS NOT NULL` clause is the only thing the article does not
need: it is our guard against legacy rows. Once we backfill, the clause
goes away.

The handler receives the max `sequence` of the returned rows. That value
is the `after` it will pass back on the next append.

## 6. The DCB append in our store

The article's guarded `INSERT … SELECT` becomes:

```sql
INSERT INTO events
  (event_id, stream_id, event_number, log_position, transaction_position,
   event_type, data, metadata, created_at, sequence, tags, dc_time)
SELECT
  ?, ?, ?, ?, ?,
  ?, ?, ?, ?,
  COALESCE(MAX(sequence), 0) + 1, ?, ?
FROM events
WHERE ? = (SELECT MAX(sequence) FROM events
           WHERE sequence IS NOT NULL
             AND event_type IN (?, ?, ?)
             AND JSON_EXTRACT(tags, '$.<key>') = ?);
```

Notes:

- We still need to fill the EventStoreDB-compat columns even on a
  DCB-only write, because the same row may be re-read by a stream
  consumer. For DCB-only callers we accept a sentinel `stream_id` (e.g.
  `__dcb__`).
- The check subquery uses `MAX(sequence)` over the same filter the
  caller is appending against. That is the freshness check.
- The atomicity comes from SQLite's file lock (`PRAGMA journal_mode =
  WAL` + `synchronous = NORMAL`, configured in `Connection.open`).
- If the `INSERT` affects zero rows, the caller has lost the race and
  must re-read, re-decide, and retry.

## 7. Operational caveats

- **Single-writer assumption.** Like the article, this implementation
  assumes one process owns the SQLite file (or that all writers use
  `PRAGMA journal_mode = WAL` and rely on SQLite's locking). It will not
  be correct on a shared network filesystem, on Litestream with multi-writer
  failover, or on any backend that is not SQLite.
- **Tags indexing.** `JSON_EXTRACT` is not auto-indexed. The article
  acknowledges this; in practice you either accept a full scan of the
  filtered-by-type slice (fast if `idx_events_type` narrows enough) or
  you maintain a side table `event_tags(event_id, key, value)` with a
  composite index. We have not done the latter yet; flag it as a TODO if
  the read latency shows up in a profile.
- **Backfill of `sequence` for legacy rows.** Right now legacy rows are
  invisible to DCB reads because of the `IS NOT NULL` clause. A one-shot
  backfill is:

  ```sql
  WITH ordered AS (
    SELECT event_id, ROW_NUMBER() OVER (ORDER BY log_position) AS rn
    FROM events WHERE sequence IS NULL
  )
  UPDATE events SET sequence = (SELECT rn FROM ordered
                                WHERE ordered.event_id = events.event_id)
  WHERE sequence IS NULL;
  ```

  Do not run this on a database that has live writers unless you take a
  `BEGIN IMMEDIATE` lock first.

- **No live read-modify-write API yet.** The schema and migration are
  in. The Zig-side `appendIfNoEventsMatch` / `read` helpers that use
  this schema are the next step; until they land, the article's
  `INSERT … SELECT` pattern can be run as raw SQL through
  `Connection.exec` to validate the design.

## 8. Where to read more

- The article: <https://maximegosselin.com/posts/implementing-a-dcb-compliant-event-store-in-sqlite/>
- The DCB spec: <https://dcb.events/specification/>
- Our schema: `src/schema.zig` (`schemaVersion = 2`)
- Our existing event-store API: `src/append.zig`, `src/read.zig`,
  `src/subscribe.zig` (still on the stream model)
