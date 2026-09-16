# EventsZite v0.1 — technical review

Date: 2026-09-15

## Executive summary

EventsZite already has a useful embedded event-store core: SQLite WAL durability, expected-revision appends, stream/global reads, catch-up subscriptions, snapshots, metadata, projection checkpoints, a DCB-oriented schema extension, and a broad test layout.

The review found most risk in worker lifetime, subscription delivery semantics, configuration contracts, and documentation accuracy rather than in the append/read core.

## Corrected in this review

### 1. Client shutdown lifetime

`Client.close()` previously had a bounded wait and could destroy SQLite/client memory while workers still existed. Shutdown is now deterministic: publish `closed`, wake waiters, wait for `active_workers == 0`, then release SQLite and the client.

### 2. Catch-up worker accounting

Worker cleanup is installed before waiter registration so allocator failure cannot leave `active_workers` permanently incremented.

### 3. Persistent worker stack lifetime

`connectPersistentSubscription()` previously spawned a worker with a pointer to a local `PersistentSubscription` value. Workers now receive a heap-owned `PSRunContext` with independent stream/group storage.

### 4. Queue ownership and loss

Catch-up queues are allocator-owned and sized from `SubscribeOptions.buffer_size` (`0` selects the documented default). When full, producers apply backpressure rather than silently discarding events. Persistent queues also stop dropping event payloads silently and drain owned messages on close.

### 5. Scheduler-friendly timed waits

Time-based polling no longer treats `std.atomic.spinLoopHint()` as elapsed milliseconds. `Waker.wait`, persistent polling, queue backpressure, receive loops, and client shutdown use Zig 0.16 `std.Io.sleep` for scheduler-friendly wall-clock waits. Atomic spin remains only around short lock acquisition.

### 6. Connection configuration contract

`OpenOptions.max_connections` was removed because v0.1 does not implement a connection pool. The actual model is one writer handle and optionally one dedicated subscription-read handle.

Plain SQLite `:memory:` plus `separate_read_connection = true` is rejected with `UnsupportedConfiguration`, because two normal `:memory:` handles are two independent databases. File-backed stores may use the second read handle under WAL.

### 7. Persistent checkpoint semantics

Schema version 3 adds `persistent_acks.event_number` and durable `acked` state. The event number is the stream coordinate; global `log_position` is retained separately.

`PersistentSubscription.ack()` now executes ACK mutation, contiguous-frontier calculation, checkpoint update, and compaction inside one SQLite transaction under writer serialization.

The durable `last_position` is defined as the **next stream revision that is not durably complete**. Therefore:

- ACK 2 while 0 and 1 are incomplete leaves checkpoint 0;
- ACK 0 moves checkpoint to 1;
- ACK 1 then closes the gap and moves checkpoint to 3;
- NACK/park never advances the checkpoint;
- ACKed rows beyond a gap remain durable and are skipped during replay rather than reset to incomplete.

A regression test executes this out-of-order sequence against a connected persistent worker.

### 8. Persistent worker shutdown

Persistent workers observe `Client.closed`, use real timed sleeps, and own their run context independently. `Client.close()` can therefore wait for them without destroying memory they still reference.

## Documentation corrections

The product/repository name is **EventsZite**. The current Zig import/package compatibility surface still uses `eventstoredb` / `eventstoredb_zig`; this is a compatibility name, not the product identity.

The project implements an **EventStoreDB-inspired programming model over SQLite**. It is not wire-compatible with EventStoreDB and should not be described as a network drop-in replacement. `docs/EVENTSTOREDB-COMPATIBILITY.md` is the authority for that boundary.

## Current concurrency model

For v0.1:

- one canonical writer SQLite connection per `Client`;
- optional second read connection for subscription workers on file-backed stores;
- no generic connection pool;
- writer-side mutations are serialized by the client writer lock;
- subscription queues are bounded and backpressured;
- timed waits yield through `std.Io.sleep` rather than synthetic spin timing.

## Remaining roadmap items

The following are follow-up improvements, not hidden correctness gaps in the paths corrected above:

1. Consider a shared-memory SQLite URI mode if multi-connection in-memory operation becomes a real requirement.
2. Consider moving short atomic mutexes to `std.Io` synchronization if profiling shows contention long enough to justify blocking primitives.
3. Add crash/reopen tests for every intermediate persistent ACK frontier state using a file-backed store.
4. Define an explicit API for replaying/resolving parked persistent messages.
5. Decide the versioned rename strategy from compatibility import `eventstoredb` to first-class `eventszite` without breaking consumers.
6. Keep Linux + Windows CI and `zig fmt --check` as mandatory merge gates.

## Merge criterion

This corrective branch is mergeable only when the current PR head passes the Linux and Windows test jobs plus formatting checks. The review should not claim completion from historical test counts alone; the changed worker/checkpoint paths must compile and execute in CI.
