# EventsZite v0.1 — technical review

Date: 2026-09-15

## Executive summary

EventsZite already has a useful embedded event-store core: SQLite WAL durability, expected-revision appends, stream/global reads, catch-up subscriptions, snapshots, metadata, projection checkpoints, a DCB-oriented schema extension, and a broad test layout.

The main risks found in this review were concentrated in background-worker lifetime and documentation accuracy rather than in the append/read core.

## Corrected in this review

### 1. Client shutdown could become use-after-free

`Client.close()` previously waited only 200 ms for background workers and then destroyed the SQLite connections and the `Client` even if `active_workers` was non-zero. The source comment explicitly acknowledged that a worker could then dereference freed memory.

Correction: shutdown now publishes `closed`, wakes catch-up waiters, and waits until every registered worker exits before destroying connection/client memory.

### 2. Catch-up worker registration failure leaked worker ownership

`runStream` / `runAll` registered their `active_workers` decrement only after `Waker.register()`. An allocator failure during waiter registration therefore allowed a worker to return while the client counter remained non-zero.

Correction: cleanup/accounting defers are installed before waiter registration. Registration failure now closes the queue, marks the subscription done, releases worker context and decrements the client counter.

### 3. Persistent subscription worker used a pointer to a stack value

`connectPersistentSubscription()` spawned `runPS` with `&sub`, where `sub` was local to the function. Once the function returned, the worker could continue using an invalid stack pointer.

Correction: persistent workers now receive a heap-owned `PSRunContext` containing their own stream/group copies plus shared queue/done pointers. Worker context lifetime is independent of movement/copying of the public `PersistentSubscription` value.

### 4. Persistent queue could leak event allocations

Messages dropped because the bounded queue was full were discarded without freeing their owned `RecordedEvent` slices, and subscription close did not drain queued persistent messages.

Correction: the queue is allocator-aware, frees dropped messages, and drains remaining messages on close.

### 5. Persistent worker did not observe client shutdown

The worker loop previously observed only the subscription `done` flag. `Client.close()` could therefore wait for it while the worker continued polling a client being closed.

Correction: `runPS` also observes `Client.closed` and exits before connection teardown.

### 6. Persistent connection path lacked regression coverage

The existing persistent tests covered create/delete/overwrite but never called `connectPersistentSubscription`, allowing Zig's lazy analysis to leave the worker path effectively untested.

Correction: a regression test now creates a connected persistent worker and closes the client while that worker exists.

## Documentation corrections

The repository name and product are EventsZite, but public source/build compatibility still uses `eventstoredb`/`eventstoredb_zig`. That should be treated as an API compatibility name, not the product name.

The previous README wording described the project as an "EventStoreDB-compatible" / "drop-in" local replacement. That is too strong for the current implementation because there is no EventStoreDB wire-protocol implementation and persistent subscription semantics are not yet at server parity.

`docs/EVENTSTOREDB-COMPATIBILITY.md` now defines the compatibility boundary explicitly.

## Remaining technical gaps

These are not hidden by this review and should remain visible in the roadmap:

1. **Persistent checkpoint semantics are incomplete.** `ack()` currently removes the in-flight row but does not yet advance a durable contiguous consumer checkpoint. Restart behavior can therefore redeliver events from the stored `last_position`. This needs a dedicated semantic design rather than a one-line update because out-of-order acknowledgements must not skip earlier unacked events.

2. **Busy-spin synchronization is CPU-expensive.** `Spinlock`, `Waker.wait`, queue receive loops and `sleepMs` all rely on spin hints rather than scheduler-backed waits. This is acceptable only for explicitly bounded/embedded workloads and should eventually move to Zig 0.16 `std.Io` synchronization or another blocking primitive.

3. **Queue capacity is hard-coded to 4096.** `SubscribeOptions.buffer_size` exists but the catch-up queue does not use it. Either wire this option to a dynamic bounded queue or remove/document it until implemented.

4. **`OpenOptions.max_connections` is currently descriptive only.** The client exposes the option but there is no pool enforcing it.

5. **`separate_read_connection` should likely become the production default for file-backed stores.** It avoids self-contention between subscription reads and writes in WAL mode. `:memory:` needs special handling because independent SQLite `:memory:` connections do not share the same database unless a shared-cache URI is used.

6. **CI must be treated as the merge gate.** The `v0.0.1` main commit had no workflow run attached when this review started. The corrective PR should not merge until Linux and Windows builds plus format checks pass.

## Recommended next milestone

For v0.2, prioritize semantics over feature count:

- durable contiguous persistent-subscription checkpoints;
- redelivery/retry/parking rules with restart tests;
- allocator-failure tests for worker startup;
- close-during-subscription chaos tests;
- dynamic queue capacity or removal of the unused option;
- blocking synchronization instead of indefinite CPU spinning;
- rename strategy from compatibility import `eventstoredb` to first-class `eventszite` without breaking existing users.
