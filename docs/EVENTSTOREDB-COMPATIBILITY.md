# EventStoreDB compatibility

EventsZite implements an EventStoreDB-inspired programming model on top of SQLite. It is **not** a wire-compatible EventStoreDB server and should not be described as a drop-in replacement for an EventStoreDB deployment.

## What is intentionally compatible

The public Zig API models familiar EventStoreDB concepts:

- named streams and per-stream revisions;
- append with expected revision checks;
- global log positions;
- stream and `$all` reads;
- catch-up subscriptions;
- persistent-subscription-like consumer groups;
- stream metadata, snapshots, projection checkpoints and tombstones.

The Zig import name remains `eventstoredb` for source compatibility during the EventsZite rename.

## What is not compatible

EventsZite currently does not implement the EventStoreDB gRPC/TCP wire protocols, server clustering, quorum replication, server-side projections, authentication/authorization semantics, or the full persistent subscription protocol.

Persistent subscriptions in v0.1 should be treated as an embedded consumer-group implementation. Delivery/in-flight tracking exists, but full restart/checkpoint semantics and parity with EventStoreDB persistent subscriptions are still incomplete.

## Concurrency model

SQLite remains the durability and serialization boundary. EventsZite uses one writer connection per `Client`. `OpenOptions.separate_read_connection = true` opens a second connection for subscription workers and is recommended when append and subscription load run concurrently.

## Compatibility policy

Compatibility claims in documentation should use the wording **"EventStoreDB-inspired API/model"** unless a behavior is covered by a regression test. Wire compatibility must never be implied unless an actual EventStoreDB protocol endpoint exists and is verified against official clients.
