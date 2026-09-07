//! Public types: events, positions, expected revisions, options, etc.
//!
//! All types are allocator-aware where appropriate. The Client
//! is not (it owns a `*sqlite3` connection and is created with
//! `Client.open`). Owned slices are documented with their owner.

const std = @import("std");

/// 128-bit universally unique identifier, stored as 16 bytes.
/// Matches the wire layout used by the official EventStoreDB
/// client and the Go port in `eventstoredb-sqlite`.
pub const Uuid = [16]u8;

/// Event input to `appendToStream`.
pub const EventData = struct {
    /// Optional caller-supplied ID; the client fills one in if
    /// left zero. Reusing the same ID on retry of the same
    /// logical write is idempotent.
    event_id: Uuid = .{0} ** 16,
    event_type: []const u8,
    data: []const u8,
    metadata: ?[]const u8 = null,
};

/// An event as it lives in the store. The `data` / `metadata`
/// pointers are borrowed from the row buffer and only valid
/// until the next operation on the same `Client`. Copy them
/// out if you need to keep them around.
pub const RecordedEvent = struct {
    event_id: Uuid,
    stream_id: []const u8,
    event_type: []const u8,
    data: []const u8,
    metadata: ?[]const u8,

    /// Per-stream event number (0-based).
    revision: u64,

    /// Global commit position (1-based; the first event
    /// appended to an empty store gets position 1).
    log_position: u64,

    /// Global transaction position. Events appended in the
    /// same `appendToStream` call share a transaction position
    /// and a contiguous range of log positions.
    transaction_position: u64,
};

/// Result of a successful append.
pub const AppendResult = struct {
    next_revision: u64,
    log_position: u64,
    transaction_position: u64,

    /// Echoes back the events that were actually stored,
    /// including the positions they were assigned. If the
    /// call reused some pre-existing event IDs, the
    /// corresponding entries point at the original events.
    events: []const RecordedEvent,
};

/// Position in the global all-stream log. Used as a
/// checkpoint for read `$all` and subscribe `$all`.
pub const Position = struct {
    commit: u64 = 0,
    prepare: u64 = 0,

    pub fn isZero(self: Position) bool {
        return self.commit == 0 and self.prepare == 0;
    }

    pub const start_of_log: Position = .{};
    pub const end_of_log: Position = .{ .commit = std.math.maxInt(u64), .prepare = std.math.maxInt(u64) };
};

/// Result of `readStream` / `readAll`.
pub const ReadResult = struct {
    events: []const RecordedEvent,
    next_revision: u64,
    next_position: Position,
    is_end_of_stream: bool,
};

/// Reading direction.
pub const ReadDirection = enum { forward, backward };

/// Starting point for reads and subscriptions.
pub const From = union(enum) {
    /// From the very first event (or end, for backward reads).
    start: void,
    /// Live from the current tip.
    end: void,
    /// Per-stream starting revision.
    revision: u64,
    /// All-stream starting position.
    position: Position,
};

/// Expected revision contract for an append.
pub const ExpectedRevision = union(enum) {
    /// Stream must not exist.
    no_stream: void,
    /// Stream must already exist.
    stream_exists: void,
    /// Stream must currently be at exactly this revision.
    revision: u64,
    /// Disable the check (last-write-wins).
    any: void,
};

/// Per-call options for `appendToStream`.
pub const AppendOptions = struct {
    expected_revision: ExpectedRevision = .{ .any = {} },
};

/// Per-call options for reads.
pub const ReadOptions = struct {
    from: From = .{ .start = {} },
    direction: ReadDirection = .forward,
    limit: u32 = 0, // 0 = client default
};

/// Per-call options for catch-up subscriptions.
pub const SubscribeOptions = struct {
    from: From = .{ .start = {} },
    poll_interval_ms: u32 = 0, // 0 = client default
    buffer_size: u32 = 0, // 0 = default (256)
};

/// Per-call options for persistent subscriptions.
pub const PersistentOptions = struct {
    group_name: []const u8,
    from: From = .{ .start = {} },
    max_retries: u32 = 10,
    ack_timeout_ms: u32 = 30_000,
};

/// Persistent subscription configuration stored alongside the
/// group row. Tunes the read side of the engine.
pub const PersistentConfig = struct {
    resolve_link_tos: bool = false,
    extra_statistics: bool = false,
    max_retry_count: u32 = 0,
    check_point_after: u32 = 0,
    min_check_point_count: u32 = 0,
    max_check_point_count: u32 = 0,
    live_buffer_size: u32 = 0,
    read_batch_size: u32 = 0,
};

/// Per-message ack handle returned by `PersistentSubscription.messages`.
pub const PersistentMessage = struct {
    event: RecordedEvent,
    retry_count: u32 = 0,
};

/// Per-stream metadata held in `streams` (mirrors the
/// $-metadata stream of the official server).
pub const StreamMetadata = struct {
    max_count: i64 = 0,
    truncate_before: u64 = 0,
    custom_metadata: ?[]const u8 = null,
};

/// One entry of the implicit `$streams` catalog.
pub const StreamInfo = struct {
    stream_id: []const u8,
    revision: u64,
    max_count: i64,
    truncate_before: u64,
    deleted: bool,
};

/// Per-stream snapshot.
pub const Snapshot = struct {
    stream_id: []const u8,
    revision: u64,
    payload: []const u8,
    metadata: ?[]const u8,
};

/// Projector state, kept in the `projection_checkpoints` table.
pub const ProjectionState = struct {
    name: []const u8,
    last_position: u64,
    state: ?[]const u8,
};

/// Aggregated statistics. Returned by `Client.stats`.
pub const Stats = struct {
    stream_count: i64,
    event_count: i64,
    tombstoned_streams: i64,
    persistent_groups: i64,
    snapshots: i64,
    db_size_bytes: i64,
    last_log_position: u64,
};

/// Open-time options.
pub const OpenOptions = struct {
    /// Database path; ":memory:" for an in-memory store.
    path: []const u8 = ":memory:",

    /// SQLite busy_timeout in milliseconds. Default 5000.
    busy_timeout_ms: u32 = 5000,

    /// Default poll interval for catch-up subscriptions. Default 100ms.
    poll_interval_ms: u32 = 100,

    /// Default page size for read calls that don't override. Default 1024.
    max_batch_size: u32 = 1024,

    /// Max concurrent connections. Default 4.
    max_connections: u32 = 4,
};
