//! `eventstoredb-zig` — EventStoreDB-compatible event store over
//! SQLite, written in Zig.
//!
//! Open with `Client.open`, then call the various methods on the
//! returned `*Client` (defined in this file for the convenience
//! of having every public method visible at once):
//!
//!   const client = try esdb.Client.open(allocator, .{});
//!   defer client.close();
//!
//!   const result = try client.appendToStream(allocator, "s",
//!       .{ .expected_revision = .no_stream },
//!       &[_]esdb.EventData{ ... });

pub const types = @import("types.zig");
pub const errors_mod = @import("errors.zig");
pub const uuid = @import("uuid.zig");
pub const schema_mod = @import("schema.zig");

pub const Client = @import("client.zig").Client;
pub const OpenOptions = types.OpenOptions;
pub const EventData = types.EventData;
pub const RecordedEvent = types.RecordedEvent;
pub const AppendOptions = types.AppendOptions;
pub const AppendResult = types.AppendResult;
pub const ReadOptions = types.ReadOptions;
pub const ReadResult = types.ReadResult;
pub const ReadDirection = types.ReadDirection;
pub const From = types.From;
pub const ExpectedRevision = types.ExpectedRevision;
pub const SubscribeOptions = types.SubscribeOptions;
pub const PersistentOptions = types.PersistentOptions;
pub const PersistentConfig = types.PersistentConfig;
pub const PersistentMessage = types.PersistentMessage;
pub const StreamMetadata = types.StreamMetadata;
pub const StreamInfo = types.StreamInfo;
pub const Snapshot = types.Snapshot;
pub const ProjectionState = types.ProjectionState;
pub const Stats = types.Stats;
pub const Position = types.Position;
pub const Uuid = types.Uuid;

pub const Error = errors_mod.Error;
pub const WrongExpectedVersionError = errors_mod.WrongExpectedVersionError;
pub const freeEvents = types.freeEvents;
pub const freeEvent = types.freeEvent;
pub const freeSnapshots = types.freeSnapshots;
pub const freeStreamInfo = types.freeStreamInfo;
pub const freeProjectionState = types.freeProjectionState;

pub const Subscription = @import("subscribe.zig").Subscription;
pub const PersistentSubscription = @import("persistent.zig").PersistentSubscription;

// Convenient re-exports of the public methods so callers can
// write `client.appendToStream(...)` instead of
// `@import("append.zig").appendToStream(...)`.
pub const appendToStream = @import("append.zig").appendToStream;
pub const readStream = @import("read.zig").readStream;
pub const readAll = @import("read.zig").readAll;
pub const subscribeToStream = @import("subscribe.zig").subscribeToStream;
pub const subscribeToAll = @import("subscribe.zig").subscribeToAll;
pub const createPersistentSubscription = @import("persistent.zig").createPersistentSubscription;
pub const deletePersistentSubscription = @import("persistent.zig").deletePersistentSubscription;
pub const connectPersistentSubscription = @import("persistent.zig").connectPersistentSubscription;
pub const saveSnapshot = @import("snapshot.zig").saveSnapshot;
pub const loadSnapshot = @import("snapshot.zig").loadSnapshot;
pub const listStreams = @import("meta.zig").listStreams;
pub const getStreamInfo = @import("meta.zig").getStreamInfo;
pub const setStreamMetadata = @import("meta.zig").setStreamMetadata;
pub const deleteStream = @import("meta.zig").deleteStream;
pub const saveProjectionState = @import("meta.zig").saveProjectionState;
pub const loadProjectionState = @import("meta.zig").loadProjectionState;

test "library loads" {
    _ = Client;
    _ = appendToStream;
}
