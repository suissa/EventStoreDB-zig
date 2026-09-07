// Build configuration for eventstoredb-zig.
//
// The SQLite C amalgamation is downloaded on the first build from
// https://www.sqlite.org/. It is then compiled as a static library
// and linked into the library, CLI and tests. No external C compiler
// is required: Zig's bundled mingw is used for Windows targets.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // -------------------------------------------------------------
    // SQLite amalgamation: download once, compile as a static lib.
    // -------------------------------------------------------------
    const sqlite = b.dependency("sqlite", .{
        .target = target,
        .optimize = optimize,
    });
    const sqlite_lib = sqlite.artifact("sqlite3");

    // -------------------------------------------------------------
    // Library: the public eventstoredb API.
    // -------------------------------------------------------------
    const lib_mod = b.addModule("eventstoredb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_mod.linkLibrary(sqlite_lib);

    // -------------------------------------------------------------
    // Library test artifact.
    // -------------------------------------------------------------
    const test_module = b.createModule(.{
        .root_source_file = b.path("tests/eventstoredb_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "eventstoredb", .module = lib_mod },
        },
    });
    test_module.linkLibrary(sqlite_lib);
    const lib_tests = b.addTest(.{ .root_module = test_module });
    const test_step = b.step("test", "Run the library test suite");
    test_step.dependOn(&b.addRunArtifact(lib_tests).step);

    // -------------------------------------------------------------
    // CLI binary (server + stats + tail).
    // -------------------------------------------------------------
    const exe_module = b.createModule(.{
        .root_source_file = b.path("cmd/server.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "eventstoredb", .module = lib_mod },
        },
    });
    exe_module.linkLibrary(sqlite_lib);
    const exe = b.addExecutable(.{
        .name = "eventstoredb-zig",
        .root_module = exe_module,
    });
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the CLI server with default flags");
    const run_artifact = b.addRunArtifact(exe);
    run_artifact.addArg("serve");
    run_artifact.addArg("--data");
    run_artifact.addArg("eventstore.db");
    run_artifact.addArg("--listen");
    run_artifact.addArg(":2113");
    run_step.dependOn(&run_artifact.step);

    // -------------------------------------------------------------
    // Examples: each is a runnable executable.
    // -------------------------------------------------------------
    const example_targets = [_]struct { name: []const u8, path: []const u8 }{
        .{ .name = "basic", .path = "examples/basic/main.zig" },
        .{ .name = "subscribe", .path = "examples/subscribe/main.zig" },
        .{ .name = "persistent", .path = "examples/persistent/main.zig" },
        .{ .name = "snapshots", .path = "examples/snapshots/main.zig" },
    };
    const example_step = b.step("examples", "Build all examples");
    for (example_targets) |ex| {
        const ex_module = b.createModule(.{
            .root_source_file = b.path(ex.path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "eventstoredb", .module = lib_mod },
            },
        });
        ex_module.linkLibrary(sqlite_lib);
        const artifact = b.addExecutable(.{
            .name = ex.name,
            .root_module = ex_module,
        });
        const install = b.addInstallArtifact(artifact, .{
            .dest_dir = .{ .override = .{ .custom = "examples" } },
        });
        example_step.dependOn(&install.step);
    }
}
