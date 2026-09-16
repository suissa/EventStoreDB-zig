// Build script for the SQLite amalgamation.
//
// Compiles `c/sqlite3.c` (downloaded from sqlite.org) as a static
// library. No system SQLite is required at build or run time.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    lib_module.link_libc = true;
    lib_module.addCSourceFile(.{
        .file = b.path("c/sqlite3.c"),
        .flags = &[_][]const u8{
            "-DSQLITE_ENABLE_FTS5",
            "-DSQLITE_ENABLE_JSON1",
            "-DSQLITE_ENABLE_RTREE",
            "-DSQLITE_THREADSAFE=1",
            "-DSQLITE_DEFAULT_FOREIGN_KEYS=0",
            "-DSQLITE_OMIT_LOAD_EXTENSION", // we don't use loadable extensions
        },
    });
    lib_module.addIncludePath(b.path("c"));

    const lib = b.addLibrary(.{
        .name = "sqlite3",
        .linkage = .static,
        .root_module = lib_module,
    });
    lib.installHeader(b.path("c/sqlite3.h"), "sqlite3.h");
    b.installArtifact(lib);
}
