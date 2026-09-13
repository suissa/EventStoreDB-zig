# Repository Guidelines

## Project Structure & Module Organization

This is a Zig 0.16 library implementing an EventStoreDB-compatible event store on SQLite.

- `src/` contains the public library; `src/root.zig` is the `@import("eventstoredb")` entry point. Storage/schema code is in `schema.zig`, client state in `client.zig`, and operations in `append.zig`, `read.zig`, `subscribe.zig`, `persistent.zig`, `snapshot.zig`, and `meta.zig`.
- `cmd/` contains CLI programs; `examples/` contains runnable usage examples.
- `tests/unit/` covers individual features. `tests/load/`, `stress/`, `chaos/`, `security/`, `dcb/`, and `bench/` cover specialized behavior; shared helpers live in `common.zig` files.
- `docs/` documents architecture and compatibility. SQLite is vendored under `vendor/sqlite/`; do not casually regenerate the amalgamation.

## Build, Test, and Development Commands

Use Zig 0.16.x (minimum `0.16.0`):

```bash
zig build                         # Build the library, CLI, and default artifacts
zig build test                    # Run the quick full suite
zig build test-unit-append        # Run one named test module
zig build test-stress-concurrent  # Run the contention stress suite
zig build test-bench-micro        # Run micro-benchmarks
zig build examples                # Build example binaries
zig build run                     # Run the CLI against eventstore.db on :2113
```

The combined test step excludes the longer benchmark and racy stress suites. Use `zig build -Dtarget=x86_64-linux-gnu` to verify a cross-target build.

## Coding Style & Naming Conventions

Follow existing Zig formatting and run `zig fmt` on changed `.zig` files. Use four-space indentation, `snake_case` for functions/files/locals, and `PascalCase` for types. Prefer explicit allocator ownership and typed error sets. Keep SQL parameterized through `src/bind.zig`; do not interpolate user-controlled values.

## Testing Guidelines

Add focused `test "descriptive behavior"` cases near the relevant suite. Use `std.testing.allocator` for returned data and explicitly free owned event fields and slices. Include security, concurrency, or failure-path coverage when changing those areas. Run `zig build test`; run the relevant stress/bench target for concurrency or performance changes.

## Commit & Pull Request Guidelines

History contains version-style commits such as `v0.0.1`, so no detailed convention is established. Use short, imperative subjects (for example, `Fix persistent ack cursor`) and keep unrelated changes separate. PRs should explain behavior and compatibility impact, identify tests run, call out schema or migration changes, and include reproducible examples when behavior changes.

## Security & Configuration Tips

Treat database paths and event payloads as untrusted input. Preserve prepared statements, validate paths, and run the security targets when modifying input or persistence code. Avoid committing generated databases, WAL/SHM files, or debug artifacts.
