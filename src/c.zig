//! Thin `@cImport` wrapper around the vendored SQLite amalgamation.
//!
//! Every public symbol of the SQLite C API that the library uses
//! is re-exported here so the rest of the codebase only depends
//! on `eventstoredb.c`, not on the C header directly.

pub const c = @cImport({
    @cInclude("sqlite3.h");
});
