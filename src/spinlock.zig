//! Tiny spinlock built on top of `std.atomic.Mutex`. The Mutex
//! type that ships with Zig 0.16/0.17 is non-blocking, so we
//! spin on `tryLock` with a `compareAndSwap`-style loop.
//!
//! ## Two distinct use modes in this repository
//!
//! 1. **Short critical sections.** The intended use, and the
//!    only one this module's contract is designed to cover:
//!    a handful of in-memory statements with no I/O. Follow
//!    the rule: do NOT hold the lock across any blocking call.
//!
//! 2. **`Client.writer_mu` protecting a SQLite transaction.**
//!    This is a *deliberate exception*. `appendToStream`
//!    acquires `writer_mu` before `BEGIN IMMEDIATE` and only
//!    releases it after `COMMIT`, holding it across disk I/O
//!    (fsync under `synchronous=NORMAL`). The trade-off, made
//!    explicit because it contradicts the rule above:
//!
//!    - *Why we accept it:* SQLite is single-writer per file.
//!      Serialising same-process appends at the in-process
//!      level keeps the transaction code simple and avoids
//!      SQLITE_BUSY by construction; cross-process contention
//!      is still absorbed by `PRAGMA busy_timeout` configured
//!      by `schema.zig::Connection.open`. We do not hold a
//!      kernel-sleeping mutex while a co-located `PRAGMA`
//!      wait could do the same job.
//!
//!    - *Residual risk:* under heavy contention, concurrent
//!      appenders busy-spin a core while one of them waits on
//!      disk I/O. There is no priority inversion bug because
//!      the spinning threads do not hold any lock that the
//!      I/O waiter needs — but the system can burn a core
//!      pointlessly. The pragmatic mitigation for v0.1 is to
//!      bound throughput at the application level (this
//!      library is documented as single-writer / dev & edge).
//!      A future revision may swap the spinlock for a
//!      `std.Io.Mutex` once we migrate the rest of the API to
//!      take an `Io`.

const std = @import("std");

pub const Spinlock = struct {
    state: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *Spinlock) void {
        while (!self.state.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *Spinlock) void {
        self.state.unlock();
    }
};
