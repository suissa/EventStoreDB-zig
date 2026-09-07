//! Tiny spinlock built on top of `std.atomic.Mutex`. The Mutex
//! type that ships with Zig 0.16/0.17 is non-blocking, so we
//! spin on `tryLock` with a `compareAndSwap`-style loop.
//!
//! Suitable for short critical sections (a few SQLite statements)
//! where contention is rare. Do NOT hold the lock across any
//! blocking call.

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
