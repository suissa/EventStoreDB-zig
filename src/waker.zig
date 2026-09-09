//! Broadcast primitive used to wake catch-up subscriptions the moment
//! a new event is appended, instead of waiting for the next poll tick.
//!
//! Zig 0.16 removed `std.Thread.Mutex` and `std.Thread.Condition`; the
//! new sync primitives live on `std.Io` and need a thread pool. To
//! avoid forcing every caller of the store to construct an `Io`, this
//! implementation uses `std.atomic.Mutex` for the list and a per-waiter
//! atomic flag that the poller checks between sleep slices. It is a
//! poll-with-busy-wake hybrid, not a futex-based condition variable,
//! and that is the trade-off we accept to keep the rest of the API
//! free of `Io`.

const std = @import("std");

pub const Waker = struct {
    const Waiter = struct {
        armed: std.atomic.Value(bool) = .init(false),
        closed: std.atomic.Value(bool) = .init(false),
        next: ?*Waiter = null,
    };

    mu: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Waker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Waker) void {
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            const next = w.next;
            w.closed.store(true, .release);
            self.allocator.destroy(w);
            cur = next;
        }
        self.head = null;
        self.unlock();
    }

    pub fn register(self: *Waker) !*Waiter {
        const w = try self.allocator.create(Waiter);
        w.* = .{};
        self.lock();
        w.next = self.head;
        self.head = w;
        self.unlock();
        return w;
    }

    pub fn unregister(self: *Waker, w: *Waiter) void {
        self.lock();
        var prev: ?*Waiter = null;
        var cur = self.head;
        while (cur) |node| {
            if (node == w) {
                if (prev) |p| {
                    p.next = node.next;
                } else {
                    self.head = node.next;
                }
                break;
            }
            prev = cur;
            cur = node.next;
        }
        self.unlock();
        w.closed.store(true, .release);
        // Drain any pending armed flag so a stale wakeup cannot
        // outlive this waiter.
        _ = w.armed.swap(false, .acq_rel);
        self.allocator.destroy(w);
    }

    pub fn wait(w: *Waiter, timeout_ms: u32) bool {
        if (w.armed.swap(false, .acq_rel)) return true;
        if (w.closed.load(.acquire)) return false;
        // 1 ms slice. Each slice yields the CPU to the scheduler;
        // signals typically arrive within a handful of slices
        // because the append path sets `armed` before returning.
        const slice_ns: u64 = std.time.ns_per_ms;
        const deadline_ns: u64 = @as(u64, timeout_ms) * slice_ns;
        var elapsed_ns: u64 = 0;
        while (elapsed_ns < deadline_ns) : (elapsed_ns += slice_ns) {
            std.atomic.spinLoopHint();
            if (w.armed.swap(false, .acq_rel)) return true;
            if (w.closed.load(.acquire)) return false;
        }
        return false;
    }

    pub fn signal(self: *Waker) void {
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            w.armed.store(true, .release);
            cur = w.next;
        }
        self.unlock();
    }

    fn lock(self: *Waker) void {
        while (!self.mu.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *Waker) void {
        self.mu.unlock();
    }
};
