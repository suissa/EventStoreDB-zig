//! Broadcast primitive used to wake catch-up subscriptions
//! the moment a new event is appended, instead of waiting for
//! the next poll tick. Built on top of `std.atomic.Mutex` and
//! `std.Thread.Condition` (which is still exposed in Zig 0.17).

const std = @import("std");

pub const Waker = struct {
    const Waiter = struct {
        ch: std.Thread.Mutex = .{},
        cond: std.Thread.Condition = .{},
        armed: bool = false,
        closed: bool = false,
        next: ?*Waiter = null,
    };

    mu: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Waker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Waker) void {
        // Close every waiter's condition.
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            const next = w.next;
            w.ch.lock();
            w.closed = true;
            w.cond.signal();
            w.ch.unlock();
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
            prev = node;
            cur = node.next;
        }
        self.unlock();
        w.ch.lock();
        w.closed = true;
        w.cond.signal();
        w.ch.unlock();
        self.allocator.destroy(w);
    }

    pub fn wait(w: *Waiter, timeout_ms: u32) bool {
        w.ch.lock();
        defer w.ch.unlock();
        if (w.armed) {
            w.armed = false;
            return true;
        }
        if (w.closed) return false;
        const ns: u63 = @as(u63, timeout_ms) * std.time.ns_per_ms;
        w.cond.timedWait(&w.ch, ns) catch {};
        if (w.armed) {
            w.armed = false;
            return true;
        }
        return false;
    }

    pub fn signal(self: *Waker) void {
        self.lock();
        var cur = self.head;
        while (cur) |w| {
            w.ch.lock();
            w.armed = true;
            w.cond.signal();
            w.ch.unlock();
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
