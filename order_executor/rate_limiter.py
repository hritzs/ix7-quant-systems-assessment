"""Strict sliding-window (rolling) rate limiter.

At most ``limit`` events in ANY window of ``window`` seconds, boundaries
included: an event at time t is allowed only if the ``limit``-th most recent
event happened strictly more than ``window`` before t. A fixed-window counter
("reset every second") would allow 2 x limit across a boundary (10 at
0.99 s + 10 at 1.01 s); a token bucket allows a burst of ``limit`` after an idle
period but its refill can also exceed ``limit`` in a rolling window. The
sliding log is exact and costs O(1) memory per limit slot: it only keeps the
last ``limit`` dispatch times (a ``deque`` with ``maxlen``).

``safety_margin`` adds a small gap (default 1 ms) on top of the window to absorb
timestamp jitter between the moment we stamp a dispatch and the moment the
exchange stamps its arrival.

Not thread-safe on purpose: it is owned by the executor's single dispatcher
task (single-writer principle), so it needs no lock.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable

NS_PER_SECOND = 1_000_000_000


class SlidingWindowRateLimiter:
    def __init__(
        self,
        limit: int = 10,
        window: float = 1.0,
        safety_margin: float = 0.001,
        clock_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if limit <= 0 or window <= 0 or safety_margin < 0:
            raise ValueError("limit and window must be positive, margin non-negative")
        self.limit = limit
        self.window_ns = int(window * NS_PER_SECOND)
        self.margin_ns = max(1, int(safety_margin * NS_PER_SECOND))  # >= 1 ns: strictly after the window
        self.clock_ns = clock_ns
        self._events: deque[int] = deque(maxlen=limit)

    def next_slot_ns(self, now_ns: int | None = None) -> int:
        """Earliest time (clock ns) at which one more event is allowed."""
        now = self.clock_ns() if now_ns is None else now_ns
        if len(self._events) < self.limit:
            return now
        return max(now, self._events[0] + self.window_ns + self.margin_ns)

    def record(self, ts_ns: int) -> None:
        """Record an event that was allowed by ``next_slot_ns``. The executor
        records the time AFTER the exchange call returned: an upper bound on the
        exchange's arrival stamp, so the limit also holds as the exchange sees it."""
        if self._events and ts_ns < self._events[-1]:
            raise RuntimeError("clock went backwards")  # monotonic clock: cannot happen
        self._events.append(ts_ns)

    def try_acquire(self, now_ns: int | None = None) -> int | None:
        """Record an event now if allowed; returns its timestamp, or None."""
        now = self.clock_ns() if now_ns is None else now_ns
        if len(self._events) == self.limit and now < self._events[0] + self.window_ns + self.margin_ns:
            return None
        if self._events and now < self._events[-1]:
            raise RuntimeError("clock went backwards")  # monotonic clock: cannot happen
        self._events.append(now)  # maxlen drops the oldest
        return now

    def in_window(self, now_ns: int | None = None) -> int:
        now = self.clock_ns() if now_ns is None else now_ns
        return sum(1 for t in self._events if now - t <= self.window_ns)
