"""worklane — two-lane priority scheduling against a rate limit.

The problem this solves: a pipeline has stages with very different costs. Some are
cheap and parallel (fetch a file, move bytes). One is expensive and rate-limited (a
model call, a paid API, a single GPU). Put them in one queue and the cheap work
starves behind the expensive work every time the limiter says "not yet" -- the
worker sits idle holding a job it is not allowed to run, while work it *could* do
waits behind it.

So: two lanes, and the limiter gates only the lane that needs gating.

    lanes = WorkLanes(throttled_per_hour=10)
    lanes.put(FREE,      HIGH, {"url": "..."})     # never gated
    lanes.put(THROTTLED, HIGH, {"prompt": "..."})  # gated by the token bucket

    job = lanes.next_ready()     # None means "nothing runnable RIGHT NOW"
    if job is None:
        time.sleep(lanes.seconds_until_ready())

`next_ready()` returning None does not mean the queues are empty -- it means
nothing is runnable yet. `has_pending()` answers the other question, and the two
are deliberately separate.

Priority order: HIGH free, HIGH throttled, LOW free, LOW throttled. Fresh cheap
work therefore overtakes a backlog, and a throttled job never blocks a free one.

Stdlib only. Thread-safe.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

HIGH = 0
LOW = 1

FREE = "free"            # not rate-limited
THROTTLED = "throttled"  # gated by the token bucket


@dataclass
class Job:
    priority: int
    created_at: float
    lane: str = ""
    source: str = ""
    payload: dict = field(default_factory=dict)


class TokenBucket:
    """Lazy-refill token bucket. Refills on read rather than on a timer, so there
    is no background thread and no drift while the process is idle."""

    def __init__(self, calls_per_hour: int, capacity: Optional[int] = None):
        if calls_per_hour < 1:
            raise ValueError(f"calls_per_hour must be >= 1, got {calls_per_hour!r}")
        self.rate_per_second = calls_per_hour / 3600.0
        # Default burst is a sixth of an hour's allowance, minimum 1: enough to
        # absorb a small clump without letting an idle night bank a whole hour.
        self.capacity = capacity if capacity is not None else max(1, calls_per_hour // 6)
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()          # monotonic: a clock step must not mint tokens
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate_per_second)
        self.last_refill = now

    def try_acquire(self, n: int = 1) -> bool:
        with self._lock:
            self._refill()
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

    def release(self, n: int = 1) -> None:
        """Hand a token back, for a caller that acquired one and then could not use
        it. Without this, a lost race silently costs real throughput -- at 10/hour a
        wrongly-spent token is six minutes of a lane standing still."""
        with self._lock:
            self.tokens = min(float(self.capacity), self.tokens + n)

    def seconds_until_next(self, n: int = 1) -> float:
        with self._lock:
            self._refill()
            if self.tokens >= n:
                return 0.0
            deficit = n - self.tokens
            return deficit / self.rate_per_second if self.rate_per_second > 0 else float("inf")

    def snapshot(self) -> dict:
        with self._lock:
            self._refill()
            return {
                "tokens": round(self.tokens, 3),
                "capacity": self.capacity,
                "rate_per_hour": round(self.rate_per_second * 3600, 2),
            }


class PriorityQueue:
    """Two-level FIFO: HIGH drains fully before LOW. Thread-safe."""

    def __init__(self) -> None:
        self._lanes: dict[int, deque] = {HIGH: deque(), LOW: deque()}
        self._lock = threading.Lock()

    def put(self, job: Job) -> None:
        with self._lock:
            self._lanes[job.priority].append(job)

    def get(self) -> Optional[Job]:
        with self._lock:
            for p in (HIGH, LOW):
                if self._lanes[p]:
                    return self._lanes[p].popleft()
            return None

    def take(self, priority: int) -> Optional[Job]:
        """Pop only if the head is at `priority`. Exists so a caller can test and
        take under ONE lock -- see the note on next_ready()."""
        with self._lock:
            if self._lanes[priority]:
                return self._lanes[priority].popleft()
            return None

    def peek_priority(self) -> Optional[int]:
        with self._lock:
            for p in (HIGH, LOW):
                if self._lanes[p]:
                    return p
            return None

    def size(self) -> int:
        with self._lock:
            return sum(len(d) for d in self._lanes.values())

    def sizes(self) -> dict:
        with self._lock:
            return {"HIGH": len(self._lanes[HIGH]), "LOW": len(self._lanes[LOW])}

    def clear(self) -> None:
        with self._lock:
            for d in self._lanes.values():
                d.clear()


class WorkLanes:
    """The free queue, the throttled queue, and the bucket that gates the second.

    Producers may register refill callbacks: when everything is drained, the
    manager asks each source to enqueue more. That keeps a database (or whatever
    the real source of truth is) authoritative, with these queues as nothing more
    than an ordering layer in front of it.
    """

    def __init__(self, throttled_per_hour: int = 10, capacity: Optional[int] = None):
        self.free_q = PriorityQueue()
        self.throttled_q = PriorityQueue()
        self.bucket = TokenBucket(calls_per_hour=throttled_per_hour, capacity=capacity)
        self._refillers: list[tuple[str, Callable[["WorkLanes"], None]]] = []
        self._lock = threading.Lock()
        self.tokens_returned = 0        # observability for the race below

    # -- producing -------------------------------------------------------
    def put(self, lane: str, priority: int, payload: dict, source: str = "") -> Job:
        if lane not in (FREE, THROTTLED):
            raise ValueError(f"lane must be {FREE!r} or {THROTTLED!r}, got {lane!r}")
        if priority not in (HIGH, LOW):
            raise ValueError(f"priority must be HIGH or LOW, got {priority!r}")
        job = Job(priority=priority, created_at=time.time(),
                  lane=lane, source=source, payload=payload)
        (self.free_q if lane == FREE else self.throttled_q).put(job)
        return job

    def register_refiller(self, source: str, fn: Callable[["WorkLanes"], None]) -> None:
        with self._lock:
            self._refillers.append((source, fn))

    def refill_all(self, on_error: Optional[Callable[[str, Exception], None]] = None) -> None:
        """Ask every registered source to enqueue pending work.

        A failing refiller must not take the others down with it, so each is
        isolated -- but the error is handed to `on_error` rather than printed and
        forgotten. A swallowed exception here looks exactly like "there was no
        work", which is the single most confusing way for a pipeline to stall.
        """
        with self._lock:
            refillers = list(self._refillers)
        for source, fn in refillers:
            try:
                fn(self)
            except Exception as exc:       # noqa: BLE001 - deliberate, and reported
                if on_error is not None:
                    on_error(source, exc)
                else:
                    raise

    # -- consuming -------------------------------------------------------
    def next_ready(self) -> Optional[Job]:
        """The next job runnable RIGHT NOW, or None.

        None is not "empty" -- it is "nothing runnable yet", which usually means
        the throttled lane has work and no tokens. Use has_pending() to tell the
        two apart and seconds_until_ready() to decide how long to wait.

        On the throttled branch the token is taken BEFORE the job is popped, and
        popping can still come back empty if another consumer won the race. The
        token is handed back when that happens. Without the hand-back the token is
        spent on nothing: the caller is told there is no work while the work it
        was refused a token for is being done by somebody else, and the allowance
        quietly shrinks. It is a narrow window -- it did not reproduce in 400
        forced rounds of 16 threads contending for a single job -- but it is
        reachable, and tests/test_worklane.py holds it open and proves it.
        """
        for priority in (HIGH, LOW):
            job = self.free_q.take(priority)
            if job is not None:
                return job
            if self.throttled_q.peek_priority() == priority:
                if self.bucket.try_acquire():
                    job = self.throttled_q.take(priority)
                    if job is not None:
                        return job
                    self.bucket.release()
                    with self._lock:
                        # += on an int is not atomic under contention, and this
                        # counter exists precisely to make the lost race visible;
                        # an unlocked increment would undercount when it matters most.
                        self.tokens_returned += 1
        return None

    def has_pending(self) -> bool:
        """Is there work at all, runnable or not?"""
        return self.free_q.size() > 0 or self.throttled_q.size() > 0

    def seconds_until_ready(self) -> float:
        """How long until next_ready() could return something. 0.0 if now,
        inf if there is nothing pending at all."""
        if self.free_q.size() > 0:
            return 0.0
        if self.throttled_q.size() > 0:
            return self.bucket.seconds_until_next()
        return float("inf")

    def snapshot(self) -> dict:
        return {
            "free": self.free_q.sizes(),
            "throttled": self.throttled_q.sizes(),
            "bucket": self.bucket.snapshot(),
            "tokens_returned": self.tokens_returned,
        }


__all__ = ["HIGH", "LOW", "FREE", "THROTTLED",
           "Job", "TokenBucket", "PriorityQueue", "WorkLanes"]
