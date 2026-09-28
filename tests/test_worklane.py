import threading
import time
import unittest

from worklane import HIGH, LOW, FREE, THROTTLED, Job, TokenBucket, PriorityQueue, WorkLanes


class TestTokenBucket(unittest.TestCase):
    def test_starts_full_at_capacity(self):
        b = TokenBucket(calls_per_hour=60, capacity=5)
        self.assertEqual(b.snapshot()["tokens"], 5.0)

    def test_default_capacity_is_a_sixth_of_the_hourly_allowance(self):
        self.assertEqual(TokenBucket(calls_per_hour=60).capacity, 10)

    def test_default_capacity_is_never_zero(self):
        # a slow rate must still permit one call, or the lane can never start
        for rate in (1, 2, 5):
            self.assertGreaterEqual(TokenBucket(calls_per_hour=rate).capacity, 1)
            self.assertTrue(TokenBucket(calls_per_hour=rate).try_acquire())

    def test_acquire_consumes_and_then_refuses(self):
        b = TokenBucket(calls_per_hour=1, capacity=2)
        self.assertTrue(b.try_acquire())
        self.assertTrue(b.try_acquire())
        self.assertFalse(b.try_acquire())

    def test_refill_is_clamped_to_capacity(self):
        b = TokenBucket(calls_per_hour=3600, capacity=3)   # 1/sec
        b.tokens = 0.0
        b.last_refill -= 1000                              # a long idle period
        self.assertEqual(b.snapshot()["tokens"], 3.0, "an idle night must not bank an hour")

    def test_refills_over_time(self):
        b = TokenBucket(calls_per_hour=3600, capacity=10)  # 1/sec
        b.tokens = 0.0
        b.last_refill -= 5
        self.assertAlmostEqual(b.snapshot()["tokens"], 5.0, places=1)

    def test_release_hands_a_token_back(self):
        b = TokenBucket(calls_per_hour=60, capacity=2)
        b.try_acquire(); b.try_acquire()
        self.assertFalse(b.try_acquire())
        b.release()
        self.assertTrue(b.try_acquire())

    def test_release_cannot_push_above_capacity(self):
        b = TokenBucket(calls_per_hour=60, capacity=2)
        for _ in range(10):
            b.release()
        self.assertEqual(b.snapshot()["tokens"], 2.0)

    def test_seconds_until_next_is_zero_when_a_token_is_available(self):
        self.assertEqual(TokenBucket(60, capacity=1).seconds_until_next(), 0.0)

    def test_seconds_until_next_reports_the_real_wait(self):
        b = TokenBucket(calls_per_hour=3600, capacity=1)   # 1/sec
        b.try_acquire()
        self.assertAlmostEqual(b.seconds_until_next(), 1.0, places=1)

    def test_uses_a_monotonic_clock(self):
        # a wall-clock step (NTP, DST) must not be able to mint tokens
        b = TokenBucket(calls_per_hour=3600, capacity=5)
        self.assertLess(b.last_refill, time.monotonic() + 1)
        self.assertGreater(b.last_refill, time.monotonic() - 60)

    def test_concurrent_acquires_never_oversubscribe(self):
        b = TokenBucket(calls_per_hour=1, capacity=50)
        wins, start = [], threading.Barrier(25)
        def go():
            start.wait()
            for _ in range(10):
                if b.try_acquire():
                    wins.append(1)
        ts = [threading.Thread(target=go) for _ in range(25)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(len(wins), 50, "exactly capacity tokens may ever be handed out")


class TestPriorityQueue(unittest.TestCase):
    def mk(self, p, key=""):
        return Job(priority=p, created_at=time.time(), payload={"id": key})

    def test_high_drains_before_low(self):
        q = PriorityQueue()
        q.put(self.mk(LOW, "low")); q.put(self.mk(HIGH, "high"))
        self.assertEqual(q.get().payload["id"], "high")
        self.assertEqual(q.get().payload["id"], "low")

    def test_fifo_within_a_priority(self):
        q = PriorityQueue()
        for i in "abc":
            q.put(self.mk(HIGH, i))
        self.assertEqual([q.get().payload["id"] for _ in "abc"], ["a", "b", "c"])

    def test_get_on_empty_returns_none(self):
        self.assertIsNone(PriorityQueue().get())

    def test_take_only_pops_the_requested_priority(self):
        q = PriorityQueue(); q.put(self.mk(LOW, "l"))
        self.assertIsNone(q.take(HIGH), "must not steal from another priority")
        self.assertEqual(q.take(LOW).payload["id"], "l")

    def test_sizes_and_clear(self):
        q = PriorityQueue()
        q.put(self.mk(HIGH)); q.put(self.mk(LOW)); q.put(self.mk(LOW))
        self.assertEqual(q.sizes(), {"HIGH": 1, "LOW": 2})
        self.assertEqual(q.size(), 3)
        q.clear()
        self.assertEqual(q.size(), 0)

    def test_concurrent_consumers_never_get_the_same_job_twice(self):
        q = PriorityQueue()
        for i in range(500):
            q.put(self.mk(HIGH, str(i)))
        got, lock, start = [], threading.Lock(), threading.Barrier(16)
        def go():
            start.wait()
            while True:
                j = q.get()
                if j is None:
                    return
                with lock:
                    got.append(j.payload["id"])
        ts = [threading.Thread(target=go) for _ in range(16)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(len(got), 500)
        self.assertEqual(len(set(got)), 500, "a job was handed to two consumers")


class TestWorkLanes(unittest.TestCase):
    def test_rejects_an_unknown_lane_or_priority(self):
        w = WorkLanes()
        with self.assertRaises(ValueError):
            w.put("gpu", HIGH, {})
        with self.assertRaises(ValueError):
            w.put(FREE, 7, {})

    def test_the_documented_priority_order(self):
        w = WorkLanes(throttled_per_hour=3600)
        w.put(THROTTLED, LOW, {"id": "t-low"})
        w.put(FREE, LOW, {"id": "f-low"})
        w.put(THROTTLED, HIGH, {"id": "t-high"})
        w.put(FREE, HIGH, {"id": "f-high"})
        order = []
        while True:
            j = w.next_ready()
            if j is None:
                break
            order.append(j.payload["id"])
        self.assertEqual(order, ["f-high", "t-high", "f-low", "t-low"])

    def test_free_work_is_never_blocked_by_an_exhausted_bucket(self):
        # the whole point: a throttled backlog with no tokens must not starve cheap work
        w = WorkLanes(throttled_per_hour=1, capacity=1)
        self.assertTrue(w.bucket.try_acquire())       # drain it
        for i in range(3):
            w.put(THROTTLED, HIGH, {"id": f"t{i}"})
        w.put(FREE, LOW, {"id": "cheap"})
        self.assertEqual(w.next_ready().payload["id"], "cheap")

    def test_none_means_not_runnable_yet_not_empty(self):
        w = WorkLanes(throttled_per_hour=1, capacity=1)
        w.bucket.try_acquire()
        w.put(THROTTLED, HIGH, {"id": "x"})
        self.assertIsNone(w.next_ready(), "no tokens, so nothing is runnable")
        self.assertTrue(w.has_pending(), "but there IS work — these are different questions")

    def test_seconds_until_ready(self):
        w = WorkLanes(throttled_per_hour=3600, capacity=1)
        self.assertEqual(w.seconds_until_ready(), float("inf"), "nothing pending at all")
        w.put(FREE, LOW, {})
        self.assertEqual(w.seconds_until_ready(), 0.0)
        w2 = WorkLanes(throttled_per_hour=3600, capacity=1)
        w2.bucket.try_acquire()
        w2.put(THROTTLED, HIGH, {})
        self.assertGreater(w2.seconds_until_ready(), 0.0)

    def test_refill_all_reports_a_failing_source_instead_of_swallowing_it(self):
        w = WorkLanes()
        seen = []
        w.register_refiller("good", lambda m: m.put(FREE, HIGH, {"id": "g"}))
        w.register_refiller("bad", lambda m: (_ for _ in ()).throw(RuntimeError("boom")))
        w.refill_all(on_error=lambda src, exc: seen.append((src, str(exc))))
        self.assertEqual(seen, [("bad", "boom")], "the failure must be reported, not printed")
        self.assertTrue(w.has_pending(), "one bad source must not stop the others")

    def test_refill_all_raises_when_no_error_handler_is_given(self):
        w = WorkLanes()
        w.register_refiller("bad", lambda m: (_ for _ in ()).throw(RuntimeError("boom")))
        with self.assertRaises(RuntimeError):
            w.refill_all()

    def test_snapshot_shape(self):
        w = WorkLanes(throttled_per_hour=60)
        w.put(FREE, HIGH, {})
        s = w.snapshot()
        self.assertEqual(s["free"], {"HIGH": 1, "LOW": 0})
        self.assertIn("rate_per_hour", s["bucket"])
        self.assertEqual(s["tokens_returned"], 0)

    def test_a_lost_race_hands_the_token_BACK(self):
        """The defect this module was rebuilt to fix.

        next_ready() takes a token, then pops. If another consumer won the job in
        between, the pop comes back empty and the token has been spent on nothing.
        The window is real but narrow -- 400 rounds of 16 threads contending for a
        single job never hit it -- so the test HOLDS THE WINDOW OPEN rather than
        pretending a timing race is reproducible on demand.
        """
        # A SLOW refill rate on purpose: this test deliberately sleeps to hold the
        # window open, and at a fast rate the bucket refills measurably during that
        # sleep, so the token arithmetic would be measuring the clock rather than
        # the hand-back. At 1/hour the refill over 0.3s is ~0.00008 tokens.
        w = WorkLanes(throttled_per_hour=1, capacity=10)
        w.put(THROTTLED, HIGH, {"id": "only"})
        before = w.bucket.snapshot()["tokens"]

        real_peek = w.throttled_q.peek_priority
        stalled = {"done": False}
        def peek_then_stall():
            p = real_peek()
            if not stalled["done"]:
                stalled["done"] = True
                time.sleep(0.25)          # hold open a switch point that already exists
            return p
        w.throttled_q.peek_priority = peek_then_stall

        out = {}
        def slow(): out["slow"] = w.next_ready()
        def fast():
            time.sleep(0.05)
            out["fast"] = w.next_ready()
        a, b = threading.Thread(target=slow), threading.Thread(target=fast)
        a.start(); b.start(); a.join(); b.join()

        self.assertIsNotNone(out["fast"], "the winner should get the job")
        self.assertIsNone(out["slow"], "the loser correctly gets nothing")
        self.assertEqual(w.tokens_returned, 1, "the loser's token must be handed back")
        spent = before - w.bucket.snapshot()["tokens"]
        self.assertAlmostEqual(spent, 1.0, places=3,
                               msg="exactly one token for exactly one job")


if __name__ == "__main__":
    unittest.main(verbosity=2)
