import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path

from cooldown import Cooldown, DEFAULT_MARKERS, DEFAULT_COOLDOWN_SECONDS


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="cooldown-test-")
        self.gate = Cooldown(Path(self.dir) / "until")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class TestReading(Base):
    def test_no_file_means_no_cooldown(self):
        self.assertFalse(self.gate.active())
        self.assertEqual(self.gate.remaining(), 0)
        self.assertEqual(self.gate.until(), 0)

    def test_set_then_active(self):
        until = self.gate.set(60)
        self.assertTrue(self.gate.active())
        self.assertGreater(self.gate.remaining(), 0)
        self.assertEqual(self.gate.until(), until)

    def test_a_past_timestamp_is_not_a_cooldown(self):
        self.gate.path.write_text(f"{int(time.time()) - 10}\n")
        self.assertFalse(self.gate.active())
        self.assertEqual(self.gate.until(), 0)

    def test_garbage_and_empty_files_do_not_raise(self):
        for junk in ["", "   ", "not-a-number", "12.5", "\x00\x00"]:
            self.gate.path.write_text(junk)
            self.assertFalse(self.gate.active(), f"junk {junk!r} must read as no cooldown")

    def test_clear_removes_it_and_is_safe_twice(self):
        self.gate.set(60)
        self.gate.clear()
        self.assertFalse(self.gate.active())
        self.gate.clear()

    def test_default_duration(self):
        self.gate.set()
        self.assertGreater(self.gate.remaining(), DEFAULT_COOLDOWN_SECONDS - 5)

    def test_creates_a_missing_parent_directory(self):
        deep = Cooldown(Path(self.dir) / "a" / "b" / "until")
        deep.set(30)
        self.assertTrue(deep.active())


class TestDetection(Base):
    def test_returns_the_marker_that_matched_not_a_boolean(self):
        hit = self.gate.detect_and_set("ERROR: HTTP Error 429: Too Many Requests")
        self.assertEqual(hit, "http error 429")
        self.assertTrue(self.gate.active())

    def test_no_marker_means_no_cooldown_armed(self):
        self.assertIsNone(self.gate.detect_and_set("downloaded 3 files, all good"))
        self.assertFalse(self.gate.active())

    def test_scans_every_stream_it_is_given(self):
        self.assertIsNotNone(self.gate.detect_and_set("", "quota exceeded for today"))

    def test_is_case_insensitive(self):
        self.assertIsNotNone(self.gate.detect_and_set("TOO MANY REQUESTS"))

    def test_none_streams_are_ignored(self):
        self.assertIsNone(self.gate.detect_and_set(None, None))

    def test_markers_are_configurable_because_every_api_words_it_differently(self):
        g = Cooldown(Path(self.dir) / "custom", markers=["please back off"])
        self.assertIsNone(g.detect_and_set("http error 429"), "default markers must not apply")
        self.assertEqual(g.detect_and_set("PLEASE BACK OFF"), "please back off")

    def test_the_default_markers_do_not_fire_on_ordinary_output(self):
        # an over-broad marker that matches normal logs is the likely failure mode here
        for benign in ["download complete", "200 OK", "processed 12 items",
                       "connection established", "blocked_user_count=0"]:
            self.assertIsNone(self.gate.detect_and_set(benign),
                              f"default markers falsely matched {benign!r}")


class TestAtomicity(Base):
    """The defect this module was rebuilt to fix.

    A plain in-place write truncates the file before writing it. A reader landing
    in that window reads an empty string and concludes NO COOLDOWN IS ACTIVE --
    the guard fails OPEN, in the one direction a backoff must never fail, exactly
    when several processes are reacting to the same rate-limit response.
    """

    def _hammer(self, write_fn, seconds=1.0, readers=6):
        """Refresh the cooldown continuously while readers ask 'may I proceed?'.
        Returns (reads, wrong_answers)."""
        write_fn(3600)
        stop = [False]
        reads, wrong = [0], [0]
        def writer():
            while not stop[0]:
                write_fn(3600)
        def reader():
            while not stop[0]:
                reads[0] += 1
                if not self.gate.active():
                    wrong[0] += 1
        ts = [threading.Thread(target=writer)] + \
             [threading.Thread(target=reader) for _ in range(readers)]
        [t.start() for t in ts]
        time.sleep(seconds)
        stop[0] = True
        [t.join() for t in ts]
        return reads[0], wrong[0]

    def _nonatomic_write(self, seconds):
        """What the module used to do — kept HERE, as the control."""
        until = int(time.time()) + int(seconds)
        self.gate.path.parent.mkdir(parents=True, exist_ok=True)
        self.gate.path.write_text(f"{until}\n")
        return until

    def test_the_control_proves_this_test_can_detect_the_defect(self):
        # Without this, a passing atomicity test below would prove nothing: it could
        # be green because the harness never lands in the window at all.
        reads, wrong = self._hammer(self._nonatomic_write)
        self.assertGreater(reads, 1000, "harness did not perform enough reads to be meaningful")
        self.assertGreater(wrong, 0,
            "a non-atomic write MUST produce false 'no cooldown' answers; if it does "
            "not, this harness cannot detect the defect and the test below is void")

    def test_atomic_write_never_reports_a_false_all_clear(self):
        reads, wrong = self._hammer(self.gate.set)
        self.assertGreater(reads, 1000)
        self.assertEqual(wrong, 0,
            f"the guard failed OPEN {wrong} times in {reads} reads")

    def test_no_temp_files_are_left_behind(self):
        for _ in range(50):
            self.gate.set(60)
        leftovers = [p.name for p in Path(self.dir).iterdir() if p.name.startswith(".cooldown-")]
        self.assertEqual(leftovers, [], f"temp files leaked: {leftovers}")

    def test_the_file_holds_exactly_one_parsable_timestamp(self):
        until = self.gate.set(60)
        self.assertEqual(self.gate.path.read_text().strip(), str(until))


if __name__ == "__main__":
    unittest.main(verbosity=2)
