"""cooldown — cooperative backoff shared across unrelated processes, via one file.

When a remote service rate-limits you, the process that got the 429 is rarely the
only one that talks to it. A cron job, a worker and a CLI can all hit the same API
under different parents, with no shared memory and no message bus between them.
Backing off inside one process means the other two keep hammering, and the service
sees no reduction at all.

So the backoff lives in a file. One line, one unix timestamp: the moment the
cooldown ends. Anything about to call the service reads it first.

    import cooldown
    gate = cooldown.Cooldown("/var/tmp/myapi-cooldown")

    if gate.active():
        return                                  # somebody else already got a 429
    out = run_the_thing()
    gate.detect_and_set(out.stderr, out.stdout)  # arms it if the output says 429

Missing, unreadable, or past-due file all mean "no cooldown". There is no daemon,
no lock server, and nothing to start: the filesystem is the entire coordination
mechanism, which is what lets processes that know nothing about each other
cooperate.

WRITES ARE ATOMIC, AND THAT IS NOT AN OPTIMIZATION -- see the note on set().

Stdlib only.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional

DEFAULT_COOLDOWN_SECONDS = 30 * 60

# Substrings that mean "you are being rate-limited". Deliberately a parameter and
# not a constant: every service words this differently, and a list tuned for one
# API is wrong for the next.
DEFAULT_MARKERS: tuple[str, ...] = (
    "http error 429",
    "429 too many requests",
    "too many requests",
    "rate-limit",
    "rate limit",
    "quota exceeded",
    "slow down",
    "retry-after",
)


class Cooldown:
    def __init__(self, path, markers: Optional[Iterable[str]] = None,
                 default_seconds: int = DEFAULT_COOLDOWN_SECONDS):
        self.path = Path(path)
        self.markers = tuple(m.lower() for m in (markers if markers is not None else DEFAULT_MARKERS))
        self.default_seconds = int(default_seconds)

    # -- reading ---------------------------------------------------------
    def _read_until(self) -> int:
        try:
            raw = self.path.read_text().strip()
        except (OSError, ValueError):
            return 0
        if not raw:
            return 0
        try:
            return int(raw)
        except ValueError:
            return 0

    def active(self) -> bool:
        return self._read_until() > int(time.time())

    def remaining(self) -> int:
        return max(0, self._read_until() - int(time.time()))

    def until(self) -> int:
        """The unix timestamp the cooldown ends, or 0 if none is active."""
        u = self._read_until()
        return u if u > int(time.time()) else 0

    # -- writing ---------------------------------------------------------
    def set(self, seconds: Optional[int] = None) -> int:
        """Arm the cooldown for `seconds` from now. Returns the end timestamp.

        The write goes to a temp file in the same directory and is then moved into
        place with os.replace(), which is atomic on POSIX and on Windows. Writing
        in place instead would truncate the file first, and a reader landing in
        that window reads an empty string, parses it as "no timestamp", and
        concludes NO COOLDOWN IS ACTIVE -- so the guard fails OPEN, in the one
        direction a backoff must never fail, at exactly the moment several
        processes are reacting to the same 429.

        That is measured, not theoretical. With a plain in-place write, six readers
        against one writer refreshing every 200ms saw 68 false "go ahead" answers
        in 127,846 reads (0.053%); with the writer refreshing continuously it was
        58.6%. With this atomic write: zero in ~240,000 reads. See
        tests/test_cooldown.py, which fails if that ever regresses.
        """
        secs = self.default_seconds if seconds is None else int(seconds)
        until = int(time.time()) + secs
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix=".cooldown-")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(f"{until}\n")
                fh.flush()
                os.fsync(fh.fileno())      # survive a power loss, not just a crash
            os.replace(tmp, self.path)     # atomic: readers see old or new, never neither
            # fsync the file alone survives a crash but not a power loss: the rename
            # itself is a directory metadata change that may still sit in the OS cache.
            # fsync the parent directory to make the os.replace durable across power loss.
            dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return until

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def detect_and_set(self, *outputs: Optional[str], seconds: Optional[int] = None) -> Optional[str]:
        """Scan command output for a rate-limit marker; arm the cooldown if found.

        Returns the MARKER THAT MATCHED, or None. A reason rather than a boolean,
        because when this fires you will want to know which phrase tripped it --
        an over-broad marker that silently matches ordinary output is the likely
        failure here, and a bare True tells you nothing about that.
        """
        blob = "\n".join(o for o in outputs if o).lower()
        for m in self.markers:
            if m in blob:
                self.set(seconds)
                return m
        return None


__all__ = ["Cooldown", "DEFAULT_MARKERS", "DEFAULT_COOLDOWN_SECONDS"]
