# throttled-worklane

Two small, stdlib-only pieces for keeping a pipeline moving against a rate limit.

- **`worklane.py`** — two-lane priority scheduling, so cheap work never starves behind an
  expensive one waiting on a token bucket. *In-process.*
- **`cooldown.py`** — cooperative backoff shared between processes that know nothing about
  each other, through one file. *Cross-process.*

They are in one repo because they are the same problem at two scales: **what runs next when
something is rate-limited.**

---

## The starvation the lanes fix

A pipeline has stages with very different costs. Some are cheap and parallel — fetch a file,
move bytes. One is expensive and rate-limited — a model call, a paid API, a single GPU.

Put them in one queue and the cheap work starves. The worker picks up an expensive job,
the limiter says *not yet*, and the worker **sits idle holding a job it is not allowed to
run**, while work it could have finished waits behind it.

So: two lanes, and the limiter gates only the lane that needs gating.

```python
from worklane import WorkLanes, FREE, THROTTLED, HIGH, LOW

lanes = WorkLanes(throttled_per_hour=10)
lanes.put(FREE,      HIGH, {"url": "..."})      # never gated
lanes.put(THROTTLED, HIGH, {"prompt": "..."})   # gated by the bucket

job = lanes.next_ready()
if job is None:
    time.sleep(lanes.seconds_until_ready())
```

Order is HIGH free → HIGH throttled → LOW free → LOW throttled, so fresh cheap work overtakes
a backlog and a throttled job never blocks a free one.

**`next_ready()` returning `None` does not mean empty.** It means nothing is runnable *yet* —
usually a throttled backlog with no tokens. `has_pending()` answers the other question, and
the two are deliberately separate calls, because conflating them is how a worker decides it
is finished while holding a full queue.

## The two defects this was rebuilt to fix

Both were found by extracting the code, and both are reproduced in the test suite.

### 1. A lost race spent a rate-limit token on nothing

`next_ready()` takes a token, then pops the job. If another consumer won that job in between,
the pop comes back empty — and the token is already gone. The caller is told there is no
work, while the work it was refused a token for is being done by somebody else, and the
hourly allowance quietly shrinks. **At 10 calls/hour a wrongly-spent token is six minutes of
a lane standing still.**

It is a narrow window. **It did not reproduce in 400 rounds of 16 threads contending for a
single job** — the `and` short-circuits away most attempts. So the test does not pretend a
timing race is reproducible on demand; it **holds the window open** and proves the path is
reachable, then proves the token comes back. The fix is a `release()` on the bucket and a
`tokens_returned` counter so a lost race is visible rather than silent.

### 2. The cross-process cooldown failed OPEN

`cooldown.py` coordinates backoff through a file: one line, one unix timestamp, the moment
the cooldown ends. No daemon, no lock server, nothing to start — the filesystem is the whole
mechanism, which is exactly what lets a cron job, a worker and a CLI under three different
parents cooperate.

The original wrote that file in place. **An in-place write truncates first**, so a reader
landing in that window reads an empty string, parses it as no timestamp, and concludes **no
cooldown is active** — the guard fails open, in the one direction a backoff must never fail,
at precisely the moment several processes are all reacting to the same 429.

Measured with six readers against one writer:

| write strategy | refresh cadence | reads | false "go ahead" |
|---|---|---|---|
| in-place `write_text` | every 200ms | 127,846 | **68** (0.053%) |
| in-place `write_text` | continuous | 111,814 | **65,491** (58.6%) |
| atomic `os.replace` | every 200ms | 122,482 | **0** |
| atomic `os.replace` | continuous | 119,822 | **0** |

The fix is the ordinary one — write a temp file in the same directory, `fsync`, then
`os.replace()` over the target, which is atomic on POSIX and Windows alike. Readers see the
old value or the new one, never neither.

**The test suite contains the control.** `test_the_control_proves_this_test_can_detect_the_defect`
runs the same harness against a deliberately non-atomic writer and **requires it to fail**.
Without that, the passing atomicity test would prove nothing — it could be green simply
because the harness never lands in the window. (Verified the other way too: make the control
atomic and that test fails, as it must.)

## Using the cooldown

```python
import cooldown

gate = cooldown.Cooldown("/var/tmp/myapi-cooldown")

if gate.active():
    return                                        # somebody else already got a 429

result = run_the_thing()
hit = gate.detect_and_set(result.stderr, result.stdout)
if hit:
    log.warning("backing off, matched %r", hit)
```

`detect_and_set` returns **the marker that matched**, not a boolean — when it fires you want
to know which phrase tripped it, because an over-broad marker silently matching ordinary
output is the likely failure here. The marker list is a constructor argument, not a constant,
since every service words rate-limiting differently. The defaults are asserted in the tests
*not* to fire on ordinary output like `200 OK` or `blocked_user_count=0`.

## Design notes worth stealing

- **The token bucket refills on read, not on a timer.** No background thread, and no drift
  while the process is idle.
- **It refills off a monotonic clock**, so an NTP correction or a DST step cannot mint tokens.
- **Burst capacity defaults to a sixth of the hourly rate**, so a quiet night cannot bank a
  whole hour and then spend it in one second.
- **`refill_all()` reports a failing producer** through an `on_error` callback, or raises. It
  does not print and continue: a swallowed exception there looks exactly like *there was no
  work*, which is the most confusing way for a pipeline to stall.

## Tests

```bash
python3 -m pytest tests/ -q        # 45 tests
python3 -m unittest discover -s tests -t . -v
```

No dependencies beyond the standard library — pytest is a convenience, not a requirement.
The concurrency tests take about two seconds; that is the harness doing enough reads for the
measurement to mean anything.

## License

MIT.
