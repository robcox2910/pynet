# Timers

## Kitchen Timers

A cook sets several kitchen timers: eggs in 3 minutes, bread in 10. They
do not watch the clock; they glance at whichever timer rings **soonest**
and do other work until then.

`call_later(delay, cb)` sets a kitchen timer. The loop keeps all timers
in a heap (a pile where the soonest one is always on top), so finding the
next one is instant.

```
 time ─────────────────────────────────────────►
 now      +1s        +2s        +3s
  │ timer B │ timer C  │ timer A  │
  └─ top of the heap: B (earliest deadline)
```

## Details

- Timers fire in deadline order; ties go to whoever was set first.
- `handle.cancel()` turns a timer off. The loop skips it later ("lazy
  deletion") rather than digging it out of the heap.
- `call_at(when, cb)` uses an absolute loop time; `call_later` adds the
  delay to `loop.time()`.

## Testing Time with a FakeClock

Real clocks make slow, flaky tests. `FakeClock` is a pretend clock you
wind forward by hand. A loop using it never really sleeps on a timer, so a
five-second timer is tested instantly:

```python
from pynet.loop import EventLoop
from pynet.loop.testing import FakeClock

clock = FakeClock()
loop = EventLoop(clock=clock)
loop.call_later(5.0, lambda: print("ding"))

clock.advance(4.0)
loop.run_once()   # nothing yet
clock.advance(1.0)
loop.run_once()   # prints "ding" -- no real waiting
```

If you would rather not wind it yourself, `FakeClock(autojump=True)` winds
itself forward whenever everyone is just waiting: the loop jumps straight
to the next timer and fires it on that same turn. A 30-second backoff then
takes microseconds.

"Just waiting" has to be true, though, or the clock would cheat. So:

- Before jumping, the loop looks for I/O for a tiny *real* moment
  (`AUTOJUMP_GRACE`, 0.01 seconds), in case bytes are already on their way.
- While real work is in flight -- a job on a helper thread, or a child
  process that has not finished -- it never jumps. Things that do such
  work tell the loop with `loop.add_busy_check(check)`; while any check
  says "busy", fake time just follows real time.

```python
import time

from pynet.loop import get_running_loop, run, run_in_thread, sleep, wait_for
from pynet.loop.testing import FakeClock


async def main() -> None:
    loop = get_running_loop()
    await sleep(30)  # nobody busy: jumps straight there
    print("after the nap:", loop.time())
    await wait_for(run_in_thread(time.sleep, 0.05), 5)  # a helper is busy: no jump
    print("helper done before the 5 s deadline:", loop.time() < 35)


run(main(), clock=FakeClock(autojump=True))
# after the nap: 30.0
# helper done before the 5 s deadline: True
```

```
  timers waiting, nothing else?       ->  jump to the next timer
  bytes arrive within the grace?      ->  handle them, no jump
  helper thread or child still busy?  ->  wait for it, clock ticks at real speed
```

## When to Use

Timeouts, retries, heartbeats, and anything that should happen "later".

## What We Test

- A timer never fires early.
- Timers fire in deadline order; cancelled ones never fire.
- `next_timeout()` counts down to the earliest timer.
- An autojump clock waits `AUTOJUMP_GRACE` before jumping, and holds
  still while a busy check is true; a short helper-thread job, a short
  child process and a `localhost` lookup all finish inside a 5-second
  `wait_for` instead of timing out.

## Next Up

Now give the waiter bookmarks and promises to wait on: [Tasks and Await](tasks-and-await.md).
