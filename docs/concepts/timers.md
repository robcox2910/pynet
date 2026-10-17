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
wind forward by hand. A loop using it never really sleeps, so a
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

## When to Use

Timeouts, retries, heartbeats, and anything that should happen "later".

## What We Test

- A timer never fires early.
- Timers fire in deadline order; cancelled ones never fire.
- `next_timeout()` counts down to the earliest timer.

## Next Up

Now give the waiter bookmarks and promises to wait on: [Tasks and Await](tasks-and-await.md).
