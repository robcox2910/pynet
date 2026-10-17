# The Event Loop

## The Waiter

Picture a restaurant with one waiter and many tables. The waiter does
not stand beside one table until the meal is over. Instead they:

1. Do every small job that is ready (bring water, take an order).
2. Check the kitchen timers to see which one rings next.
3. If nothing is ready, **stand still and wait** until a timer rings or
   a customer waves -- they never run in circles.

The **event loop** is that waiter. One thread, many jobs, no wasted
effort.

```
        ┌──────────────────────────────────────────┐
        │                 run_once()               │
        │                                          │
        │  1. how long until something is due?     │
        │         next_timeout()                   │
        │  2. wait that long (select)              │
        │  3. move rung timers onto the ready line │
        │  4. run the jobs that were ready at the  │
        │     start of this turn                   │
        └──────────────────────────────────────────┘
              ▲ ready line (call_soon)   ▲ timers (call_later)
```

## The Rules of the Waiter

- `call_soon(cb)` joins the ready line. Jobs run first-come, first-served.
- Jobs added *during* a turn wait for the next turn, so one busy job
  cannot starve the rest.
- If a job fails, the loop reports the error (to your handler, or to the
  `pynet.loop` logger) and carries on with the next job. If your handler
  itself breaks while reporting, both errors go to the logger instead --
  a broken handler never stops the loop.
- With nothing to do, the waiter waits -- `next_timeout()` returns `None`
  ("wait as long as it takes") instead of busy-looping.

## Using It

```python
from pynet.loop import EventLoop

loop = EventLoop()
loop.call_soon(lambda: print("hello"))
loop.call_later(0.1, loop.stop)
loop.run_forever()
loop.close()
```

## When to Use

Use the loop whenever many things must progress at once (connections,
timeouts, messages) but you want a single thread and no locks.

## What We Test

- Callbacks run in the order they were scheduled.
- Jobs added mid-turn wait for the next turn.
- Errors are reported and never stop the loop -- even when the custom
  exception handler raises (both errors are then logged).
- `next_timeout()` is `None` when idle, `0` when work is ready.
- Scheduling on a closed loop raises `RuntimeError`.

## Next Up

Learn how the waiter keeps time: [Timers](timers.md).
