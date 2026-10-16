# Cancellation and Timeouts

## The Referee's Whistle

In a football match the referee carries a stopwatch. When time is up,
they blow the whistle and play stops -- wherever the ball happens to be.
Nobody has to keep checking the clock themselves; the players just play,
and the whistle interrupts them.

A **timeout** is that referee for one block of code. You say "this block
gets 5 seconds". If the block is still running when the stopwatch hits
zero, the referee **cancels the task**, which makes `Cancelled` pop out
of whatever `await` the block is paused on. As that `Cancelled` leaves
the block, the referee swaps it for a plain `TimeoutError`, so you can
tell "ran out of time" apart from "someone told me to stop".

## The Pieces

- **`timeout(seconds)`** -- an async context manager:
  `async with timeout(5): ...`. `timeout(None)` means "no limit".
  `expired()` tells you afterwards whether the whistle blew.
- **`wait_for(aw, seconds)`** -- a timeout around one `await`. If `aw` is
  a task, it is cancelled when time runs out.
- **`gather(*aws)`** -- a team captain. It starts several awaitables
  together, waits for all of them, and returns their results **in the
  order you listed them** (not the order they finished). Coroutines are
  wrapped in tasks; tasks and futures are used as they are.

## How the Whistle Works

```
  async with timeout(1):          loop                     Timeout
  ──────────────────────          ────                     ───────
  __aenter__ ──────────────────────────────────────────►  call_later(1, on_timeout)
  await sleep(100)  (paused)
                                  ...1 second...
                                  on_timeout() ─────────►  expired = True
                                                           task.cancel()   (count 0 -> 1)
  Cancelled pops out of await
  finally blocks run
  __aexit__(Cancelled) ────────────────────────────────►  task.uncancel()  (count 1 -> 0)
                                                           0 left -> raise TimeoutError
```

The trick is the task's **cancel counter**. Every `task.cancel()` adds
one; `task.uncancel()` takes one back. When the timeout leaves its block
it takes back its own cancel. If the count is back where it was when the
block started, the `Cancelled` was the referee's, so it becomes a
`TimeoutError`. If anything is left over, somebody *else* also cancelled
the task -- that is not the referee's business, so `Cancelled` keeps
flying. This is why nested timeouts work: only the one whose whistle
blew turns its own `Cancelled` into a `TimeoutError`.

If the block finishes before the whistle, the referee's timer is simply
switched off. A whistle that somehow rings after the block has ended
does nothing at all.

## Gather: One Fouls Out, Everyone Goes Off

```
  gather(a, b, c)
     │
     ├── a ──────────────── 1 ──┐
     ├── b ──── raises! ─────┐  │      return_exceptions=False:
     └── c ─────────── ...   │  │        cancel a and c, wait for them,
                             ▼  ▼        then raise b's error
                         gather wakes
```

- With `return_exceptions=False` (the default), the **first** error to
  happen is raised, after the other children are cancelled and have
  finished their `finally` blocks.
- With `return_exceptions=True`, errors are put in the result list
  instead of being raised; a child cancelled by someone else shows up as
  a `Cancelled` object.
- Cancelling the task that is waiting in `gather` cancels every child.
- `gather()` with nothing to gather returns `[]` straight away.
- Errors are never silently lost: if another child fails while gather is
  already raising, that extra error goes to the loop's exception handler.

## Using It

```python
from pynet.loop import gather, run, sleep, timeout, wait_for


async def fetch(name: str, seconds: float) -> str:
    await sleep(seconds)
    return f"{name} done"


async def main() -> None:
    # Results come back in the order we asked, even though "slow" finishes last.
    print(await gather(fetch("slow", 0.2), fetch("fast", 0.1)))

    try:
        async with timeout(0.1):
            await fetch("too slow", 5)
    except TimeoutError:
        print("whistle!")

    print(await wait_for(fetch("quick", 0.01), 1))


run(main())
# ['slow done', 'fast done']
# whistle!
# quick done
```

In tests, pass `clock=FakeClock(autojump=True)` to `run`, and even a
100-second timeout finishes instantly and always behaves the same way.

## When to Use

- Any time you wait on something outside your control -- a server
  reply, a DNS lookup, a child process -- wrap it in `timeout` or
  `wait_for`, so a silent peer cannot hang your program forever.
- Use `gather` when you need several answers and can ask for them all at
  once, such as fetching three web pages together.

## What We Test

- A long `sleep` inside a short `timeout` raises `TimeoutError` exactly
  at the limit; a quick block leaves normally; `timeout(None)` never fires.
- `finally` blocks inside a timed-out block still run.
- A cancel from outside stays `Cancelled`, not `TimeoutError`.
- Nested timeouts: the inner one firing raises only at the inner block,
  and the outer one firing raises only at the outer block.
- A timer that rings after the block finished does nothing.
- `wait_for` returns the value in time, times out when late, and cancels
  a late task.
- `gather` keeps argument order, runs children at the same time, returns
  `[]` for nothing, accepts tasks and futures, cancels siblings on the
  first error, collects errors with `return_exceptions=True`, cancels all
  children when it is cancelled, and reports secondary errors.

## Next Up

[Sync Primitives](sync-primitives.md) -- Event, Queue, Lock and
Semaphore. Or back to [Tasks and Await](tasks-and-await.md), or the
[Event Loop](event-loop.md) overview.
