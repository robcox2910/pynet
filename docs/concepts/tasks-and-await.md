# Tasks and Await

## Bookmarks in Recipes

Imagine cooking three recipes at once with only one pair of hands. You
start the soup, and when it says "simmer for 20 minutes" you **put a
bookmark in the page**, pick up the cake recipe, and mix the batter.
When the soup timer rings, you go back to the soup book, open it at the
bookmark, and carry on from exactly that line.

That is what `async def` and `await` do. A **coroutine** is a recipe
that can be paused. Each `await` is a bookmark. A **Task** is the cook
who keeps one recipe moving: it reads until the next bookmark, waits
for that step to be ready, and then opens the book again right where it
left off. The event loop (the waiter) lets many cooks share one kitchen.

## The Pieces

- **`Future`** -- an empty box with a ticket. Someone promises to fill it
  later with a result, an exception, or "cancelled". You can leave a note
  (`add_done_callback`) saying "tell me when it is filled"; the loop
  delivers the note on its *next* turn, never in the middle of whoever
  filled the box.
- **`Task`** -- a Future that drives a coroutine. The task's own box is
  filled with whatever the coroutine finally returns (or raises).
- **`run(main())`** -- build a fresh loop, run `main` to the end, cancel
  any tasks still going, close the loop, and hand back the result.
- **`spawn(coro)`** -- start another cook in the same kitchen.
- **`sleep(seconds)`** -- "set a kitchen timer and bookmark here".

## The Yield-a-Future Trick, Step by Step

There is no magic in `await`. Underneath, a coroutine is a generator,
and `await future` runs `Future.__await__`:

```python
def __await__(self):
    if not self.done():
        yield self          # (1) hand the box up to the Task
    return self.result()    # (4) open it once it is filled
```

Here is what happens when a task meets `await sleep(1)`:

```
   Task._step()                     coroutine                   loop
   ────────────                     ─────────                   ────
   coro.send(None) ───────────────► runs until `await fut`
                                    __await__: yield fut  (1)
   gets fut ◄──────────────────────
   fut.add_done_callback(wakeup) (2)
   returns -- the cook is free
                                                        ...1 second...
                                                        timer: fut.set_result(None)
                                                        next turn: wakeup(fut) (3)
   Task._step()
   coro.send(None) ───────────────► resumes after the yield
                                    return fut.result()   (4)
                                    ... carries on to the next await
```

1. The coroutine *yields the Future itself* -- that is the bookmark.
2. The task asks the Future to call it back when it is filled.
3. The loop delivers that callback on a later turn, so the task steps
   again.
4. The coroutine wakes up inside `__await__` and returns the result (or
   raises the stored exception) right at the `await` line.

When a coroutine finishes, Python raises `StopIteration(value)`; the
task catches it and puts `value` in its own box. If the coroutine
raises, the task stores the exception instead.

Only our own Futures can be yielded. If you `await` something from
asyncio, the task throws a `TypeError` saying "only pynet Futures can be
awaited" back into your coroutine, so the mistake shows up at once.

## Cancelling

`task.cancel()` is someone shouting "stop that recipe!":

- If the task is waiting on a Future, that Future is cancelled, so the
  awaiting line raises `Cancelled`. `finally` blocks run as normal.
- `Cancelled` is a `BaseException`, so an everyday `except Exception`
  does not swallow it by accident.
- A coroutine may catch `Cancelled` and return anyway; then the task
  finishes normally with that result.
- `cancelling()` counts outstanding cancel requests and `uncancel()`
  takes one back (timeouts use this to tell their own cancels apart).

## Using It

```python
from pynet.loop import run, sleep, spawn


async def cook(dish: str, minutes: float) -> str:
    await sleep(minutes)
    return f"{dish} ready"


async def main() -> list[str]:
    soup = spawn(cook("soup", 0.2))
    cake = spawn(cook("cake", 0.1))
    return [await soup, await cake]  # both cook at once: ~0.2s total


print(run(main()))  # ['soup ready', 'cake ready']
```

Testing something slow? Pass a virtual clock and nothing really waits:

```python
from pynet.loop import run, sleep
from pynet.loop.testing import FakeClock


async def nap() -> None:
    await sleep(1000)  # 1000 virtual seconds, finished in microseconds


run(nap(), clock=FakeClock(autojump=True))
```

## Errors Nobody Saw

If a task fails and nobody ever awaits it or asks for its exception,
the error is not lost: when the task is garbage-collected the loop
reports `"Future exception was never retrieved"` to its exception
handler (by default, the `pynet.loop` logger).

## When to Use

Use tasks whenever your program waits on several things at once --
network replies, timers, child processes -- and you want plain,
top-to-bottom code instead of a tangle of callbacks.

## What We Test

- `run` returns main's result, re-raises its exception, and closes the loop.
- Two `sleep(0.1)` tasks finish together in under 0.2 seconds.
- An autojump `FakeClock` makes `sleep(1000)` instant.
- Done callbacks run on a later turn, never inline.
- `result()` on a pending Future raises `InvalidStateError`.
- Awaiting a foreign awaitable raises a `TypeError` that mentions asyncio.
- `get_running_loop()` raises outside a loop; `current_task()` works inside.
- Cancelling throws `Cancelled` at the `await` and runs `finally`;
  `cancelling()`/`uncancel()` count requests.
- Leftover tasks are cancelled when `run` ends.
- An unretrieved task exception is reported once it is collected.

## Next Up

Learn how to stop a task that takes too long, and how to wait for many
tasks at once: [Cancellation and Timeouts](cancellation-and-timeouts.md).
