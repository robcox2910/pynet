# Sync Primitives -- Pistol, Conveyor Belt, Key and Car Park

## The Analogy

Tasks share one stage, so they need house rules:

- **Event** is a **starting pistol**. Runners wait on the line; one shot
  releases all of them at once.
- **Queue** is a **conveyor belt**. Producers put items on, consumers
  take them off, oldest first. A belt with a size limit makes producers
  wait when it is full.
- **Lock** is a **bathroom key**. One person holds it; everyone else
  queues in the order they arrived.
- **Semaphore** is a **car park** with a fixed number of spaces. Cars
  beyond the limit wait at the barrier until someone leaves.

## How It Works

Every primitive keeps a line of waiting `Future`s in a `deque`. When a
task must wait it makes a future, joins the line and `await`s it. When
the resource frees up, the releaser fills in the first future in line --
and hands the key (or space, or item) *straight over*, so nobody can
sneak in front.

```
 acquire() -- key taken --> [ waiter1 ][ waiter2 ][ waiter3 ]
 release()  ---------------> waiter1 gets the key (still "locked")
```

### The subtle part: cancellation

- A waiter cancelled **while still in line** is simply removed.
- A waiter cancelled **after it was handed the key** must pass the key
  to the next in line, or it would be lost and everyone behind would wait
  for ever. `Queue.get()` likewise puts a handed-over item back at the
  front of the belt.

## Try It

```python
from pynet.loop import Queue, Semaphore, gather, run, sleep

async def main():
    belt: Queue[int] = Queue(maxsize=2)
    spaces = Semaphore(2)

    async def producer():
        for n in range(5):
            await belt.put(n)          # waits when the belt is full

    async def consumer():
        for _ in range(5):
            async with spaces:         # at most 2 inside at once
                print("got", await belt.get())
                await sleep(0.1)

    await gather(producer(), consumer())

run(main())
```

## When to Use

- `Event` -- "start when I say go", or "tell everyone shutdown began".
- `Queue` -- messaging between tasks; the bounded form gives
  back-pressure so a fast producer cannot flood a slow consumer.
- `Lock` -- protect a shared thing that several awaits must update together.
- `Semaphore` -- limit concurrency. The scheduler will use it for
  resource pools (for example, at most N jobs running at once).

## What We Test

- An event wakes every waiter; `clear` re-arms it; cancelled waiters leave quietly.
- `get` waits for `put`; a full bounded queue makes `put` wait; `get_nowait`
  and `put_nowait` raise `QueueEmpty` / `QueueFull`.
- A cancelled getter after an item was assigned does not lose the item.
- Locks are FIFO; releasing an unlocked lock raises `RuntimeError`.
- A semaphore never lets more than its limit in; over-releasing raises `ValueError`.
- A cancelled waiter never steals a slot, and one cancelled after being
  handed a slot or lock passes it on.

## Next Up

Back to the [Event Loop](event-loop.md) overview, or revisit
[Cancellation and Timeouts](cancellation-and-timeouts.md).
