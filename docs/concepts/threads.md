# Worker Threads: Sending a Slow Errand to a Helper

## The Analogy

The event loop is a single **waiter**. Most jobs are quick: take an
order, carry a plate. But some jobs are slow and cannot be broken up --
chopping a mountain of onions (a blocking `getaddrinfo` call, a database
query). If the waiter chops, every table waits.

So the waiter hands the onions to a **kitchen helper**, goes back to the
tables, and when the helper finishes they ring the **doorbell**
(`call_soon_threadsafe`). The waiter then collects the result.

## How It Works

```
 loop thread (waiter)                 worker threads (helpers)
 --------------------                 ------------------------
 await run_in_thread(fn)  --submit-->  [queue] --> helper runs fn()
 ...serves other tasks...                               |
 doorbell rings  <------ call_soon_threadsafe ----------+
 future filled, task resumes with result (or exception)
```

- `WorkerPool(max_workers=4)` owns our own daemon threads and a
  `queue.SimpleQueue`. Threads start lazily, up to the cap.
- `run_in_thread(fn, *args, **kwargs)` submits the job and awaits a `Future`.
- An exception raised by `fn` is carried back and re-raised in the
  awaiting coroutine -- never lost.
- Cancelling the awaiting task cancels the future. The thread still
  finishes (you cannot stop a thread mid-job), but its result is dropped.
- `loop.close()` shuts the pool down: one "go home" sentinel per worker,
  then it joins them, so it never hangs. A result that arrives after the
  loop closed has nobody to go to and is dropped quietly.

## Try It

```python
import time
from pynet.loop import gather, run, run_in_thread

async def main():
    start = time.monotonic()
    await gather(*(run_in_thread(time.sleep, 0.1) for _ in range(4)))
    print(f"4 blocking calls took {time.monotonic() - start:.2f}s")  # ~0.10

run(main())
```

## When to Use

- A library call blocks and has no async version (`socket.getaddrinfo`,
  a synchronous database call).
- CPU-light but slow work that would freeze other tasks.

Do not use it for work the loop can already do without blocking (sockets
with `add_reader`, timers): that is cheaper than a thread.

## What We Test

- The value comes back; args and kwargs are passed through.
- Exceptions are re-raised in the caller (even `StopIteration`, as `RuntimeError`).
- The function runs on a different thread than the loop.
- Four 0.1s sleeps overlap (total well under 0.3s).
- Other tasks keep ticking while a thread blocks.
- Cancelled or too-late results are dropped without noise.
- `shutdown(wait=True)` finishes queued jobs and joins every thread;
  submitting afterwards raises `RuntimeError`.

## Next Up

Back to the [Event Loop](event-loop.md) overview. Streams will use
`run_in_thread` for DNS lookups.
