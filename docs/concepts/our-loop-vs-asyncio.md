# Our Loop vs asyncio

## A Go-Kart and a Factory Car

Imagine you build a **go-kart** in the garage. You pick every bolt, you
know what each lever does, and when it rattles you know exactly why.
Next door, a **factory car** rolls off the production line: heated seats,
airbags, a roof, a thousand safety tests. Both have an engine, a
steering wheel and four wheels. Both get you down the road.

PyNet's event loop is the go-kart. Python's built-in `asyncio` is the
factory car. We built the go-kart *so that we would understand the car*.
The ideas are the same; the names are very close; the factory car has
many extras that a learner does not need on day one.

```
   Go-kart (pynet.loop)             Factory car (asyncio)
  ┌─────────────────────┐          ┌─────────────────────────────┐
  │ engine: select()    │          │ engine: select / epoll /    │
  │ wheel:  Task        │          │         kqueue / IOCP       │
  │ brake:  Cancelled   │          │ wheel:  Task, TaskGroup     │
  │ no roof, no radio   │          │ brake:  CancelledError      │
  └─────────────────────┘          │ roof (TLS), radio (many     │
     ~ a few small files           │ protocols), airbags ...     │
                                   └─────────────────────────────┘
```

## Side by Side

Every name in the left column is a real public name you can import from
`pynet.loop`, `pynet.streams` or `pynet.process`.

| Idea | PyNet | asyncio |
|------|-------|---------|
| The waiter | `EventLoop` | the event loop (`asyncio.get_running_loop()`) |
| Start the waiter | `run(main())` | `asyncio.run(main())` |
| A promise | `Future` | `asyncio.Future` |
| A running coroutine | `Task`, made by `spawn(coro)` | `asyncio.Task`, made by `asyncio.create_task(coro)` |
| Stopped on purpose | `Cancelled` | `asyncio.CancelledError` |
| Nap | `sleep(seconds)` | `asyncio.sleep(seconds)` |
| Time limit on a block | `async with timeout(seconds):` | `async with asyncio.timeout(seconds):` |
| Time limit on one await | `wait_for(aw, seconds)` | `asyncio.wait_for(aw, seconds)` |
| Many at once | `gather(*aws, return_exceptions=...)` | `asyncio.gather(*aws, return_exceptions=...)` |
| Event, Lock, Semaphore, Queue | `Event`, `Lock`, `Semaphore`, `Queue` | the same four names in `asyncio` |
| Blocking call in a thread | `run_in_thread(fn, *args)` | `asyncio.to_thread(fn, *args)` |
| Read from a socket | `StreamReader` | `asyncio.StreamReader` |
| Write to a socket | `StreamWriter` | `asyncio.StreamWriter` |
| Connect | `open_connection(host, port)` | `asyncio.open_connection(host, port)` |
| Serve | `start_server(handler, host, port)` | `asyncio.start_server(handler, host, port)` |
| Run a child program | `create_process(args)` | `asyncio.create_subprocess_exec(*args)` |
| Read exactly n bytes | `read_exactly(n)` | `readexactly(n)` |
| Read up to a separator | `read_until(sep)` | `readuntil(sep)` |

A few differences worth noticing:

- Our `Task` is made with `spawn`, not `create_task`.
- Our `gather` and `wait_for` are plain `async def` functions, and
  cancelling a `gather` cancels every child it started.
- `create_process` takes a list (`["echo", "hi"]`), and gives you pipes
  as `StreamReader`/`StreamWriter` objects plus `kill_group` for stopping
  a child and everything it started.
- `run()` installs the child watcher for you (see
  [The Self-Pipe Trick](self-pipe-trick.md) and
  [Subprocesses](subprocesses.md)).

## The Same Program, Twice

This program runs on both loops. Only the imports and two names change.

```python
import asyncio

from pynet.loop import gather, run, sleep, spawn, wait_for


async def worker(name, delay):
    await sleep(delay)
    return f"{name} done"


async def ours():
    task = spawn(worker("kart", 0.01))
    others = await gather(worker("a", 0.02), worker("b", 0.01))
    return [await wait_for(task, 1.0), *others]


async def theirs():
    async def worker(name, delay):
        await asyncio.sleep(delay)
        return f"{name} done"

    task = asyncio.create_task(worker("car", 0.01))
    others = await asyncio.gather(worker("a", 0.02), worker("b", 0.01))
    return [await asyncio.wait_for(task, 1.0), *others]


print(run(ours()))
print(asyncio.run(theirs()))
```

Output:

```
['kart done', 'a done', 'b done']
['car done', 'a done', 'b done']
```

## What asyncio Adds That We Skipped

The factory car has features we left out on purpose, to keep the
go-kart small enough to read in an afternoon:

- **TLS on the event loop.** `asyncio` can wrap a connection in
  encryption (`ssl=` on `open_connection` and `start_server`). Our
  streams are plain TCP; the [TLS](tls.md) page is a separate
  simulation of the handshake.
- **Windows.** asyncio has a "proactor" loop built on Windows I/O
  completion ports. PyNet is macOS and Linux only (it uses `select`,
  POSIX signals and process groups).
- **Faster engines.** asyncio can swap in other loops, such as the
  C-based `uvloop`, and its own loop has C-accelerated parts for `Future`
  and `Task`. Ours is plain Python, written to be read, not raced.
- **Structured concurrency.** `asyncio.TaskGroup` (and `asyncio.timeout`
  nested inside it) starts a group of tasks and waits for all of them,
  cancelling the rest if one fails. We have `gather` and `timeout`, but
  no `TaskGroup`.
- **More ways to wait.** `asyncio.wait`, `as_completed`, `shield` and
  friends. We kept `gather`, `wait_for` and `timeout`.
- **More transports and protocols.** UDP datagram endpoints, Unix domain
  sockets, the low-level `Protocol`/`Transport` callback API, and
  `loop.sock_*` helpers. Our streams sit directly on non-blocking sockets
  and pipes.
- **More sync tools.** `Condition`, `Barrier`, `PriorityQueue`,
  `LifoQueue` and more.
- **Debug mode and tooling.** Slow-callback warnings, task introspection,
  `contextvars` support in every task and a large test suite built over
  many years.

Building the go-kart shows that none of these is magic: each one is
"a bit more of the same ideas".

## When to Use

- Use **PyNet's loop** to learn, to read, and to experiment. Put a
  `print` inside the reactor and watch it work.
- Use **asyncio** for real programs: it is faster, safer, runs on every
  platform and works with the whole ecosystem.

## What We Test

Each name in the table above is exercised by the loop, streams and
process test suites. The "Same Program, Twice" snippet was run on both
loops and printed the output shown.

## Next Up

That is the end of the real-networking path. Go back to the
[Event Loop](event-loop.md) and watch the waiter again with new eyes, or
open the examples in `examples/` and run them.
