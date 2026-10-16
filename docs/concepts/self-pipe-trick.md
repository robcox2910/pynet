# The Self-Pipe Trick

## A Doorbell the Waiter Can Hear

The waiter (our event loop) naps in the kitchen until a timer rings or a
customer waves. But what if someone in *another room* (another thread)
has a new order? The waiter is asleep and cannot see them.

The fix: install a **doorbell**. The waiter always listens for it, even
while napping. Anyone can press it to say "wake up, there is work!"

## How It Works

```
 other thread                       loop thread
 ------------                       -----------
 call_soon_threadsafe(cb)
   1. lock, add cb to ready queue
   2. write 1 byte  ------>  [socketpair]  ------>  select() wakes up
                                                     drain the bytes
                                                     run cb
```

The "doorbell" is a `socket.socketpair()`: two connected sockets. The
loop registers the read end in its selector. Writing one byte to the
other end makes the selector report "readable", so `select` returns.

If the pipe is already full, a bell is already ringing, so the extra
byte is simply skipped (`BlockingIOError` is ignored).

## Watching Doors (File Descriptors)

```python
import socket
from pynet.loop import EventLoop

loop = EventLoop()
a, b = socket.socketpair()
loop.add_reader(a.fileno(), lambda: print("got", a.recv(10)))
b.send(b"hi")
loop.run_once()          # prints: got b'hi'
loop.remove_reader(a.fileno())
```

One fd may have a reader and a writer at once. Adding a second reader
replaces the first. The loop's own doorbell never counts as "work", so
an idle loop sleeps in `select` instead of spinning.

## When to Use

- `call_soon_threadsafe` -- whenever another thread must hand work to the loop.
- `call_soon` -- only from the loop's own thread (it is not thread-safe).
- `add_reader` / `add_writer` -- to react to sockets and pipes.

## What We Test

- Readers and writers fire, can be removed, and can share one fd.
- A sleeping loop wakes when another thread calls `call_soon_threadsafe`.
- An idle loop makes only a few `select` calls (no spinning).
- A callback that reschedules itself cannot starve I/O or timers.

## Next Up

Now the loop can hear the outside world. Next we give it
promises to wait on: see [Tasks and Await](tasks-and-await.md).
