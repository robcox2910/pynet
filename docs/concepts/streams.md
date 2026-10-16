# Streams: Water Pipes with a Tap

## The Analogy

A network connection is a **water pipe** between two programs.

- At your end there is a **tap** (the `StreamReader`). You turn it on
  (`await reader.read()`) when you want water. While the tap is off,
  nobody needs to watch the pipe.
- To send water the other way you pour into a **bucket** (the
  `StreamWriter`). The pipe takes what it can straight away; whatever
  does not fit waits in the bucket.
- If the far end is slow to drink, the bucket fills up. The rule is
  **don't overfill the bucket**: `await writer.drain()` makes you wait
  while the bucket is above the **high-water line**, until it has
  emptied down to the **low-water line**. That waiting is called
  **backpressure** -- the slow reader pushes back on the fast writer.

## How It Works

Everything is non-blocking. The loop only watches a file descriptor
while somebody actually needs it:

- **Reading.** `read`, `read_exactly` and `read_until` look in the
  reader's `bytearray` buffer first. If there is not enough, the reader
  asks the loop to watch the fd (`add_reader`), sleeps on a `Future`,
  and removes the watch again when the data arrives -- **or when the read
  is cancelled**, for example by a timeout. So a timed-out read never
  leaves a stale registration behind, and the next read works.
- **Writing.** `write` tries `send` at once. Whatever the kernel would
  not take goes into the bucket, and the loop is asked to say when the
  fd is writable (`add_writer`) until the bucket is empty.

```
 writer.write(data)                         peer's tap
      |                                          ^
      v                                          |
 +-----------+   send() what fits   +--------------------+
 |  bucket   | -------------------> | kernel buffers ... |
 | (buffer)  |                      +--------------------+
 +-----------+
 |~~~~~~~~~~~| <- high water (64 KiB): drain() starts waiting here
 |           |
 |~~~~~~~~~~~| <- low water (16 KiB): ...and wakes up here
 +-----------+
```

Some house rules:

- **One fd, one transport.** A TCP socket's reader and writer share one
  selector registration. `writer.close()` closes the whole socket (we do
  not do half-close / `write_eof`); the reader then sees EOF.
- **Closing flushes first.** `close()` waits for the bucket to empty,
  then closes. `abort()` throws the bucket away and closes now.
- **Peer reset.** If the other side resets the connection, the writer
  records it. The `write` that finds out returns quietly; the next
  `drain()` or `write()` raises `ConnectionClosedError` -- and so does a
  `drain()` that was already waiting, whoever found the reset. The reader sees
  a reset as plain EOF (bytes already buffered are still handed over).
- **EOF.** `read()` returns `b""` at EOF. `read_exactly(n)` and
  `read_until(sep)` raise `IncompleteReadError`, whose `.partial` holds
  the bytes that did arrive. `read_until` raises `LimitOverrunError` if
  the answer would be longer than `limit` (64 KiB by default), leaving
  the buffer untouched.
- **Servers.** `start_server(handler, host, port)` sets `SO_REUSEADDR`
  and runs each connection's `handler(reader, writer)` in its own task.
  If a handler raises, the error goes to the loop's error reporter, that
  connection is closed, and the server keeps accepting. `Server.close()`
  stops accepting; `wait_closed()` waits for the listening socket to
  close (not for running handlers to finish).
- **Clients.** `open_connection(host, port)` uses IP addresses as they
  are, and looks names up with `getaddrinfo` on a helper thread
  (`run_in_thread`) so the loop never blocks. The connect itself is
  non-blocking; a refused connection raises `NetConnectionError`.
- **Pipes.** `pipe_reader(fd)` and `pipe_writer(fd)` give the same
  reader and writer over an OS pipe (using `os.read`/`os.write`). The
  pipe reader closes its fd at EOF; the pipe writer closes it on
  `close()`.

## Try It

```python
from pynet.loop import run
from pynet.streams import open_connection, start_server


async def echo(reader, writer):
    line = await reader.read_until()
    writer.write(line)
    await writer.drain()


async def main():
    async with await start_server(echo, "127.0.0.1", 0) as server:
        reader, writer = await open_connection("127.0.0.1", server.port)
        writer.write(b"hello\n")
        await writer.drain()
        print(await reader.read_until())  # b'hello\n'
        writer.close()
        await writer.wait_closed()


run(main())
```

There is a complete echo server in `examples/echo_server.py`:
`uv run python examples/echo_server.py`, then `nc 127.0.0.1 8888`.

## When to Use

- Talking TCP to another program: a server with `start_server`, a
  client with `open_connection`.
- Reading a child process's output, or feeding its input, through pipes.
- Any time you write a lot: call `await writer.drain()` after `write`
  so a slow peer cannot make your bucket grow forever.

## What We Test

- An echo server round-trips a line; port 0 reports the real port.
- `read`, `read_exactly` and `read_until` (with a custom separator and
  a `limit`), including EOF behaviour and `IncompleteReadError.partial`.
- A peer that never reads makes `drain()` wait (it times out), and a
  peer that does read lets it finish.
- Writing after `close()`, and draining after a peer reset, raise
  `ConnectionClosedError` -- even when the *reader* is the one that
  finds the reset, or `abort()` throws the bucket away mid-`drain()`.
- A handler that raises is reported, its connection is closed, and the
  server accepts the next connection.
- A read cancelled by a timeout removes its fd from the selector, and
  the next read on the same stream works.
- Refused connections, a port already in use, a closed server, host-name
  lookup, and pipes in both directions.

## Next Up

Back to the [Event Loop](event-loop.md) overview, or compare the real
thing with the simulated handshake in [TCP](tcp.md).
