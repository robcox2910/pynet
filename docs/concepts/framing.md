# Framing

## Envelopes With the Page Count on the Front

TCP delivers a river of bytes with no gaps. If you mail two letters
back to back, they can arrive glued together, or one letter can arrive
in pieces. How does the reader know where one message ends?

Put the **page count on the front of the envelope**. Before every
message we write its length in 4 bytes. The reader reads those 4 bytes
first, then reads exactly that many bytes, and knows precisely where
the message stops. This is **framing**, and it is the base of every
message protocol built on top of PyNet.

## The Bytes

```
 send_frame(b"hi")           send_frame(b"")

 +-------------+-------+     +-------------+
 | 00 00 00 02 | h  i  |     | 00 00 00 00 |
 +-------------+-------+     +-------------+
   4-byte size   payload       size 0, no payload
   (big-endian)

 On the wire, back to back:
 | 00 00 00 02 | h i | 00 00 00 00 | 00 00 00 03 | a b c |
 \_____ frame 1 _____/ \__ frame 2 _/ \_____ frame 3 _____/
```

## Using It

```python
from pynet.framing import FramedConnection
from pynet.loop import Event, run
from pynet.streams import open_connection, start_server


async def main() -> None:
    done = Event()

    async def handler(reader, writer):
        conn = FramedConnection(reader, writer)
        print("got", await conn.recv_frame())
        conn.close()
        done.set()

    server = await start_server(handler, "127.0.0.1", 0)
    reader, writer = await open_connection("127.0.0.1", server.port)
    conn = FramedConnection(reader, writer)
    await conn.send_frame(b"hello")
    await done.wait()
    conn.close()
    await conn.wait_closed()
    server.close()
    await server.wait_closed()


run(main())
```

## Rules

- **Too big?** A header claiming more than `max_frame_size` (16 MiB by
  default) is refused before the body is read, the connection is
  aborted, and `FrameTooLargeError` (with `.size` and `.limit`) is
  raised. Otherwise a liar could make us buffer gigabytes.
- **Goodbye vs. cut off.** The peer closing between frames raises
  `ConnectionClosedError("peer closed")`. Closing halfway through a
  frame raises `ConnectionClosedError` mentioning `mid-frame`. Calling
  `recv_frame()` again keeps raising it.
- **One sender at a time.** `send_frame` is not safe for several tasks
  at once; share a connection by guarding sends with a `Lock`.

## When to Use

Any time you send discrete messages over TCP: chat, RPC, the PyNet
message protocol. (Line-based text protocols can use `read_until`
instead.)

## What We Test

- `encode_frame(b"hi")` is `00 00 00 02` then `hi`.
- Many frames in one write come out as separate frames.
- A frame dribbled across several writes is reassembled.
- Empty frames work.
- Oversize payloads are refused when encoding and sending.
- An oversize header closes the connection and never reads the body.
- Clean EOF and EOF mid-frame (or mid-header) are told apart.

## Next Up

Messages built on frames come next. Meanwhile, revisit
[Streams](streams.md) to see how `read_exactly` works.
