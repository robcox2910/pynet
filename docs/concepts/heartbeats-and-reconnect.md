# Heartbeats and Reconnect

## "Are You Still There?"

You are on the phone and the other end has gone quiet. Maybe they are
thinking. Maybe they walked off. Maybe the line died. You can't tell
from silence alone, so you ask: "Hello? Are you still there?" If they
say "yes", fine. If you ask three times and hear nothing, you hang up
and assume the call is lost.

A TCP connection has the same problem. When the other program exits
cleanly, we get a goodbye (end of file) straight away. But when its
machine freezes, loses power, or a cable is pulled somewhere in the
middle, **nothing arrives at all** -- no goodbye, no error. The socket
looks open forever. A **heartbeat** is the "are you still there?": a
tiny `ping` sent on a timer, which the other side answers with a `pong`.

## How the Peer Does It

Every [`Peer`](messaging.md) has a heartbeat, on both ends, by default:

| Setting | Default | Meaning |
|---------|---------|---------|
| `heartbeat_interval` | `5.0` seconds | Time between pings (`None` turns sending off) |
| `max_missed_pongs` | `3` | Pings in a row that may go unanswered |

- **Ping at once, then every interval.** The first ping goes out when
  the peer starts; then one every `heartbeat_interval` seconds.
- **Answer pings straight away.** An incoming `ping` is answered with a
  `pong` by the reader itself (not a handler task), echoing its `body`.
  Even a peer with `heartbeat_interval=None` still answers.
- **Count the silence.** Each ping sent adds one to an "unanswered"
  counter; any `pong` resets it to zero. Only pongs count -- other
  messages arriving do not reset it. That keeps the rule simple: "I asked,
  did they answer?"
- **Hang up.** At ping time, if more than `max_missed_pongs` pings in a
  row are unanswered, the peer closes with `HeartbeatTimeoutError` (a
  kind of `ConnectionClosedError`). A silent remote is therefore declared
  dead `(max_missed_pongs + 1) * heartbeat_interval` seconds after the
  first ping -- 20 seconds with the defaults.
- **Pings are not calls.** A ping carries no `id` (its `body` is just a
  counter: 1, 2, 3, ...), so a pong can never be mistaken for the answer
  to a `request`.

```
   client Peer                              silent server
   -----------                              -------------
   t=0   ping 1  ---------------------->    (frozen: reads, never answers)
         unanswered = 1
   t=5   ping 2  ---------------------->
         unanswered = 2
   t=10  ping 3  ---------------------->
         unanswered = 3
   t=15  ping 4  ---------------------->
         unanswered = 4
   t=20  4 > max_missed_pongs (3)
         close(HeartbeatTimeoutError)
           - every waiting request fails with ConnectionClosedError
           - on_close(cause) runs with the HeartbeatTimeoutError
           - the socket is cut at once (no waiting to flush)

   a healthy server answers each ping:
   t=0   ping 1  ---------------------->
         <----------------------  pong 1    unanswered = 0
```

Closing on heartbeat death works exactly like any other broken
connection (see [Hanging Up](messaging.md#hanging-up)): pending requests
fail at once, `on_close` callbacks run once with the cause, and the
socket is aborted rather than politely flushed -- nobody is reading
anyway. A send that is stuck because the remote stopped reading does not
stop the heartbeat either: each ping is sent from its own task, so the
clock keeps ticking.

This is how a scheduler notices that an agent machine is gone: register
`on_close` and check whether the cause is a `HeartbeatTimeoutError`.

## Try It

A hand-made server that reads everything and answers nothing, and a
client with a very fast heartbeat so the demo finishes quickly:

```python
from pynet.errors import ConnectionClosedError, HeartbeatTimeoutError
from pynet.framing import FramedConnection
from pynet.loop import get_running_loop, run, wait_for
from pynet.messaging import Peer, connect
from pynet.streams import StreamReader, StreamWriter, start_server


async def silent(reader: StreamReader, writer: StreamWriter) -> None:
    conn = FramedConnection(reader, writer)
    try:
        while True:
            await conn.recv_frame()  # read every ping, answer none
    except ConnectionClosedError:
        conn.abort()


async def main() -> None:
    server = await start_server(silent, "127.0.0.1", 0)
    began = get_running_loop().time()

    def setup(peer: Peer) -> None:
        def closed(cause: BaseException | None) -> None:
            elapsed = get_running_loop().time() - began
            dead = isinstance(cause, HeartbeatTimeoutError)
            print(f"closed after {elapsed:.1f}s, heartbeat death: {dead}")

        peer.on_close(closed)

    client = await connect(
        "127.0.0.1", server.port, setup=setup, heartbeat_interval=0.1, max_missed_pongs=2
    )
    await wait_for(client.wait_closed(), 5.0)
    server.close()
    await server.wait_closed()


run(main())
```

Output:

```
closed after 0.3s, heartbeat death: True
```

Three pings (at 0, 0.1 and 0.2 seconds) went unanswered; at 0.3 seconds
that is more than `max_missed_pongs=2`, so the client hangs up.

For a demo that shows the pongs and then a server that freezes:

```
uv run python examples/heartbeat_demo.py
```

```
server: ping 1 -> pong
server: ping 2 -> pong
server: ping 3 -> pong
server: *freezes* (socket still open, nobody answering)
client: declared dead after 0.6s: HeartbeatTimeoutError
```

## When to Use

Keep heartbeats on for any long-lived connection where you need to know
promptly that the other side is gone -- an agent that may lose power, a
client on a flaky network. Turn sending off (`heartbeat_interval=None`)
for short request/response sessions, or when the other side is known to
be local and its exit will be seen as end of file anyway. Pick the
interval as a trade: shorter notices death sooner but sends more pings.

## What We Test

Time is driven by `FakeClock(autojump=True)`: the sockets are real, but
whenever the loop is idle the clock jumps straight to the next timer, so
many 5-second intervals pass in milliseconds.

- Pings go out at once and then exactly every `heartbeat_interval`, with
  no `id` and a counting body; both `connect()` and `serve()` peers ping
  by default.
- `heartbeat_interval=None` sends no pings; a zero interval or a negative
  `max_missed_pongs` is refused with `ValueError`.
- A server that answers only every third ping keeps the connection alive
  well past the death time: each pong resets the count.
- A silent server is declared dead after exactly
  `(max_missed_pongs + 1) * heartbeat_interval`: `on_close` sees a
  `HeartbeatTimeoutError`, and a waiting request fails with
  `ConnectionClosedError` caused by it.
- A remote that stopped reading is declared dead even while a huge send
  is stuck behind backpressure.
- A pong carrying a pending request's id never settles that request, and
  a malformed `ping` or `push` with an id is reported, not answered with
  an `error`.

## Next Up

Reconnect: when the line drops, the client redials with growing waits
(exponential backoff with jitter), and tells you when it is connected,
disconnected and reconnected.
