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
  counter. *Anything* that arrives from the other side -- a `pong`, but
  also a request, a push, a response or their own `ping` -- resets it to
  zero. The rule is: "Have I heard a peep from them lately?" A pong is
  the usual peep, but any frame proves they are still there.
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

Why "any frame" and not "only pongs"? Messages go out one at a time,
so our ping waits in the queue behind any big message already being
sent. Picture a huge upload to a peer that reads it slowly: our pings
cannot get out, so no pongs come back -- yet the peer is perfectly
healthy. If only pongs counted, we would hang up on it. Because the
other side heartbeats too (both ends do by default), its own pings keep
arriving and keep our counter at zero.

```
   us (sending a huge message)               slow reader
   t=0   [big message .......................>   reading slowly]
   t=1   ping 2 stuck in the queue             <-- their ping   unanswered = 0
   t=2   ping 3 stuck in the queue             <-- their ping   unanswered = 0
   ...   still alive: we keep hearing from them
```

A remote that has frozen completely sends nothing at all, so it is
still declared dead on time -- even while our big send is stuck.

One caution remains. The *slow reader's* own pongs to us -- and its
view of us -- depend on frames arriving whole. If a single frame takes
longer than the death time to arrive (a giant frame on a crawling
link), the other side may give up on us. For transfers like that, raise
`heartbeat_interval` or send the data in smaller pieces.

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

## Calling Back: the Reconnecting Client

The heartbeat tells you the line is dead. What then? If you were on the
phone, you would ring back -- but not a hundred times a second. You
would try again in a moment, then wait a bit longer, then longer still,
and if the whole office was cut off at once, you would not all ring at
exactly the same second.

A `ReconnectingClient` does exactly that. It keeps one `Peer` connected
to a server, and whenever the connection is lost -- the server
restarts, the network blips, the heartbeat gives up -- it **redials**:

- **Exponential backoff.** Wait `initial_backoff` (0.5 s) after the
  first failure, then double it each time -- 1 s, 2 s, 4 s ... -- but
  never more than `max_backoff` (30 s). A server that is down for an
  hour gets one knock every 30 seconds, not a flood.
- **Jitter.** Each wait is nudged by a random amount, up to `jitter`
  (0.1 means +/-10%). If a hundred agents lost the same scheduler at
  the same moment, jitter spreads their redials out instead of having
  them all knock at once (a "thundering herd").
- **Start over after success.** Once a call gets through, the next drop
  starts again from 0.5 s.

```
   delay = min(max_backoff, initial_backoff * 2**attempt) * (1 + random(-jitter, +jitter))

   attempt   base delay   with 10% jitter, somewhere in
   -------   ----------   -----------------------------
      0        0.5 s      [0.45 ...... 0.55]
      1        1.0 s      [0.9  ...... 1.1 ]
      2        2.0 s      [1.8  ...... 2.2 ]
      3        4.0 s      [3.6  ...... 4.4 ]
      ...
      6+      30.0 s      [27.0 ...... 33.0]   (capped)

   time --->
   dial  x . x . . x . . . . x . . . . . . . . x . . . . . . . . . . . . . . . . . o
         0  0.5   1.5       3.5               7.5                               15.5
         x = refused     o = connected          (no jitter shown)

   three agents losing the same server, with jitter:
   agent A   x . . x . . . . . x
   agent B   x . . . x . . . . . . x          <- spread out, not all at once
   agent C   x . . x . . . . . . x
```

### Events

`on_event(cb)` tells you what is happening, with a `ConnectionEvent`:

| Event | When |
|-------|------|
| `CONNECTED` | The first successful connection |
| `DISCONNECTED` | A connection that was up has gone |
| `RECONNECTED` | Any later successful connection |

A dial that is refused fires nothing -- there was no connection to
lose. Callbacks run straight away (not in a task); one that raises is
reported to the loop and the others still run.

### While the Line Is Down

`request` and `push` work just like a `Peer`'s, but while there is no
connection they raise `NotConnectedError` **at once**. Nothing is
queued up to be sent later, and nothing is replayed after reconnecting:
only the caller knows whether "start job 7" is safe to send twice. To
wait for the line, call `await client.wait_connected(timeout)` (it
raises `TimeoutError` if the time runs out).

`setup` runs on every new `Peer`, so the handlers it registers are there
again after each reconnect -- the server can call the client's methods
on the new connection just as before.

`stop()` cancels the redial loop (even mid-backoff) and closes the
connection. If one was up, that sends one last `DISCONNECTED`; after
that the client is silent for good (it cannot be started again), and
anyone in `wait_connected` gets `NotConnectedError`.

### Try It: Redialling

The server turns up late, then restarts on the same port. Short
backoffs keep the demo quick:

```python
from pynet.errors import NotConnectedError
from pynet.loop import Event, run, sleep
from pynet.messaging import ConnectionEvent, JsonValue, Message, Peer, ReconnectingClient, serve

HOST = "127.0.0.1"
server_peers: list[Peer] = []


async def echo(msg: Message) -> JsonValue:
    return msg.body


def server_setup(peer: Peer) -> None:
    peer.on("echo", echo)
    server_peers.append(peer)


async def main() -> None:
    # Find a free port and let it go: the server will turn up there later.
    probe = await serve(HOST, 0, server_setup)
    port = probe.port
    probe.close()
    await probe.wait_closed()

    reconnected = Event()

    def on_event(event: ConnectionEvent) -> None:
        print("client:", event)
        if event is ConnectionEvent.RECONNECTED:
            reconnected.set()

    client = ReconnectingClient(HOST, port, initial_backoff=0.05, max_backoff=0.4)
    client.on_event(on_event)
    client.start()

    try:
        await client.request("echo", "too early")
    except NotConnectedError:
        print("request while down: NotConnectedError")

    await sleep(0.2)  # the client redials meanwhile and is refused
    server = await serve(HOST, port, server_setup)
    await client.wait_connected(timeout=5.0)
    print("echo:", (await client.request("echo", "hello")).body)

    print("server: restarting")
    server.close()
    await server.wait_closed()
    for peer in server_peers:
        await peer.close()
    await sleep(0.2)
    server = await serve(HOST, port, server_setup)
    await reconnected.wait()
    print("echo:", (await client.request("echo", "hello again")).body)

    await client.stop()
    server.close()
    await server.wait_closed()


run(main())
```

Output:

```
request while down: NotConnectedError
client: connected
echo: hello
server: restarting
client: disconnected
client: reconnected
echo: hello again
client: disconnected
```

The last `disconnected` comes from `stop()` closing the connection.

## When to Use

Keep heartbeats on for any long-lived connection where you need to know
promptly that the other side is gone -- an agent that may lose power, a
client on a flaky network. Turn sending off (`heartbeat_interval=None`)
for short request/response sessions, or when the other side is known to
be local and its exit will be seen as end of file anyway. Pick the
interval as a trade: shorter notices death sooner but sends more pings.

Use a `ReconnectingClient` for anything that should stay connected on
its own -- like a remote agent that must find its scheduler again after
the scheduler restarts. Use plain `connect` for one-off conversations,
where giving up and reporting the error is the right answer.

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
- A send that holds our pings up for ten intervals does not kill a
  healthy peer: the other side's own pings reset the count.
- A silent server is declared dead after exactly
  `(max_missed_pongs + 1) * heartbeat_interval`: `on_close` sees a
  `HeartbeatTimeoutError`, and a waiting request fails with
  `ConnectionClosedError` caused by it.
- A remote that stopped reading is declared dead even while a huge send
  is stuck behind backpressure.
- A pong carrying a pending request's id never settles that request, and
  a malformed `ping` or `push` with an id is reported, not answered with
  an `error`.

For the reconnecting client:

- `backoff_delay` doubles from 0.5 s and caps at 30 s (no jitter), stays
  within +/-`jitter` of that (and really varies), and does not overflow
  after thousands of attempts.
- With `connect` swapped for a script (so only timers move the clock),
  dials happen at exactly 0, 0.5, 1.5, 3.5 s, and after a good
  connection drops the waits start again from 0.5 s.
- A client started before its server connects once the server appears;
  a server restarted on the same port is redialled, giving events
  `CONNECTED, DISCONNECTED, RECONNECTED`, and `setup`'s handlers work on
  the new connection.
- `request` and `push` raise `NotConnectedError` while down;
  `wait_connected` times out with `TimeoutError`.
- `stop()` cancels the redial loop (a server that appears afterwards is
  never dialled), closes a live connection with one `DISCONNECTED`, is
  safe to call twice, and wakes `wait_connected` waiters.
- A `setup` or event callback that raises is reported to the loop, and
  the client carries on.

## Next Up

Putting it to work: a PyStack shell command that serves, connects and
sends one message round trip. After that, the phone call goes real:
[WebSocket, For Real](websocket-real.md).
