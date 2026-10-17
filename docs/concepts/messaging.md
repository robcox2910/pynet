# Messaging

## Walkie-Talkies with Call Signs

On a walkie-talkie everyone shares one channel, so you say who you are
and what you want: "Base, this is Unit 42, requesting a weather report.
Over." Base answers "Unit 42, this is Base, sunny. Over." The **call
sign** lets you match each answer to the question that caused it, even
when several calls are in the air at once.

Raw frames (see [Framing](framing.md)) are just anonymous chunks of
bytes. Messaging adds the call signs: an **envelope** that says what kind
of message this is, which call it belongs to, and what is being asked.

## The Envelope

Every message starts with one JSON envelope:

```json
{"v":1,"type":"request","id":1,"method":"echo","body":{"x":1}}
```

| Key | Meaning |
|-----|---------|
| `v` | Protocol version (always written; must be `1`) |
| `type` | `request`, `response`, `error`, `push`, `ping` or `pong` |
| `id` | The call sign; required on request, response and error |
| `method` | What is asked for; required on request and push |
| `body` | Any JSON value (always written, may be `null`) |
| `attachments` | How many binary frames follow (written only when above 0) |

`id` and `method` are left out when they are `None`.

## Frame Layout

Each frame payload starts with a one-byte **kind**: `J` for the JSON
envelope, `B` for raw attachment bytes.

```
frame 1: [J][{"v":1,"type":"push","method":"blob","body":null,"attachments":2}]
frame 2: [B][00 ff]
frame 3: [B][second attachment bytes]
```

Binary data never gets turned into text, so there is no Base64 bloat.
The decoder returns a finished `Message` only after the envelope and all
declared attachments have arrived.

## Try It

```python
from pynet.messaging import Message, MessageDecoder, MessageType, encode_message

message = Message(
    MessageType.PUSH, method="blob", body={"name": "x"}, attachments=(b"\x00\xff", b"hi")
)
frames = encode_message(message)
for frame in frames:
    print(frame)

decoder = MessageDecoder()
results = [decoder.feed(frame) for frame in frames]
print(results[:-1])
print(results[-1] == message)
```

## Talking Over a Live Connection: the `Peer`

The codec only packs and unpacks envelopes. A **`Peer`** is one
walkie-talkie: it owns a live connection, keeps track of its own call
signs, and does the talking and listening for you.

```
   client Peer                                    server Peer
   -----------                                    -----------
   request("echo", {"x":1})
     id 1 --->  {"type":"request","id":1,...}  --->  runs the "echo"
                                                     handler in its
   request("slow")                                   own task
     id 2 --->  {"type":"request","id":2,...}  --->  runs "slow" in
                                                     another task
     <---  {"type":"response","id":1,...}  <---  "echo" done first
   the id-1 caller wakes up
     <---  {"type":"response","id":2,...}  <---  "slow" done later
   the id-2 caller wakes up

     <---  {"type":"push","method":"news"}  <---  push(): no id,
   the "news" handler runs                        no answer expected
```

- **Request / response.** `await peer.request(method, body)` picks the
  next call sign (1, 2, 3, ...), sends the request, and waits for the
  answer with the same id. Every incoming request runs its handler in
  its own task, so a slow call never holds up a fast one on the same
  connection. A handler returns a JSON value (the response body) or a
  `Reply(body, attachments)` when it wants to send bytes back too.
- **Push.** `await peer.push(method, body)` is "Unit 42, FYI, over" -- no
  call sign and no answer. Either side can push at any time. A push
  nobody registered a handler for is logged and dropped. Pushes on one
  connection are handled **one at a time, in the order they arrived**,
  like news read out from a single in-tray: the handler for the second
  push does not start until the first one has finished, even if the
  first one stops to `await sleep(...)`. (Requests are different: each
  gets its own task.) So a push handler that waits a long time holds up
  the pushes behind it -- hand slow work to `spawn(...)` if order does
  not matter for it.

```
  pushes in:  [news 1] [news 2] [news 3]
                 |
  push worker:  news 1 ......done -> news 2 ..done -> news 3 ..done
  requests:     each one in its own task, side by side
```
- **Timeouts.** `request(..., timeout=...)` (default 30 seconds) raises
  `TimeoutError` if no answer comes in time. If the answer turns up
  later, it is logged and ignored.
- **One speaker at a time.** Sends are queued behind a `Lock`, so the
  attachment frames of one message never get mixed into another's. A
  message is always sent whole, even if the task that sent it is
  cancelled halfway through. A frame over the size limit raises
  `FrameTooLargeError` *before* anything is written, so nothing half-sent
  is left on the wire.

## When Things Go Wrong

Errors come back as an `error` message with body
`{"code": ..., "message": ...}`, and `request()` raises
`RemoteError` with `.code` and `.remote_message`:

| Code | When |
|------|------|
| `unknown_method` | No handler is registered for the method |
| `handler_error` | The handler raised (the exception is also reported to the loop) |
| `bad_message` | The request broke the protocol but had a usable `id` |
| `unsupported_version` | The request's `v` was not `1` |

A handler can also `raise RemoteError("not_found", "no such job")` on
purpose to send its own code. After any of these the connection keeps
working. Other broken frames -- no usable `id`, or a type other than
`request` (a broken push, ping or pong is never answered) -- are
reported to the loop and skipped; only framing errors (a truncated or
oversized frame) end the connection. An incoming `ping` is answered with
a `pong` straight away.

## Hanging Up

`await peer.close()` hangs up (calling it twice is fine). When a
connection ends -- we close, they close, or the cable is cut -- the peer:

1. stops reading and cancels running handlers,
2. fails every waiting `request()` with `ConnectionClosedError` (at once,
   not when its timeout runs out) -- even one that is still sending,
3. runs every `on_close(callback)` exactly once with the cause: `None`
   if we hung up, otherwise the error that ended the connection.

If the other side has stopped listening (its radio is on but nobody is
holding it), bytes we already queued could wait forever. So hanging up
never waits for them: a message still being sent is cut off at once,
and anything else queued gets `CLOSE_GRACE` (1 second) before the line
is cut anyway.

## Try It

`serve()` gives every new connection its own `Peer` (your `setup`
registers its handlers) and `connect()` dials out:

```python
from pynet.errors import RemoteError
from pynet.loop import Event, run, wait_for
from pynet.messaging import Message, Peer, Reply, connect, serve


async def main() -> None:
    news = Event()

    def setup_server(peer: Peer) -> None:
        async def echo(msg: Message) -> Reply:
            await peer.push("news", "someone called echo")
            return Reply(msg.body, msg.attachments)

        peer.on("echo", echo)

    def setup_client(peer: Peer) -> None:
        async def on_news(msg: Message) -> None:
            print("push:", msg.body)
            news.set()

        peer.on("news", on_news)
        peer.on_close(lambda cause: print("closed, cause:", cause))

    server = await serve("127.0.0.1", 0, setup_server, heartbeat_interval=None)
    client = await connect(
        "127.0.0.1", server.port, setup=setup_client, heartbeat_interval=None
    )
    reply = await client.request("echo", {"x": 1}, attachments=[b"\x00\xff"])
    print("reply:", reply.body, reply.attachments)
    await wait_for(news.wait(), 1.0)
    try:
        await client.request("nope")
    except RemoteError as error:
        print("error:", error.code)
    await client.close()
    server.close()
    await server.wait_closed()


run(main())
```

Output (the push was sent before the response, so it arrives first):

```
push: someone called echo
reply: {'x': 1} (b'\x00\xff',)
error: unknown_method
closed, cause: None
```

For a bigger example, `examples/chat_room.py` is a chat server that
pushes every line to every member:

```
uv run python examples/chat_room.py
```

## When to Use

Any time two programs need to exchange named calls, answers and
notifications over one connection, optionally with binary payloads.

## What We Test

- The envelope for each message type has exactly the documented keys.
- Messages with attachments (including `b"\x00\xff"`) round-trip.
- The decoder waits until every attachment has arrived.
- Every protocol breach raises `ProtocolError`: unknown kind, orphan
  binary frame, bad UTF-8 or JSON, non-object JSON, wrong version
  (`unsupported_version`), unknown type, missing `id` or `method`, bad
  attachment count, NaN/Infinity.
- The decoder is reusable after an error.
- Over real sockets: request/response, replies with attachments, pushes
  both ways, and a fast request overtaking a slow one on one connection.
- Two pushes back to back: the second push's handler starts only after
  the first (which sleeps) has finished.
- `unknown_method`, `handler_error` (and the connection still works
  afterwards), a handler's own `RemoteError`, `bad_message`,
  `unsupported_version`, junk frames, stray responses, ping/pong.
- A request timeout, with the late answer ignored.
- Big concurrent sends never interleave, and a cancelled sender never
  leaves half a message on the wire.
- An oversize attachment raises `FrameTooLargeError` with nothing sent,
  and the next request works.
- Closing while the remote has stopped reading returns promptly and fails
  the still-sending request with `ConnectionClosedError`.
- When the other side drops mid-request, the request fails with
  `ConnectionClosedError` well within a second; `on_close` runs once with
  the cause.

## Next Up

[Heartbeats and Reconnect](heartbeats-and-reconnect.md): the peer pings
every few seconds and hangs up when too many pongs go missing, so a
silently dead connection is noticed quickly.
