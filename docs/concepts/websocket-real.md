# WebSocket, For Real

## The Phone Call, For Real

The [WebSocket](websocket.md) page pretended to make a phone call.
This one actually dials. Before anyone can chat, two things must happen:
the caller **dials** (an ordinary HTTP request that asks to switch to
WebSocket) and the other side **picks up** and proves it heard the right
caller (an HTTP `101 Switching Protocols` reply). Only then does the
line stay open for talking.

The real code lives in the `pynet.ws` package (`pynet.websocket` stays
as the simulation).

## The Opening Handshake

```
Client                                         Server
  |  GET /chat HTTP/1.1                           |
  |  Upgrade: websocket                           |
  |  Connection: Upgrade                          |
  |  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==  |
  |  Sec-WebSocket-Version: 13                    |
  | --------------------------------------------> |
  |                                               |
  |  HTTP/1.1 101 Switching Protocols             |
  |  Upgrade: websocket                           |
  |  Connection: Upgrade                          |
  |  Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
  | <-------------------------------------------- |
  |                                               |
  |======== from here on: WebSocket frames ========|
```

## The Accept Key: "Yes, It's Really You"

Why not just say "OK"? Because an old web server (or a cache) might
reply to anything. The caller picks a random 16-byte **key**. The
server must glue on a fixed secret-sounding string from the standard
(the **GUID**, `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`), run the result
through SHA-1, and Base64 the digest. Only something that really
understands WebSocket would do that, so a matching **accept** value is
the proof that the callee picked up.

```
accept = base64( sha1( key + GUID ) )
```

## Try It

```python
from pynet.ws import (
    client_handshake_request,
    compute_accept,
    generate_key,
    parse_http_head,
    server_handshake_response,
    validate_client_handshake,
)

# The RFC 6455 example: this exact key must give this exact accept value.
print(compute_accept("dGhlIHNhbXBsZSBub25jZQ=="))

# Caller dials...
key = generate_key()
request = client_handshake_request("example.com", "/chat", key)

# ...the server checks the call and picks up...
method, target, headers = parse_http_head(request)
client_key = validate_client_handshake(method, headers)
response = server_handshake_response(client_key)

# ...and the caller checks the answer.
version, status, reply_headers = parse_http_head(response)
print(status, reply_headers["sec-websocket-accept"] == compute_accept(key))
```

Output:

```
s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
101 True
```

A bad call -- the wrong method, no `Upgrade`, version 8, a key that is
not 16 bytes -- raises `HandshakeError` naming what was wrong.

`parse_http_head` returns `(method, target, headers)` for a request and
`(version, status_code, headers)` for a response. Header names come
back lower-cased and repeated headers are joined with `", "`.

## Frames: Envelopes for Words

Once the line is open, every message travels in an **envelope** called a
frame: a tiny header, then the words (the payload).

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| payload len |   extended payload length     |
|I|S|S|S|  (4)  |A|    (7)      |   (16 or 64 bits, if needed)  |
|N|V|V|V|       |S|             |                               |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+-------------------------------+
| masking key (4 bytes, only if MASK = 1) | payload ...         |
+-----------------------------------------+---------------------+
```

- **FIN** -- 1 means "this is the last piece of the message".
- **RSV1-3** -- reserved; must be 0 (we reject anything else).
- **opcode** -- `TEXT`, `BINARY`, `CONTINUATION`, or the control frames
  `CLOSE`, `PING`, `PONG`. Control frames must be whole (FIN) and at most
  125 bytes.
- **MASK** -- 1 when a 4-byte key follows. Clients must mask; servers must not.
- **payload len** -- 0-125 is the length itself. 126 means "the real
  length is in the next 2 bytes"; 127 means "the next 8 bytes". The
  shortest form must be used, and a peer that wastes bytes is rejected.

### Masking: Scrambling with a Stencil

The client picks a random 4-byte key and XORs byte `i` of the payload
with `key[i % 4]`. XOR is its own undo, so the server applies the same
stencil to read the words. It is not encryption (the key is right there
in the frame); it only stops a client's bytes looking like HTTP to a
confused proxy. `apply_mask` repeats the key to the payload's length and
XORs it as one big integer, which keeps 1 MiB fast.

```python
from pynet.loop import run
from pynet.streams import pipe_reader
from pynet.ws import Frame, Opcode, encode_ws_frame, read_ws_frame
import os

MASK = bytes([0x37, 0xFA, 0x21, 0x3D])
wire = encode_ws_frame(Frame(True, Opcode.TEXT, b"Hello"), mask_key=MASK)
print(wire.hex())


async def main():
    read_fd, write_fd = os.pipe()
    os.write(write_fd, wire)
    os.close(write_fd)
    return await read_ws_frame(pipe_reader(read_fd), max_payload=1024, expect_masked=True)


print(run(main()))
```

Output:

```
818537fa213d7f9f4d5158
Frame(fin=True, opcode=<Opcode.TEXT: 1>, payload=b'Hello')
```

`read_ws_frame` checks the announced length against `max_payload` before
reading a single payload byte (`FrameTooLargeError`), and a stream that
ends mid-frame raises `ConnectionClosedError`.

`read_ws_frame` is a one-off read. A frame arrives in steps (header,
length, mask key, payload), and if a timeout cancels the read halfway,
the steps already done are lost. For reading frame after frame, keep a
`FrameReader(reader, expect_masked=...)`: each `await frames.read(max_payload)`
writes every finished step down, so a read cut off by a timeout is
picked up by the next one.

## Talking: A Real Conversation

With the line open and envelopes in hand, `pynet.ws.WebSocket` holds
the actual conversation. You send whole messages with `send_text` or
`send_bytes`, and `recv()` hands you the next whole message -- a `str`
for text, `bytes` for binary. Behind the scenes it:

- **Glues fragments together.** A long message may arrive in pieces
  (`TEXT` with FIN=0, then `CONTINUATION` frames until FIN=1). You only
  ever see the finished message. The pieces collected so far live on
  the `WebSocket` itself, so `wait_for(ws.recv(), 1.0)` can time out in
  the middle of a message and the next `recv()` still gets all of it.
- **Answers "are you there?"** A `PING` is answered with a `PONG`
  carrying the same bytes, even between fragments. Pongs we receive are
  skipped.
- **Checks the rules.** Text must be valid UTF-8, a message must fit in
  `max_message_size` (1 MiB by default), and frames from a client must
  be masked. Break a rule and we hang up with the matching code.

Sends go through a lock, so a pong can never be wedged in the middle of
another frame's bytes.

## Hanging Up Politely

You do not just slam the phone down. Whoever wants to finish says
"goodbye" (a `CLOSE` frame with a code and reason), the other side says
"goodbye" back, and only then is the line put down.

```
Client                                 Server
  |  CLOSE 1000 "bye"                    |
  | -----------------------------------> |  recv() raises WebSocketClosed(1000, "bye")
  |                      CLOSE 1000      |
  | <----------------------------------- |
  |  close() returns                     |
  X  TCP connection closed               X
```

`close()` sends the goodbye, waits for the reply, then closes the
stream -- all within 5 seconds (`CLOSE_TIMEOUT`). That one deadline
covers *sending* the goodbye too: if another task is stuck pushing a big
message to a peer that has stopped reading, our `CLOSE` would be stuck
in the queue behind it. When time runs out we drop the line at once
(`abort()`), which also frees the stuck sender, and the conversation
ends with 1006. Pongs we owe the peer get the same deadline: a pong
that cannot get out in time ends the conversation instead of freezing
`recv()`. Calling it twice is harmless. Once a
conversation is over, every `recv()` and send raises the same
`WebSocketClosed` (it is a `ConnectionClosedError`, so old handlers that
catch that still work), and `ws.closed` / `ws.close_code` tell you how
it ended.

### Close Codes

| Code | Name | Meaning |
|------|------|---------|
| 1000 | `CLOSE_NORMAL` | All done, goodbye. |
| 1001 | `CLOSE_GOING_AWAY` | Leaving (page closed, server shutting down). |
| 1002 | `CLOSE_PROTOCOL_ERROR` | You broke the rules (unmasked client frame, stray continuation...). |
| 1003 | `CLOSE_UNSUPPORTED_DATA` | I cannot accept that kind of data. |
| 1005 | -- | The CLOSE had no code. Only ever *reported*, never sent. |
| 1006 | -- | The line dropped with no CLOSE at all. Only ever *reported*, never sent. |
| 1007 | `CLOSE_INVALID_UTF8` | Your text (or close reason) was not valid UTF-8. |
| 1009 | `CLOSE_TOO_BIG` | Your message was bigger than I can swallow. |
| 1011 | `CLOSE_INTERNAL_ERROR` | My handler crashed. |

## Answering and Dialling

- `serve_websocket(handler, host, port)` is a whole switchboard: it reads
  each request head (at most 16 KiB), checks the handshake, answers
  `101`, and runs `handler(ws)`. A bad request gets
  `400 Bad Request` and is hung up on. When the handler returns the
  socket is closed with 1000; if it crashes, with 1011.
- `accept_websocket(reader, writer, method=..., headers=...)` does the
  answering part only, for a web server (like PyWeb) that already read
  the request head itself.
- `connect_websocket(host, port, path="/")` dials, then checks the
  answer is `101` with the right `Sec-WebSocket-Accept`.

```python
from pynet.loop import run
from pynet.ws import WebSocketClosed, connect_websocket, serve_websocket


async def shout(ws):
    """Send every text message back in capitals."""
    try:
        while True:
            await ws.send_text((await ws.recv()).upper())
    except WebSocketClosed as closed:
        print("server saw goodbye:", closed.code, closed.reason)


async def main():
    async with await serve_websocket(shout, "127.0.0.1", 0) as server:
        ws = await connect_websocket("127.0.0.1", server.port)
        await ws.send_text("hello")
        print(await ws.recv())
        await ws.close(1000, "bye")
        print("client closed:", ws.closed, ws.close_code)


run(main())
```

Output:

```
HELLO
server saw goodbye: 1000 bye
client closed: True 1000
```

### From a Browser

Start the example server (`uv run python examples/ws_echo.py`), then
**paste this into a browser page** (for example a file `echo.html`
opened in your browser). It is not run by our tests:

```html
<!-- paste into a browser page -->
<input id="msg" value="hello"> <button id="send">Send</button>
<pre id="log"></pre>
<script>
  const log = (line) => (document.getElementById("log").textContent += line + "\n");
  const ws = new WebSocket("ws://127.0.0.1:8765/");
  ws.onopen = () => log("connected");
  ws.onmessage = (event) => log("echo: " + event.data);
  ws.onclose = (event) => log("closed " + event.code);
  document.getElementById("send").onclick = () => ws.send(document.getElementById("msg").value);
</script>
```

The browser does the handshake and masking for you; our server does
the rest.

## When to Use

- Live updates pushed to a browser (like the job scheduler's dashboard).
- Upgrading an HTTP connection (for example from PyWeb) with `accept_websocket`.
- Dialling a WebSocket server from Python with `connect_websocket`.
- Not for talking between two Python programs you control: plain
  [messaging](messaging.md) over TCP is simpler.

## What We Test

- The RFC 6455 example key gives the RFC's accept value.
- Header names and the `Upgrade`/`Connection` values work in any case.
- Wrong method, missing `Upgrade`, version 8 and a 15-byte key are rejected.
- The 101 response and upgrade request are byte-for-byte what we expect.
- Malformed HTTP heads raise `HandshakeError`.
- `b64_decode_bytes` returns binary data exactly and rejects bad Base64.

### What We Test (Frames)

- The RFC 6455 section 5.7 examples, masked and unmasked.
- 16-bit and 64-bit length forms round-trip; non-minimal forms are rejected.
- Reserved bits, unknown opcodes, bad control frames and mask mismatches.
- Oversize frames fail before the payload is read; truncation is a closed connection.
- `apply_mask` equals the byte-by-byte definition on 1 MiB.

### What We Test (Connections)

- Text and binary messages echo back; the example server echoes both.
- A `TEXT` fin=0 + `CONTINUATION` fin=1 pair is reassembled, even with a ping between.
- A `recv()` that times out after one fragment and half of the next
  frame is finished by the next `recv()`, which returns the whole message.
- Pings are answered with matching pongs; pongs are skipped.
- Closing with 1000 "bye" reaches the server exactly; codes are echoed;
  an empty CLOSE is 1005; a 1-byte CLOSE or reserved code is 1002.
- Bad UTF-8 is 1007, oversize messages (one frame or several) are 1009,
  unmasked client frames and stray/overlapping fragments are 1002.
- A peer that vanishes gives 1006; `close()` gives up after its timeout,
  even when a big send to a peer that never reads is stuck ahead of the
  goodbye; a pong stuck the same way ends `recv()` with 1006.
- After closing, every call raises `WebSocketClosed`.
- Bad handshakes get `400 Bad Request`; the client rejects a non-101
  answer, a wrong accept value, and a server that hangs up.

## Next Up

The phone line is real. Last stop: how does our homemade loop compare with
the one in Python's standard library? See [Our Loop vs asyncio](our-loop-vs-asyncio.md).

In PyStack, the job scheduler's dashboard gets live updates through PyWeb,
which upgrades its HTTP connections with `accept_websocket`.

## See also

- [PyWeb: WebSockets in PyWeb](https://robcox2910.github.io/pyweb/concepts/websockets-in-pyweb/) -- how a web server answers the phone call and upgrades the connection.
