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

## When to Use

- Upgrading an HTTP connection (for example from PyWeb) to WebSocket.
- Dialling a WebSocket server from a client.
- Never by itself: the handshake only opens the line; frames carry the words.

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

## Next Up

Frames are the envelopes. Coming soon: connections that send and
receive whole messages (fragments, pings, closing) and endpoints that use them.
