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

## When to Use

- Upgrading an HTTP connection (for example from PyWeb) to WebSocket.
- Dialling a WebSocket server from a client.
- Never by itself: it only does the opening. Frames come next.

## What We Test

- The RFC 6455 example key gives the RFC's accept value.
- Header names and the `Upgrade`/`Connection` values work in any case.
- Wrong method, missing `Upgrade`, version 8 and a 15-byte key are rejected.
- The 101 response and upgrade request are byte-for-byte what we expect.
- Malformed HTTP heads raise `HandshakeError`.
- `b64_decode_bytes` returns binary data exactly and rejects bad Base64.

## Next Up

The line is open, but nobody has spoken yet. Coming soon: real
WebSocket frames, then connections and endpoints that use them.
