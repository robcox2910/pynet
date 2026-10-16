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

## Next Up

Requests, responses and heartbeats over a live connection come next, built
on top of this codec and [Framing](framing.md).
