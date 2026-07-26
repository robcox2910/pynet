# WebSocket

## A Phone Call (Not Letters)

HTTP is like sending letters -- you write one, mail it, wait for a
reply, then write another. Each letter is independent.

A **WebSocket** is like a phone call -- you dial once, and then both
sides can talk whenever they want, as many times as they want, until
someone hangs up.

## When to Use WebSocket

- **Chat apps** -- new messages appear instantly
- **Live sports scores** -- updates push to you automatically
- **Multiplayer games** -- constant back-and-forth
- **Stock tickers** -- prices update in real time

## The Upgrade

WebSocket starts as an HTTP request, then "upgrades" to a persistent
connection:

```
Client: "Hey, can we switch to WebSocket?"
  (HTTP GET with Upgrade: websocket header)

Server: "Sure, switching now."
  (HTTP 101 Switching Protocols)

Now both sides can send messages anytime.
```

## WebSocket Frames in PyNet

```python
from pynet.websocket import WebSocketFrame, make_handshake

# The upgrade request
handshake = make_handshake("example.com", "/chat")

# A text message frame
frame = WebSocketFrame.text("Hello!")
print(frame.opcode)   # 1 (text)
print(frame.payload)  # "Hello!"
```

## What We Test

- Handshake request is valid HTTP with upgrade headers.
- Text frames carry the right opcode and payload.
- Close frames use the close opcode and an empty payload.

In PyNet a `WebSocketFrame` is a simple data holder (opcode + payload);
it does not serialise itself to the real over-the-wire binary format.
Encoding a frame into actual bytes is a great exercise to try yourself.

## Next Up

We've covered real-time connections. What about email?
Head to [SMTP](smtp.md).
