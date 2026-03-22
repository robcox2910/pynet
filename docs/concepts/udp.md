# UDP

## Shouting Across a Room

TCP is like registered mail -- careful, tracked, guaranteed. But
sometimes you just want to **shout across the room**. It's fast,
but the other person might not hear you, and you won't know unless
they shout back.

**UDP** (User Datagram Protocol) is fire-and-forget:

- **Fast** -- no handshake, no tracking, no waiting for ACKs
- **Unreliable** -- packets might be lost, duplicated, or arrive out of order
- **Connectionless** -- just throw the data and hope it arrives

## When to Use UDP vs TCP

| | TCP | UDP |
|---|---|---|
| **Like** | Registered mail | Shouting |
| **Reliable?** | Yes | No |
| **Fast?** | Slower (overhead) | Very fast |
| **Use for** | Web pages, files, email | Video calls, games, DNS |

Video calls use UDP because a dropped frame is better than a delayed
one. Web pages use TCP because you need every byte of the HTML.

## UDP in PyNet

```python
from pynet.udp import create_datagram

dg = create_datagram("Hello!", dest_port=9000, source_port=5000)
print(dg.data)  # "Hello!"
```

## What We Test

- Messages can be sent and received.
- No connection setup is needed.
- Messages include the sender's address.

## Next Up

HTTP opens and closes a connection for each request. What if you
want to keep talking? Head to [WebSocket](websocket.md).
