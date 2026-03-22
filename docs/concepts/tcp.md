# TCP

## Registered Mail

When you send registered mail, the post office:
1. Numbers each package
2. Gets a signature confirming delivery
3. Re-sends anything that gets lost

**TCP** (Transmission Control Protocol) does the same for data:

- **Reliable** -- every packet is acknowledged; lost ones are re-sent
- **Ordered** -- packets arrive in the right order (even if the network scrambles them)
- **Connection-based** -- you "dial" first (three-way handshake), then talk

```
Client                Server
  │── SYN ──────────────►│   "Can we talk?"
  │◄── SYN-ACK ──────────│   "Yes, ready!"
  │── ACK ──────────────►│   "Great, let's go!"
  │                       │
  │── Data ─────────────►│   "Here's my message"
  │◄── ACK ──────────────│   "Got it!"
```

## TCP in PyNet

```python
from pynet.tcp import TCPConnection, TCPState

conn = TCPConnection()
packets = conn.handshake()
# packets = [SYN, SYN-ACK, ACK]
print(conn.state)  # TCPState.ESTABLISHED
```

## What We Test

- Packets are numbered sequentially.
- ACKs confirm receipt.
- The three-way handshake establishes a connection.
- Data is delivered in order.

## Next Up

TCP is reliable but slow. What if speed matters more?
Head to [UDP](udp.md).
