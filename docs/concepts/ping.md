# Ping (ICMP)

## Knocking on the Door

Before you start a long conversation, you might knock on the door
first: "Are you there?" If someone answers, great -- you know they're
home. If not, something's wrong.

**Ping** does exactly this for computers. It sends a tiny "are you
there?" message and waits for a reply. If the reply comes back, the
server is alive. The time it takes is the **latency**.

```
You:    "Ping! Are you there?" ──────►  Server
You:    ◄────── "Pong! I'm here!"      Server
        Round-trip time: 12ms
```

## ICMP: The Protocol Behind Ping

Ping uses **ICMP** (Internet Control Message Protocol). An ICMP
echo request contains:

- **Type**: 8 (echo request) or 0 (echo reply)
- **Code**: 0
- **Checksum**: Error detection
- **Identifier**: Which ping session this belongs to
- **Sequence**: Which ping in the series (1st, 2nd, 3rd...)

## Ping in PyNet

```python
from pynet.ping import PingMessage

# Create a ping request
request = PingMessage.echo_request(sequence=1)
print(request)  # "ICMP Echo Request seq=1"

# Create a ping reply
reply = PingMessage.echo_reply(sequence=1, rtt_ms=12.5)
print(reply)  # "ICMP Echo Reply seq=1 time=12.5ms"
```

## What We Test

- Echo requests have type 8.
- Echo replies have type 0.
- Sequence numbers increment.
- Round-trip time is recorded.

## Next Up

We've been sending text. But what about binary data like images?
Head to [Base64](base64.md).
