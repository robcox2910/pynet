# DNS

## The Phone Book

You know your friend's name is "Alice", but your phone needs her
number (555-0123) to make the call. The **phone book** maps names
to numbers.

**DNS** (Domain Name System) does the same for the internet. You
type `google.com` and DNS translates it to `142.250.80.46` -- the
IP address your computer needs to make the connection.

## How DNS Works (Simplified)

```
You type: google.com
    │
    └── Your computer asks the DNS server:
        "What's the IP for google.com?"
    │
    └── DNS server replies:
        "142.250.80.46"
    │
    └── Your computer connects to 142.250.80.46
```

In reality there's a chain of DNS servers (root → TLD → authoritative),
but the concept is the same: look up a name, get a number.

## DNS in PyNet

```python
from pynet.dns import resolve

ip = resolve("example.com")
# "93.184.216.34"
```

Our educational DNS resolver uses Python's `socket.getaddrinfo` under
the hood, but wraps it in a simple interface that teaches the concept.

## What We Test

- Known hostnames resolve to IP addresses.
- The result is a valid IPv4 address format.
- Unknown hostnames raise a clear error.

## Next Up

We have the address and the phone number. Time to make the call.
Head to [HTTP Client](http-client.md).
