# PyNet

**An educational networking library built from scratch in Python.**

## Telephones for Computers

Imagine you want to talk to your friend who lives across town. You pick up the
phone, dial their number, and start chatting. The internet works the same way!

When your computer visits a website, it:

1. **Reads the address** -- like reading a phone number off a sticky note
   (URL parsing)
2. **Looks up the number** -- like finding someone in a phone book
   (DNS lookup)
3. **Dials and talks** -- like making the phone call and having a conversation
   (HTTP request and response)

PyNet teaches you how all of this works by building each piece from scratch.

## Examples

### URL Parsing -- Reading the Address

A URL is like a phone number with extra info: who to call, what department,
and what to ask about.

```python
from pynet.url import parse_url

url = parse_url("https://example.com:8080/api/users?page=2#results")

print(url.scheme)    # "https"   -- how to connect
print(url.host)      # "example.com" -- who to call
print(url.port)      # 8080      -- which door to knock on
print(url.path)      # "/api/users" -- what to ask for
print(url.query)     # {"page": "2"} -- extra details
print(url.fragment)  # "results" -- where to scroll on the page
```

### DNS Lookup -- The Phone Book

DNS translates names like `example.com` into numbers like `93.184.216.34`
that computers actually use to find each other.

```python
from pynet.dns import resolve, is_valid_ipv4

ip = resolve("localhost")
print(ip)  # "127.0.0.1" -- your own computer!

print(is_valid_ipv4("192.168.1.1"))  # True
print(is_valid_ipv4("999.0.0.1"))    # False
```

### HTTP GET -- Making the Call

An HTTP request is like picking up the phone and asking a question. The server
picks up and gives you an answer.

```python
from pynet.http import get

response = get("http://example.com/")
print(response.status)   # 200 (OK -- the server understood!)
print(response.body[:50])  # The first 50 characters of the page
```

### JSON API -- Structured Conversations

When programs talk to each other, they use JSON -- a structured format both
sides understand, like filling out a form instead of writing a free-form letter.

```python
from pynet.json_client import json_get

data = json_get("http://api.example.com/users/1")
print(data["name"])  # The user's name from the API
```

### Real Networking -- A Phone Line That Rings

PyNet has its own event loop, built from scratch, and real sockets that run
on it. This echo server and client talk over a real TCP connection on your
own computer:

```python
from pynet.loop import run
from pynet.streams import open_connection, start_server


async def echo(reader, writer):
    data = await reader.read(1024)
    writer.write(data)
    await writer.drain()
    writer.close()


async def main():
    async with await start_server(echo, "127.0.0.1", 0) as server:
        reader, writer = await open_connection("127.0.0.1", server.port)
        writer.write(b"hello, phone line!")
        await writer.drain()
        print(await reader.read(1024))
        writer.close()
        await writer.wait_closed()


run(main())  # b'hello, phone line!'
```

## Features

- **URL Parsing** -- Break apart web addresses into their components
- **DNS Resolution** -- Translate hostnames into IP addresses
- **HTTP Client** -- Send GET and POST requests over raw TCP sockets
- **JSON Client** -- Talk to web APIs with structured data
- **Custom Errors** -- Clear error types for DNS, connection, and HTTP failures
- **TCP** -- Reliable, ordered byte-stream delivery
- **UDP** -- Fast, connectionless datagrams
- **WebSocket** -- Full-duplex communication over a single connection
- **SMTP** -- Send emails programmatically
- **FTP** -- Transfer files between machines
- **Ping/ICMP** -- Check if a host is reachable
- **Base64** -- Encode and decode binary data as text
- **TLS** -- Encrypted connections with certificates
- **Event Loop** -- A from-scratch `select`-based loop with timers, tasks,
  `await`, cancellation, timeouts, and sync primitives (Event, Queue, Lock,
  Semaphore)
- **Real TCP Streams** -- Non-blocking `start_server` and `open_connection`
  with backpressure
- **Subprocesses** -- Run many commands at once and kill whole process groups
- **Framing and Messaging** -- Length-prefixed frames and request/reply/push
  messages with binary attachments
- **Heartbeats and Reconnect** -- Ping/pong liveness checks and redialling
  with exponential backoff and jitter
- **Real WebSocket** -- RFC 6455 handshake, frames, and live connections
- **CLI Tool** -- Interactive demo of networking concepts
- **100% Typed** -- Full type annotations with strict Pyright checking

## Quick Start

```bash
# Install with uv
uv add pynet

# Or install from source
git clone https://github.com/robcox2910/pynet.git
cd pynet
uv sync --all-extras

# Run the CLI demo
uv run pynet

# Run the tests
uv run pytest
```

## Examples to Run

Each file in `examples/` runs with `uv run python examples/<name>.py`:

- `echo_server.py` -- a parrot on a real TCP line (try it with `nc 127.0.0.1 8888`)
- `job_runner.py` -- start three commands at once and watch each finish
- `chat_room.py` -- a group channel built on messaging requests and pushes
- `heartbeat_demo.py` -- watch pings go out and a frozen server get noticed
- `ws_echo.py` -- a WebSocket echo server you can connect to from a browser

<!-- docs:start -->
## Documentation

Full docs (with kid-friendly explanations of every idea): https://robcox2910.github.io/pynet/
<!-- docs:end -->

## Releasing

Other projects in the series ([PyStack](https://github.com/robcox2910/pystack)) use a pinned release of this one,
not whatever is on `main`. To publish a change for them:

1. Merge the change to `main`.
2. Tag it: `git tag -a vX.Y.Z -m vX.Y.Z && git push origin vX.Y.Z`.
   A change to the public API bumps the minor version (`v0.1.0` → `v0.2.0`);
   anything else bumps the patch (`v0.1.0` → `v0.1.1`).
3. In each project that uses it, edit the tag in `[tool.uv.sources]`, run
   `uv lock --upgrade-package <name>`, and open a pull request so CI checks
   the pair together.

<!-- series:start -->
## Related Projects

This project is one of a series that builds real systems "from scratch in Python" to teach how they work. See the [series home](https://robcox2910.github.io/) for the full story.

| Project | What it teaches | Repository | Docs |
| --- | --- | --- | --- |
| PyOS | Operating systems | [py-os](https://github.com/robcox2910/py-os) | [Docs](https://robcox2910.github.io/py-os/) |
| Pebble | Compilers and programming languages | [pebble-lang](https://github.com/robcox2910/pebble-lang) | [Docs](https://robcox2910.github.io/pebble-lang/) |
| PyDB | Relational databases | [pydb](https://github.com/robcox2910/pydb) | [Docs](https://robcox2910.github.io/pydb/) |
| PyKV | Key-value stores | [pykv](https://github.com/robcox2910/pykv) | [Docs](https://robcox2910.github.io/pykv/) |
| PyDocDB | Document databases | [pydoc-db](https://github.com/robcox2910/pydoc-db) | [Docs](https://robcox2910.github.io/pydoc-db/) |
| PyGraphDB | Graph databases | [pygraphdb](https://github.com/robcox2910/pygraphdb) | [Docs](https://robcox2910.github.io/pygraphdb/) |
| PyTSDB | Time-series databases | [pytsdb](https://github.com/robcox2910/pytsdb) | [Docs](https://robcox2910.github.io/pytsdb/) |
| PyVecDB | Vector databases | [pyvecdb](https://github.com/robcox2910/pyvecdb) | [Docs](https://robcox2910.github.io/pyvecdb/) |
| PyColDB | Column-family databases | [pycoldb](https://github.com/robcox2910/pycoldb) | [Docs](https://robcox2910.github.io/pycoldb/) |
| PyCrypt | Cryptography | [pycrypt](https://github.com/robcox2910/pycrypt) | [Docs](https://robcox2910.github.io/pycrypt/) |
| **PyNet** | **Computer networking** | [pynet](https://github.com/robcox2910/pynet) | [Docs](https://robcox2910.github.io/pynet/) |
| PyWeb | HTTP web servers | [pyweb](https://github.com/robcox2910/pyweb) | [Docs](https://robcox2910.github.io/pyweb/) |
| PyGit | Version control | [pygit](https://github.com/robcox2910/pygit) | [Docs](https://robcox2910.github.io/pygit/) |
| PySearch | Search engines | [pysearch](https://github.com/robcox2910/pysearch) | [Docs](https://robcox2910.github.io/pysearch/) |
| PyMQ | Message queues | [pymq](https://github.com/robcox2910/pymq) | [Docs](https://robcox2910.github.io/pymq/) |
| PyLLM | Language models (the brain) | [pyllm](https://github.com/robcox2910/pyllm) | [Docs](https://robcox2910.github.io/pyllm/) |
| PyScheduler | Job scheduling | [pyscheduler](https://github.com/robcox2910/pyscheduler) | [Docs](https://robcox2910.github.io/pyscheduler/) |
| PyStack | Full-stack integration | [pystack](https://github.com/robcox2910/pystack) | [Docs](https://robcox2910.github.io/pystack/) |
<!-- series:end -->

## License

MIT -- see [LICENSE](LICENSE) for details.
