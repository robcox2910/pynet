---
title: "Build your own networking library in Python"
description: "Networking built from scratch in Python: DNS, TCP, UDP, HTTP, WebSockets, TLS and an event loop, explained simply with working code."
---

# PyNet: Build your own networking library in Python

## Telephones for Computers

When you want to talk to a friend who lives far away, you pick up a
**telephone**: dial their number, wait for them to answer, talk back
and forth, then hang up. Networking is the same thing for computers.

Every time you visit a website, send a message, or play an online
game, your computer is making a "phone call" to another computer
somewhere in the world. **PyNet** builds the tools for making those
calls from scratch.

## The Big Ideas

### 1. Sockets -- The Telephone

A **socket** is like a telephone handset. You create one, connect it
to another computer, send data through it, and close it when you're
done.

### 2. TCP -- Registered Mail

**TCP** is like registered mail: every packet is numbered, the
receiver confirms each one, and missing packets are re-sent. It's
reliable but has some overhead (all that tracking).

### 3. DNS -- The Phone Book

You remember `google.com`, not `142.250.80.46`. **DNS** is the phone
book that translates human-friendly names into computer-friendly
numbers (IP addresses).

### 4. URLs -- The Address on the Envelope

A URL like `https://example.com:443/about?page=2` has many parts:
the protocol (https), the host (example.com), the port (443), the
path (/about), and the query (?page=2). PyNet parses all of these.

### 5. HTTP Client -- Writing and Mailing Letters

An **HTTP client** writes a request letter (GET, POST, etc.), sends
it to a server, and reads the reply. This is what `curl`, `wget`,
and Python's `requests` library do under the hood.

## Our Building Blocks

| Concept | Analogy | What It Does |
|---------|---------|-------------|
| **URL Parser** | Reading an address | Break apart `http://host:port/path?query` |
| **DNS Resolver** | Phone book lookup | Translate `example.com` → `93.184.216.34` |
| **TCP Socket** | Telephone call | Connect, send, receive, close |
| **HTTP Client** | Letter writing | Build requests, read responses |
| **JSON Client** | Structured letters | Talk to web APIs with JSON |
| **TCP** | Registered mail | Reliable, ordered delivery with three-way handshake |
| **UDP** | Shouting across a room | Fast, fire-and-forget datagrams |
| **WebSocket** | Phone call | Persistent two-way connection over HTTP upgrade |
| **SMTP** | Writing a postcard | Send email with the SMTP command sequence |
| **FTP** | Sending packages | Transfer files with login, list, retrieve, store |
| **Ping (ICMP)** | Knocking on the door | Check if a host is reachable and measure RTT |
| **Base64** | Encoding photos for email | Convert binary data to safe ASCII text |
| **TLS** | Secret handshake | Negotiate encrypted connections with cipher suites |
| **[Event Loop](concepts/event-loop.md)** | The waiter | Run callbacks and timers in one thread without spinning |
| **[Tasks and Await](concepts/tasks-and-await.md)** | Bookmarks in recipes | Pause one coroutine, run another, resume exactly where it left off |
| **[Cancellation and Timeouts](concepts/cancellation-and-timeouts.md)** | The referee's whistle | Stop a block when time runs out; gather many results at once |
| **[Sync Primitives](concepts/sync-primitives.md)** | Pistol, conveyor belt, key, car park | Event, Queue, Lock and Semaphore for coordinating tasks |
| **[Worker Threads](concepts/threads.md)** | Kitchen helper | Run blocking calls in threads without freezing the loop |
| **[Streams](concepts/streams.md)** | Water pipes with a tap | Non-blocking TCP and pipe readers/writers with backpressure |
| **[Subprocesses](concepts/subprocesses.md)** | Helpers and baby monitors | Run many commands at once, stream their output, notice exits, kill process groups |
| **[Framing](concepts/framing.md)** | Envelopes with the page count on the front | Length-prefixed messages so the reader knows where each one ends |
| **[Messaging](concepts/messaging.md)** | Walkie-talkies with call signs | Versioned message envelopes with ids, methods and binary attachments |
| **[Heartbeats and Reconnect](concepts/heartbeats-and-reconnect.md)** | "Are you still there?" on the phone | Ping on a timer, hang up when pongs go missing, and redial with backoff and jitter |
| **[Timers](concepts/timers.md)** | Kitchen timers | Run a callback later, in order, without a thread per timer |
| **[The Self-Pipe Trick](concepts/self-pipe-trick.md)** | Tapping the waiter's shoulder | Wake a sleeping loop from another thread |
| **[WebSocket, For Real](concepts/websocket-real.md)** | The phone call, for real | The RFC 6455 handshake, frames, and live connections |
| **[Our Loop vs asyncio](concepts/our-loop-vs-asyncio.md)** | Go-kart vs factory car | Side-by-side names, and what asyncio adds |

## Real Networking: The Learning Path

The simulation pages above show *how* the protocols work. The pages
below build the machinery to make them work for real, with a from-scratch
event loop and real sockets. Read them in this order; each page ends
with a "Next Up" link to the following one.

| Step | Simulation page | Real page |
|------|-----------------|-----------|
| 1 | | [Event Loop](concepts/event-loop.md) |
| 2 | | [Timers](concepts/timers.md) |
| 3 | | [Tasks and Await](concepts/tasks-and-await.md) |
| 4 | | [Cancellation and Timeouts](concepts/cancellation-and-timeouts.md) |
| 5 | | [Sync Primitives](concepts/sync-primitives.md) |
| 6 | | [Worker Threads](concepts/threads.md) |
| 7 | | [The Self-Pipe Trick](concepts/self-pipe-trick.md) |
| 8 | | [Subprocesses](concepts/subprocesses.md) |
| 9 | [TCP](concepts/tcp.md) | [Streams](concepts/streams.md) |
| 10 | | [Framing](concepts/framing.md) |
| 11 | | [Messaging](concepts/messaging.md) |
| 12 | | [Heartbeats and Reconnect](concepts/heartbeats-and-reconnect.md) |
| 13 | [WebSocket](concepts/websocket.md) | [WebSocket, For Real](concepts/websocket-real.md) |
| 14 | | [Our Loop vs asyncio](concepts/our-loop-vs-asyncio.md) |

Runnable examples live in the `examples/` folder: an echo server, a job
runner, a chat room, a heartbeat demo and a WebSocket echo server.

## Install

```bash
pip install fromscratch-pynet
```

Or get the whole series at once with `pip install fromscratch-pystack`. To read the code or run the tests, clone it from [GitHub](https://github.com/robcox2910/pynet).

## Let's Start!

Head to [URLs](concepts/urls.md) to learn how web addresses are
built, or jump straight to the [Event Loop](concepts/event-loop.md) to
start the real-networking path.
