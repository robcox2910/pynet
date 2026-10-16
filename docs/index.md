# What Is Networking?

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

## Let's Start!

Head to [URLs](concepts/urls.md) to learn how web addresses are
built.
