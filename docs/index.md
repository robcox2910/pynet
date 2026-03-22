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

## Let's Start!

Head to [URLs](concepts/urls.md) to learn how web addresses are
built.
