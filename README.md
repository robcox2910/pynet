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

## Features

- **URL Parsing** -- Break apart web addresses into their components
- **DNS Resolution** -- Translate hostnames into IP addresses
- **HTTP Client** -- Send GET and POST requests over raw TCP sockets
- **JSON Client** -- Talk to web APIs with structured data
- **Custom Errors** -- Clear error types for DNS, connection, and HTTP failures
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

## Documentation

Full documentation with kid-friendly explanations of every concept:
[https://robcox2910.github.io/pynet/](https://robcox2910.github.io/pynet/)

## Related Projects

PyNet is part of a series of educational "build it from scratch" projects:

| Project | What it builds |
|---------|---------------|
| [PyOS](https://github.com/robcox2910/pyos) | An operating system |
| [Pebble](https://github.com/robcox2910/pebble) | A database engine |
| [PyDB](https://github.com/robcox2910/pydb) | A SQL database |
| [PyStack](https://github.com/robcox2910/pystack) | A network stack |
| [PyWeb](https://github.com/robcox2910/pyweb) | An HTTP framework |
| [PyGit](https://github.com/robcox2910/pygit) | A version control system |
| [PyCrypt](https://github.com/robcox2910/pycrypt) | A cryptography library |

## License

MIT -- see [LICENSE](LICENSE) for details.
