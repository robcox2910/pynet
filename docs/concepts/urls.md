# URLs

## The Address on the Envelope

Before you mail a letter, you write the address:

```
Ms. Alice Smith         ← who (host)
Apartment 443           ← door number (port)
123 Main Street         ← street (path)
London, UK              ← city/country (protocol tells us HOW to get there)
```

A **URL** (Uniform Resource Locator) is the address for something on
the internet:

```
https://example.com:443/about?page=2#section1
  │         │        │    │      │       │
  │         │        │    │      │       └── fragment (bookmark on the page)
  │         │        │    │      └── query (extra info)
  │         │        │    └── path (which page)
  │         │        └── port (which door)
  │         └── host (which building)
  └── scheme (how to get there: http, https, ftp)
```

## URLs in PyNet

```python
from pynet.url import parse_url

url = parse_url("https://example.com:8080/api/users?page=2")
url.scheme    # "https"
url.host      # "example.com"
url.port      # 8080
url.path      # "/api/users"
url.query     # {"page": "2"}
```

## Default Ports

If no port is specified, the scheme tells us:

| Scheme | Default Port |
|--------|-------------|
| `http` | 80 |
| `https` | 443 |

## What We Test

- All URL parts are parsed correctly.
- Default ports are assigned by scheme.
- Query parameters are parsed into a dictionary.
- Missing parts get sensible defaults.

## Next Up

We have the address. Now we need to look up the actual phone number.
Head to [DNS](dns.md).
