# HTTP Client

## Writing and Mailing Letters

We've learned about web servers (in PyWeb) -- they receive letters
and send replies. Now we're on the **other side**: we're the one
writing the letter and waiting for a reply.

An HTTP client:
1. **Builds a request** (the letter) -- method, URL, headers, body
2. **Sends it** to the server (mails the letter)
3. **Reads the response** (opens the reply)

## Making Requests with PyNet

```python
from pynet.http import get, post

# GET request (ask for a page)
response = get("http://example.com")
response.status   # 200
response.body     # "<html>..."

# POST request (send data)
response = post("http://httpbin.org/post", body='{"name": "Alice"}')
response.status   # 200
```

## Request vs Response (From the Client's View)

| | Request (you send) | Response (you receive) |
|---|---|---|
| **First line** | `GET /about HTTP/1.1` | `HTTP/1.1 200 OK` |
| **Headers** | `Host: example.com` | `Content-Type: text/html` |
| **Body** | (optional, for POST) | The page content |

## How Long Is the Letter?

The reader needs to know where the body ends. Servers tell you in one of two
ways:

- **`Content-Length: 12`** -- "the body is exactly 12 bytes". Notice it's
  *bytes*, not letters: an emoji like 🙂 is one letter but four bytes. That's
  why PyNet reads the response as raw bytes and only turns it into text at the
  very end.
- **`Transfer-Encoding: chunked`** -- the server doesn't know the size up
  front, so it sends the body in numbered parcels. Each parcel starts with its
  size (in hexadecimal), and an empty parcel of size `0` means "that's
  everything":

  ```
  5\r\nhello\r\n        <- a 5-byte parcel
  7\r\n, world\r\n      <- a 7-byte parcel
  0\r\n\r\n             <- the end
  ```

  PyNet unwraps the parcels and gives you `hello, world`.

If a reply is cut off -- fewer bytes than promised, or no final empty parcel --
or the status line is nonsense, you get a clear `HTTPError` rather than half a
page.

## What We Test

- GET requests return a response with status and body.
- POST requests send a body to the server.
- Headers are included in the request.
- Connection errors raise clear exceptions.
- A tiny real server on a free port answers `get()` and `post()` over TCP.
- Chunked bodies are reassembled; `Content-Length` counts bytes; cut-off
  replies and bad status lines raise `HTTPError`.

## Next Up

Many modern APIs speak JSON. Head to [JSON Client](json-client.md)
to learn how to talk to web APIs.
