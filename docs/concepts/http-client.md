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

## What We Test

- GET requests return a response with status and body.
- POST requests send a body to the server.
- Headers are included in the request.
- Connection errors raise clear exceptions.

## Next Up

Many modern APIs speak JSON. Head to [JSON Client](json-client.md)
to learn how to talk to web APIs.
