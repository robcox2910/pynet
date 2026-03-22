# JSON Client

## Structured Conversations

When you talk to a friend, you use sentences. When your program talks
to a web API, it uses **JSON** -- a structured format that both sides
understand:

```json
{"name": "Alice", "score": 95, "active": true}
```

A **JSON client** sends JSON data and reads JSON replies. It's how
mobile apps talk to servers, how weather apps get forecasts, and how
almost every modern web service works.

## JSON Client in PyNet

```python
from pynet.json_client import json_get, json_post

# GET a JSON API
data = json_get("http://api.example.com/users/1")
# {"name": "Alice", "age": 25}

# POST JSON data
result = json_post("http://api.example.com/users", {
    "name": "Bob",
    "age": 30
})
# {"id": 2, "status": "created"}
```

## What We Test

- GET requests parse JSON responses into dicts.
- POST requests serialize dicts to JSON and send them.
- Non-JSON responses raise clear errors.

## Next Up

You've learned every layer of networking! From URLs to DNS to HTTP
to JSON APIs. These are the same tools that power every app, website,
and connected device in the world.
