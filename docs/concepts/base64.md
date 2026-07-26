# Base64

## Encoding Photos for Email

Email was designed for text, not binary data like images. If you try
to send raw image bytes through email, they'll get corrupted.

**Base64** solves this by encoding binary data as text characters.
It takes every 3 bytes of binary and converts them to 4 text
characters from a safe alphabet (A-Z, a-z, 0-9, +, /).

```
Binary: [72, 101, 108]  →  "SGVs"
         H    e    l

"Hello" → "SGVsbG8="
```

The `=` at the end is padding (when the input isn't a multiple of 3).

## Why Base64 Matters

- **Email attachments** -- images and files are Base64-encoded
- **Data URLs** -- `<img src="data:image/png;base64,iVBOR...">`
- **API tokens** -- JWT tokens are Base64-encoded JSON
- **HTTP Basic Auth** -- `Authorization: Basic dXNlcjpwYXNz`

## Base64 in PyNet

`b64_encode` takes raw **bytes** (like the contents of a photo) or a
text string, and returns a safe Base64 string:

```python
from pynet.base64 import b64_encode, b64_decode

# Encode real binary bytes -- e.g. the first bytes of a PNG file.
b64_encode(bytes([0x89, 0x50, 0x4E]))   # "iVBO"

# Encode text (converted to UTF-8 bytes for you).
encoded = b64_encode("Hello, world!")
# "SGVsbG8sIHdvcmxkIQ=="

# Decode back to text.
decoded = b64_decode(encoded)
# "Hello, world!"
```

Note: this from-scratch `b64_decode` returns a **text** string, so it
round-trips text perfectly. Recovering the raw bytes of a photo (having
`b64_decode` return `bytes`) is a great exercise to try yourself.

## What We Test

- Encoding then decoding text returns the original.
- Raw bytes (such as a PNG header) encode to the expected Base64.
- Known test vectors produce the expected output.
- Padding is added correctly for different input lengths.
- Empty input produces empty output.

## Next Up

We can encode data for safe transmission. But how do we make the
transmission itself secure? Head to [TLS](tls.md).
