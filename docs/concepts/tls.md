# TLS Handshake

## The Secret Handshake

Imagine you and your friend have a secret handshake that proves
you're really who you say you are. Once you've done the handshake,
you switch to a secret language that only you two understand.

**TLS** (Transport Layer Security) does this for the internet. It's
the "S" in HTTPS. Before any data is sent, the client and server
perform a **handshake** to:

1. **Verify identity** -- "Are you really google.com?"
2. **Agree on encryption** -- "Let's use AES-256"
3. **Exchange keys** -- "Here's my half of the secret"

```
Client                          Server
  │── ClientHello ────────────►│   "I speak TLS 1.3, I know AES/ChaCha"
  │◄── ServerHello ────────────│   "Let's use TLS 1.3 with AES-256"
  │◄── Certificate ────────────│   "Here's my ID card (certificate)"
  │── Key Exchange ───────────►│   "Here's my half of the shared secret"
  │◄── Finished ───────────────│   "Ready! Everything from now is encrypted"
  │── Finished ───────────────►│   "Agreed!"
  │                             │
  │══ Encrypted data ══════════│   🔒 All data is now encrypted
```

## The Key Ideas

| Step | What Happens | Analogy |
|------|-------------|---------|
| ClientHello | Client lists what it supports | "I speak English and French" |
| ServerHello | Server picks the best option | "Let's speak French" |
| Certificate | Server proves its identity | Showing your passport |
| Key Exchange | Both sides create a shared secret | Mixing two paint colours |
| Finished | Both confirm they agree | The secret handshake |

## TLS Handshake in PyNet

```python
from pynet.tls import ClientHello, ServerHello, TLSVersion

hello = ClientHello(
    version=TLSVersion.TLS_1_3,
    cipher_suites=["AES_256_GCM", "CHACHA20_POLY1305"],
)

response = ServerHello(
    version=TLSVersion.TLS_1_3,
    chosen_cipher="AES_256_GCM",
)
```

## What We Test

- ClientHello lists supported versions and ciphers.
- ServerHello picks one cipher from the client's list.
- Version negotiation works correctly.
- The handshake sequence is correct.

## What's Next?

You've learned every major network protocol! TCP, UDP, WebSocket,
SMTP, FTP, Ping, Base64, and TLS. These are the building blocks
that power the entire internet.
