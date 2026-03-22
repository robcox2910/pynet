# FTP

## Sending Packages vs Letters

HTTP is great for web pages (letters). But what about big files
(packages)? You wouldn't mail a sofa in a letter envelope!

**FTP** (File Transfer Protocol) is designed for moving files. It
uses two separate connections:

1. **Control channel** -- for commands (like talking to the clerk)
2. **Data channel** -- for the actual file transfer (the delivery truck)

```
Client                      Server
  │── USER alice ──────────►│   "Who are you?"
  │◄── 331 Password? ──────│
  │── PASS secret ─────────►│   "OK, logged in"
  │◄── 230 Login OK ────────│
  │── LIST ────────────────►│   "What files do you have?"
  │◄── (file listing) ──────│   (sent on data channel)
  │── RETR file.txt ────────►│   "Send me this file"
  │◄── (file data) ─────────│   (sent on data channel)
  │── QUIT ─────────────────►│   "Goodbye!"
```

## FTP in PyNet

```python
from pynet.ftp import FTPCommand, build_ftp_session

commands = [
    FTPCommand("USER", "alice"),
    FTPCommand("PASS", "secret"),
    FTPCommand("LIST"),
    FTPCommand("RETR", "readme.txt"),
    FTPCommand("QUIT"),
]

session = build_ftp_session(commands)
print(session)
```

## What We Test

- FTP commands format correctly.
- Sessions follow the right sequence.
- Commands include arguments when provided.

## Next Up

How do you know if a server is even there? Head to [Ping](ping.md).
