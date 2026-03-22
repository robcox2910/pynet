# SMTP

## Writing a Postcard

When you send a postcard:
1. Write the sender and recipient addresses
2. Write the message
3. Drop it in a mailbox
4. The postal system delivers it

**SMTP** (Simple Mail Transfer Protocol) is how computers send email.
It's literally a conversation between your computer and a mail server:

```
You:    HELO mycomputer
Server: 250 Hello mycomputer

You:    MAIL FROM:<alice@example.com>
Server: 250 OK

You:    RCPT TO:<bob@example.com>
Server: 250 OK

You:    DATA
Server: 354 Start mail input

You:    Subject: Hello!
        Hi Bob, how are you?
        .
Server: 250 OK, message queued
```

## SMTP in PyNet

```python
from pynet.smtp import build_email, format_smtp_session

email = build_email(
    sender="alice@example.com",
    recipient="bob@example.com",
    subject="Hello!",
    body="Hi Bob, how are you?"
)

# See the SMTP conversation
session = format_smtp_session(email)
print(session)
```

## What We Test

- Emails have sender, recipient, subject, and body.
- The SMTP session follows the correct command sequence.
- The DATA section ends with a lone dot on a line.

## Next Up

SMTP sends emails. FTP sends files. Head to [FTP](ftp.md).
