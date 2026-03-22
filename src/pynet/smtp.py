"""Writing a postcard -- how email works.

Model SMTP concepts without making real network calls. SMTP (Simple
Mail Transfer Protocol) is the language mail servers speak when
delivering email. Think of it as the postal worker who reads the
address on your postcard, routes it to the right office, and drops
it in the recipient's mailbox.

An SMTP conversation is a strict sequence of commands: greet the
server (HELO), say who the letter is from (MAIL FROM), who it goes
to (RCPT TO), then send the body (DATA), and finally quit.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Email:
    """Represent an email message.

    Attributes:
        sender: The sender's email address.
        recipient: The recipient's email address.
        subject: The subject line.
        body: The message body.

    """

    sender: str
    recipient: str
    subject: str
    body: str


def build_email(sender: str, recipient: str, subject: str, body: str) -> Email:
    """Create an Email with the given fields.

    Args:
        sender: The sender's email address.
        recipient: The recipient's email address.
        subject: The subject line.
        body: The message body.

    Returns:
        A new Email instance.

    """
    return Email(sender=sender, recipient=recipient, subject=subject, body=body)


def format_smtp_session(email: Email) -> str:
    """Format the full SMTP command sequence for an email.

    Walk through the conversation a mail client has with a server,
    just like filling out a postcard step by step: return address,
    destination, then the message itself.

    Args:
        email: The email to format.

    Returns:
        The complete SMTP session as a multi-line string.

    """
    lines = [
        "HELO localhost",
        f"MAIL FROM:<{email.sender}>",
        f"RCPT TO:<{email.recipient}>",
        "DATA",
        f"Subject: {email.subject}",
        "",
        email.body,
        ".",
        "QUIT",
    ]
    return "\r\n".join(lines)
