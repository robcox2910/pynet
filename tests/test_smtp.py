"""Test SMTP email modelling."""

from pynet.smtp import Email, build_email, format_smtp_session

SENDER = "alice@example.com"
RECIPIENT = "bob@example.com"
SUBJECT = "Hello"
BODY = "Hi Bob, how are you?"


class TestEmail:
    """Test the Email dataclass."""

    def test_email_stores_fields(self) -> None:
        """Store sender, recipient, subject, and body."""
        email = Email(sender=SENDER, recipient=RECIPIENT, subject=SUBJECT, body=BODY)

        assert email.sender == SENDER
        assert email.recipient == RECIPIENT
        assert email.subject == SUBJECT
        assert email.body == BODY

    def test_email_equality(self) -> None:
        """Consider two emails with identical fields equal."""
        e1 = Email(sender=SENDER, recipient=RECIPIENT, subject=SUBJECT, body=BODY)
        e2 = Email(sender=SENDER, recipient=RECIPIENT, subject=SUBJECT, body=BODY)

        assert e1 == e2


class TestBuildEmail:
    """Test the build_email factory function."""

    def test_build_email_returns_email(self) -> None:
        """Return an Email instance with the given fields."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)

        assert isinstance(email, Email)
        assert email.sender == SENDER

    def test_build_email_empty_body(self) -> None:
        """Allow an empty body string."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, "")

        assert email.body == ""


class TestFormatSmtpSession:
    """Test the SMTP session formatter."""

    def test_session_starts_with_helo(self) -> None:
        """Start the session with a HELO command."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert session.startswith("HELO localhost\r\n")

    def test_session_contains_mail_from(self) -> None:
        """Include the MAIL FROM command with the sender address."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert f"MAIL FROM:<{SENDER}>" in session

    def test_session_contains_rcpt_to(self) -> None:
        """Include the RCPT TO command with the recipient address."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert f"RCPT TO:<{RECIPIENT}>" in session

    def test_session_contains_subject(self) -> None:
        """Include the Subject header inside the DATA section."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert f"Subject: {SUBJECT}" in session

    def test_session_ends_with_quit(self) -> None:
        """End the session with a QUIT command."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert session.endswith("QUIT")

    def test_session_contains_body(self) -> None:
        """Include the email body in the DATA section."""
        email = build_email(SENDER, RECIPIENT, SUBJECT, BODY)
        session = format_smtp_session(email)

        assert BODY in session
