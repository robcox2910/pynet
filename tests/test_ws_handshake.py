"""Test the real WebSocket opening handshake (RFC 6455)."""

import pytest

from pynet.base64 import b64_decode_bytes, b64_encode
from pynet.errors import HandshakeError, ProtocolError
from pynet.ws import (
    KEY_BYTES,
    WS_GUID,
    client_handshake_request,
    compute_accept,
    generate_key,
    parse_http_head,
    server_handshake_response,
    validate_client_handshake,
)

RFC_KEY = "dGhlIHNhbXBsZSBub25jZQ=="
RFC_ACCEPT = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
GOOD_HEADERS = {
    "Host": "example.com",
    "Upgrade": "websocket",
    "Connection": "Upgrade",
    "Sec-WebSocket-Key": RFC_KEY,
    "Sec-WebSocket-Version": "13",
}
SHORT_KEY = b64_encode(b"x" * (KEY_BYTES - 1))
EXPECTED_RESPONSE = (
    b"HTTP/1.1 101 Switching Protocols\r\n"
    b"Upgrade: websocket\r\n"
    b"Connection: Upgrade\r\n"
    b"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
    b"\r\n"
)
EXPECTED_REQUEST = (
    b"GET /chat HTTP/1.1\r\n"
    b"Host: example.com\r\n"
    b"Upgrade: websocket\r\n"
    b"Connection: Upgrade\r\n"
    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    b"Sec-WebSocket-Version: 13\r\n"
    b"\r\n"
)
STATUS_SWITCHING = "101"


def _without(name: str) -> dict[str, str]:
    """Return the good headers minus ``name``."""
    return {k: v for k, v in GOOD_HEADERS.items() if k != name}


class TestAccept:
    """Test key generation and accept computation."""

    def test_rfc_accept_vector(self) -> None:
        """Compute the RFC 6455 example accept value."""
        assert compute_accept(RFC_KEY) == RFC_ACCEPT

    def test_guid_constant(self) -> None:
        """Expose the fixed GUID."""
        assert GUID == WS_GUID

    def test_generate_key_decodes_to_16_bytes(self) -> None:
        """Generate a key of KEY_BYTES random bytes."""
        assert len(b64_decode_bytes(generate_key())) == KEY_BYTES

    def test_generate_key_uses_injected_source(self) -> None:
        """Ask the injected source for exactly KEY_BYTES bytes."""
        asked: list[int] = []

        def source(count: int) -> bytes:
            """Record the request and return zeros."""
            asked.append(count)
            return bytes(count)

        assert generate_key(source) == b64_encode(bytes(KEY_BYTES))
        assert asked == [KEY_BYTES]


class TestValidate:
    """Test validate_client_handshake."""

    def test_valid_returns_key(self) -> None:
        """Return the client's key."""
        assert validate_client_handshake("GET", GOOD_HEADERS) == RFC_KEY

    def test_headers_case_insensitive(self) -> None:
        """Accept any header-name case and value case."""
        headers = {
            "host": "x",
            "UPGRADE": "WebSocket",
            "connection": "keep-alive, UPGRADE",
            "SEC-WEBSOCKET-KEY": RFC_KEY,
            "sec-websocket-version": "13",
        }
        assert validate_client_handshake("GET", headers) == RFC_KEY

    @pytest.mark.parametrize(
        ("method", "headers"),
        [
            ("POST", GOOD_HEADERS),
            ("GET", _without("Upgrade")),
            ("GET", {**GOOD_HEADERS, "Upgrade": "h2c"}),
            ("GET", _without("Connection")),
            ("GET", {**GOOD_HEADERS, "Connection": "keep-alive"}),
            ("GET", {**GOOD_HEADERS, "Sec-WebSocket-Version": "8"}),
            ("GET", _without("Sec-WebSocket-Version")),
            ("GET", _without("Sec-WebSocket-Key")),
            ("GET", {**GOOD_HEADERS, "Sec-WebSocket-Key": SHORT_KEY}),
            ("GET", {**GOOD_HEADERS, "Sec-WebSocket-Key": "not base64!"}),
        ],
    )
    def test_invalid_handshake_rejected(self, method: str, headers: dict[str, str]) -> None:
        """Raise HandshakeError for each broken request."""
        with pytest.raises(HandshakeError):
            validate_client_handshake(method, headers)

    def test_error_names_problem(self) -> None:
        """Name the offending header in the message."""
        with pytest.raises(HandshakeError, match="Sec-WebSocket-Version"):
            validate_client_handshake("GET", {**GOOD_HEADERS, "Sec-WebSocket-Version": "8"})

    def test_is_protocol_error(self) -> None:
        """Make HandshakeError a ProtocolError."""
        assert issubclass(HandshakeError, ProtocolError)


class TestMessages:
    """Test the handshake request and response bytes."""

    def test_response_contains_accept(self) -> None:
        """Build the exact 101 response for the RFC key."""
        assert server_handshake_response(RFC_KEY) == EXPECTED_RESPONSE

    def test_request_bytes(self) -> None:
        """Build the exact upgrade request."""
        assert client_handshake_request("example.com", "/chat", RFC_KEY) == EXPECTED_REQUEST

    def test_request_validates(self) -> None:
        """Round-trip the request through parse and validate."""
        method, target, headers = parse_http_head(
            client_handshake_request("example.com", "/chat", generate_key())
        )
        assert (method, target) == ("GET", "/chat")
        assert validate_client_handshake(method, headers)


class TestParseHead:
    """Test parse_http_head."""

    def test_request(self) -> None:
        """Return method, target and lower-cased headers."""
        method, target, headers = parse_http_head(EXPECTED_REQUEST)
        assert (method, target) == ("GET", "/chat")
        assert headers["sec-websocket-key"] == RFC_KEY

    def test_response(self) -> None:
        """Return version, status and headers for a response."""
        version, status, headers = parse_http_head(EXPECTED_RESPONSE)
        assert (version, status) == ("HTTP/1.1", STATUS_SWITCHING)
        assert headers["sec-websocket-accept"] == RFC_ACCEPT

    def test_without_blank_line(self) -> None:
        """Accept a head with no terminating blank line."""
        _, _, headers = parse_http_head(b"GET / HTTP/1.1\r\nHost: a")
        assert headers == {"host": "a"}

    def test_duplicates_joined(self) -> None:
        """Join repeated headers with a comma and space."""
        _, _, headers = parse_http_head(b"GET / HTTP/1.1\r\nX: a\r\nx: b\r\n\r\n")
        assert headers["x"] == "a, b"

    @pytest.mark.parametrize(
        "raw",
        [
            b"",
            b"GARBAGE\r\n\r\n",
            b"GET / HTTP/1.1\r\nno colon here\r\n\r\n",
            b"GET / HTTP/1.1\r\n: empty\r\n\r\n",
            b"\xff\xfe / HTTP/1.1\r\n\r\n",
            b"HTTP/1.1\r\n\r\n",
            b" / HTTP/1.1\r\n\r\n",
        ],
    )
    def test_malformed_rejected(self, raw: bytes) -> None:
        """Raise HandshakeError for malformed heads."""
        with pytest.raises(HandshakeError):
            parse_http_head(raw)
