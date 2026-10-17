"""Dialling and answering -- the WebSocket opening handshake.

Before two people can chat on the phone, one dials and the other picks
up and says "hello, it's me". A WebSocket starts the same way: the
client sends an ordinary HTTP request asking to "upgrade", with a
random ``Sec-WebSocket-Key``. The server proves it understood by
answering ``101 Switching Protocols`` with a ``Sec-WebSocket-Accept``
derived from that key. After the blank line, the bytes are frames.

Everything here is pure bytes-and-strings logic with no sockets, so the
HTTP server and the endpoints can both reuse it.
"""

import hashlib
import os
from collections.abc import Callable, Mapping

from pynet.base64 import b64_decode_bytes, b64_encode
from pynet.errors import HandshakeError

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
KEY_BYTES = 16
WS_VERSION = "13"

_HTTP_VERSION = "HTTP/1.1"
_CRLF = "\r\n"
_MIN_START_LINE_PARTS = 2
_UPGRADE_WORD = "upgrade"
_WEBSOCKET_WORD = "websocket"


def compute_accept(key: str) -> str:
    """Compute the ``Sec-WebSocket-Accept`` value for a client key.

    Args:
        key: The client's ``Sec-WebSocket-Key``.

    Returns:
        The Base64 SHA-1 of the key plus the fixed GUID.

    """
    return b64_encode(hashlib.sha1((key + WS_GUID).encode()).digest())  # noqa: S324


def generate_key(randbytes: Callable[[int], bytes] = os.urandom) -> str:
    """Make a fresh random ``Sec-WebSocket-Key``.

    Args:
        randbytes: Source of random bytes; replace it in tests.

    Returns:
        The Base64 text of ``KEY_BYTES`` random bytes.

    """
    return b64_encode(randbytes(KEY_BYTES))


def _lookup(headers: Mapping[str, str], name: str) -> str | None:
    """Find a header value ignoring the case of its name."""
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


def validate_client_handshake(method: str, headers: Mapping[str, str]) -> str:
    """Check that a request is a valid WebSocket upgrade.

    Args:
        method: The HTTP method of the request.
        headers: The request headers, with names in any case.

    Returns:
        The client's ``Sec-WebSocket-Key``.

    Raises:
        HandshakeError: If anything required is missing or wrong.

    """
    if method != "GET":
        msg = f"WebSocket handshake needs GET, got {method!r}"
        raise HandshakeError(msg)
    upgrade = _lookup(headers, "Upgrade")
    if upgrade is None or upgrade.strip().lower() != _WEBSOCKET_WORD:
        msg = "missing or wrong Upgrade header (expected 'websocket')"
        raise HandshakeError(msg)
    connection = _lookup(headers, "Connection")
    tokens = [t.strip().lower() for t in (connection or "").split(",")]
    if _UPGRADE_WORD not in tokens:
        msg = "Connection header must contain 'Upgrade'"
        raise HandshakeError(msg)
    version = _lookup(headers, "Sec-WebSocket-Version")
    if version is None or version.strip() != WS_VERSION:
        msg = f"unsupported Sec-WebSocket-Version {version!r} (expected {WS_VERSION!r})"
        raise HandshakeError(msg)
    key = _lookup(headers, "Sec-WebSocket-Key")
    if key is None:
        msg = "missing Sec-WebSocket-Key header"
        raise HandshakeError(msg)
    key = key.strip()
    try:
        raw = b64_decode_bytes(key)
    except ValueError as exc:
        msg = f"Sec-WebSocket-Key is not valid Base64: {exc}"
        raise HandshakeError(msg) from exc
    if len(raw) != KEY_BYTES:
        msg = f"Sec-WebSocket-Key must decode to {KEY_BYTES} bytes, got {len(raw)}"
        raise HandshakeError(msg)
    return key


def server_handshake_response(key: str) -> bytes:
    """Build the ``101 Switching Protocols`` answer to a client key.

    Args:
        key: The client's validated ``Sec-WebSocket-Key``.

    Returns:
        The full response head, ending with the blank line.

    """
    lines = [
        f"{_HTTP_VERSION} 101 Switching Protocols",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Accept: {compute_accept(key)}",
        "",
        "",
    ]
    return _CRLF.join(lines).encode("ascii")


def client_handshake_request(host: str, path: str, key: str) -> bytes:
    """Build the upgrade request a client sends to dial.

    Args:
        host: The ``Host`` header value.
        path: The request target, such as ``/chat``.
        key: The client's ``Sec-WebSocket-Key`` (see ``generate_key``).

    Returns:
        The full request head, ending with the blank line.

    """
    lines = [
        f"GET {path} {_HTTP_VERSION}",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        f"Sec-WebSocket-Version: {WS_VERSION}",
        "",
        "",
    ]
    return _CRLF.join(lines).encode("ascii")


def parse_http_head(raw: bytes) -> tuple[str, str, dict[str, str]]:
    """Parse the head (start line and headers) of an HTTP message.

    The first two slots depend on what kind of message it is:

    - Request (``GET /chat HTTP/1.1``): ``(method, target, headers)``.
    - Response (``HTTP/1.1 101 Switching Protocols``): ``(version,
      status_code, headers)``, both strings, e.g. ``("HTTP/1.1", "101", ...)``.

    Header names are lower-cased and repeated headers are joined with
    ``", "``. The terminating blank line may be present or absent.

    Args:
        raw: The head bytes, using CRLF line endings.

    Returns:
        The two start-line slots and the headers dictionary.

    Raises:
        HandshakeError: If the head is not valid HTTP.

    """
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        msg = "HTTP head is not ASCII"
        raise HandshakeError(msg) from exc
    lines = text.split(_CRLF)
    parts = lines[0].split(" ", 2)
    if len(parts) < _MIN_START_LINE_PARTS:
        msg = f"malformed start line: {lines[0]!r}"
        raise HandshakeError(msg)
    first, second = parts[0], parts[1]
    if not first or not second:
        msg = f"malformed start line: {lines[0]!r}"
        raise HandshakeError(msg)
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            break
        name, sep, value = line.partition(":")
        name = name.strip().lower()
        if not sep or not name:
            msg = f"malformed header line: {line!r}"
            raise HandshakeError(msg)
        value = value.strip()
        headers[name] = f"{headers[name]}, {value}" if name in headers else value
    return first, second, headers
