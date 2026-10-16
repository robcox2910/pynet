"""HTTP client -- writing and mailing letters to web servers.

An HTTP client builds a request, sends it to a server over a TCP
socket, and reads the response. This is what happens every time your
browser visits a webpage.
"""

import socket
from urllib.parse import urlencode

from pynet.errors import HTTPError, NetConnectionError
from pynet.url import URL, parse_url

BUFFER_SIZE = 4096
HTTP_VERSION = "HTTP/1.1"


class Response:
    """An HTTP response received from a server.

    Args:
        status: The HTTP status code (e.g., 200).
        headers: Response headers as a dict.
        body: The response body as a string.

    """

    __slots__ = ("body", "headers", "status")

    def __init__(self, status: int, headers: dict[str, str], body: str) -> None:
        """Create a response."""
        self.status = status
        self.headers = headers
        self.body = body


def get(url: str, headers: dict[str, str] | None = None) -> Response:
    """Send an HTTP GET request.

    Args:
        url: The URL to request.
        headers: Optional extra headers.

    Returns:
        The server's response.

    """
    return _request("GET", url, headers=headers)


def post(url: str, body: str = "", headers: dict[str, str] | None = None) -> Response:
    """Send an HTTP POST request.

    Args:
        url: The URL to send to.
        body: The request body.
        headers: Optional extra headers.

    Returns:
        The server's response.

    """
    return _request("POST", url, body=body, headers=headers)


def _request_target(parsed: URL) -> str:
    """Build the request target -- the path plus any query string.

    Like writing the full room number on an envelope: "/users" alone
    reaches the wrong desk, so we tack the "?page=2" back on to land
    exactly where the caller asked.

    Args:
        parsed: The parsed URL whose path and query we combine.

    Returns:
        The path, followed by "?" and the url-encoded query if present.

    """
    if parsed.query:
        return f"{parsed.path}?{urlencode(parsed.query)}"
    return parsed.path


def _request(
    method: str,
    url: str,
    body: str = "",
    headers: dict[str, str] | None = None,
) -> Response:
    """Build and send an HTTP request, return the response.

    Args:
        method: The HTTP method (GET, POST, etc.).
        url: The URL to request.
        body: The request body (for POST).
        headers: Extra headers to include.

    Returns:
        The parsed response.

    Raises:
        NetConnectionError: If the connection fails, or if the URL uses
            https:// (this from-scratch client speaks plain HTTP only).

    """
    parsed = parse_url(url)
    if parsed.scheme == "https":
        msg = (
            "HTTPS is not supported: PyNet's HTTP client speaks plain, "
            "unencrypted HTTP only. Use an http:// URL. (Wrapping the socket "
            "in TLS is a great exercise to try yourself -- see pynet.tls.)"
        )
        raise NetConnectionError(msg)
    all_headers = {
        "Host": parsed.host,
        "Connection": "close",
        "User-Agent": "PyNet/1.0",
    }
    if headers:
        all_headers.update(headers)
    if body:
        all_headers["Content-Length"] = str(len(body.encode("utf-8")))

    # Build the raw HTTP request.
    request_line = f"{method} {_request_target(parsed)} {HTTP_VERSION}"
    header_lines = [f"{k}: {v}" for k, v in all_headers.items()]
    raw_request = "\r\n".join([request_line, *header_lines, "", body])

    # Send it over a TCP socket.
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(10.0)
            sock.connect((parsed.host, parsed.port))
            sock.sendall(raw_request.encode("utf-8"))

            # Read the full response.
            chunks: list[bytes] = []
            while True:
                chunk = sock.recv(BUFFER_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
    except (OSError, TimeoutError) as exc:
        msg = f"Connection to {parsed.host}:{parsed.port} failed: {exc}"
        raise NetConnectionError(msg) from exc

    return _parse_response(b"".join(chunks))


def _parse_response(raw: bytes) -> Response:
    """Parse a raw HTTP response, byte by byte.

    We work with **bytes**, not text, because the sizes in a response --
    ``Content-Length`` and the chunk sizes below -- count bytes. An emoji is
    one letter but four bytes, so counting letters would cut the body short.

    Args:
        raw: The raw HTTP response bytes, exactly as they arrived.

    Returns:
        A parsed Response object.

    Raises:
        HTTPError: If the response is malformed.

    """
    if not raw.strip():
        msg = "Empty response"
        raise HTTPError(msg)

    # Split headers from body (servers should use CRLF, but some send LF).
    separator = b"\r\n\r\n" if b"\r\n\r\n" in raw else b"\n\n"
    head, _, body = raw.partition(separator)
    lines = head.decode("latin-1").replace("\r\n", "\n").split("\n")

    status = _parse_status_line(lines[0])

    # Parse headers.
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", maxsplit=1)
            headers[key.strip()] = value.strip()

    if _header(headers, "Transfer-Encoding").lower() == "chunked":
        body = _decode_chunked(body)
    elif content_length := _header(headers, "Content-Length"):
        body = _take_content_length(body, content_length)

    return Response(status=status, headers=headers, body=body.decode("utf-8", errors="replace"))


def _parse_status_line(line: str) -> int:
    """Read the status code from a line like ``HTTP/1.1 200 OK``."""
    parts = line.split(maxsplit=2)
    min_status_parts = 2
    if len(parts) < min_status_parts:
        msg = f"Malformed status line: {line!r}"
        raise HTTPError(msg)
    try:
        return int(parts[1])
    except ValueError:
        msg = f"Malformed status code in status line: {line!r}"
        raise HTTPError(msg) from None


def _header(headers: dict[str, str], name: str) -> str:
    """Look up a header, ignoring capital letters (``content-length`` == ``Content-Length``)."""
    wanted = name.lower()
    return next((value for key, value in headers.items() if key.lower() == wanted), "")


def _take_content_length(body: bytes, content_length: str) -> bytes:
    """Keep exactly the number of body bytes the server promised."""
    try:
        length = int(content_length)
    except ValueError:
        msg = f"Bad Content-Length: {content_length!r}"
        raise HTTPError(msg) from None
    if len(body) < length:
        msg = f"Response ended early: expected {length} body bytes, got {len(body)}"
        raise HTTPError(msg)
    return body[:length]


def _decode_chunked(body: bytes) -> bytes:
    r"""Reassemble a ``Transfer-Encoding: chunked`` body.

    Instead of saying the total size up front, the server sends the body in
    pieces, each labelled with its size in hexadecimal, ending with a
    zero-size piece -- like a delivery of numbered parcels where the last box
    is empty to say "that's everything"::

        5\r\nhello\r\n   <- 5 bytes
        0\r\n\r\n        <- the end
    """
    result = bytearray()
    position = 0
    while True:
        line_end = body.find(b"\r\n", position)
        if line_end == -1:
            msg = "Chunked response ended before the final chunk"
            raise HTTPError(msg)
        size_text = body[position:line_end].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError:
            msg = f"Bad chunk size: {size_text!r}"
            raise HTTPError(msg) from None
        if size == 0:
            return bytes(result)
        start = line_end + 2
        chunk = body[start : start + size]
        if len(chunk) < size:
            msg = "Chunked response ended in the middle of a chunk"
            raise HTTPError(msg)
        result.extend(chunk)
        position = start + size + 2  # skip the chunk and its trailing CRLF
