"""HTTP client -- writing and mailing letters to web servers.

An HTTP client builds a request, sends it to a server over a TCP
socket, and reads the response. This is what happens every time your
browser visits a webpage.
"""

import socket

from pynet.errors import ConnectionError as NetConnectionError
from pynet.errors import HTTPError
from pynet.url import parse_url

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
        NetConnectionError: If the connection fails.

    """
    parsed = parse_url(url)
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
    request_line = f"{method} {parsed.path} {HTTP_VERSION}"
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

    raw_response = b"".join(chunks).decode("utf-8", errors="replace")
    return _parse_response(raw_response)


def _parse_response(raw: str) -> Response:
    """Parse a raw HTTP response string.

    Args:
        raw: The raw HTTP response text.

    Returns:
        A parsed Response object.

    Raises:
        HTTPError: If the response is malformed.

    """
    if not raw.strip():
        msg = "Empty response"
        raise HTTPError(msg)

    # Split headers from body.
    if "\r\n\r\n" in raw:
        header_section, body = raw.split("\r\n\r\n", maxsplit=1)
        lines = header_section.split("\r\n")
    else:
        parts = raw.split("\n\n", maxsplit=1)
        header_section = parts[0]
        body = parts[1] if len(parts) > 1 else ""
        lines = header_section.split("\n")

    # Parse status line.
    if not lines:
        msg = "Missing status line"
        raise HTTPError(msg)

    status_parts = lines[0].split(maxsplit=2)
    min_status_parts = 2
    if len(status_parts) < min_status_parts:
        msg = f"Malformed status line: {lines[0]!r}"
        raise HTTPError(msg)
    status = int(status_parts[1])

    # Parse headers.
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", maxsplit=1)
            headers[key.strip()] = value.strip()

    return Response(status=status, headers=headers, body=body)
