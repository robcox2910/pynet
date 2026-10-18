"""URL parsing -- reading the address on the envelope.

A URL like `https://example.com:443/about?page=2` has many parts.
This module breaks it apart so other modules know where to connect,
what to request, and what extra info to send.
"""

from dataclasses import dataclass, field

# Default ports for common schemes.
DEFAULT_PORTS: dict[str, int] = {
    "http": 80,
    "https": 443,
    "ftp": 21,
}


@dataclass(frozen=True)
class URL:
    """A parsed URL with all its parts.

    Args:
        scheme: The protocol (http, https, ftp).
        host: The server hostname or IP.
        port: The port number.
        path: The path on the server.
        query: Query parameters as a dict.
        fragment: The fragment/anchor (after #).

    """

    scheme: str
    host: str
    port: int
    path: str = "/"
    query: dict[str, str] = field(default_factory=lambda: {})
    fragment: str = ""


def parse_url(raw: str) -> URL:
    """Parse a URL string into its components.

    Args:
        raw: The URL string (e.g., "https://example.com:8080/path?q=1").

    Returns:
        A parsed URL object.

    Raises:
        ValueError: If the text after the last ":" of the host is not a
            whole number (a port like ``example.com:abc``).

    """
    # Extract scheme.
    scheme = "http"
    rest = raw
    if "://" in raw:
        scheme, rest = raw.split("://", maxsplit=1)

    # Extract fragment.
    fragment = ""
    if "#" in rest:
        rest, fragment = rest.split("#", maxsplit=1)

    # Extract query string.
    query: dict[str, str] = {}
    if "?" in rest:
        rest, query_string = rest.split("?", maxsplit=1)
        for pair in query_string.split("&"):
            if "=" in pair:
                key, value = pair.split("=", maxsplit=1)
                query[key] = value
            elif pair:
                query[pair] = ""

    # Extract path.
    path = "/"
    if "/" in rest:
        host_port, path = rest.split("/", maxsplit=1)
        path = "/" + path
    else:
        host_port = rest

    # Extract port.
    port = DEFAULT_PORTS.get(scheme, 80)
    host = host_port
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", maxsplit=1)
        port = int(port_str)

    return URL(
        scheme=scheme,
        host=host,
        port=port,
        path=path,
        query=query,
        fragment=fragment,
    )
