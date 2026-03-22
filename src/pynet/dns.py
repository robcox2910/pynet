"""DNS resolution -- the phone book for the internet.

DNS translates human-friendly hostnames like `example.com` into
computer-friendly IP addresses like `93.184.216.34`. Without DNS,
you'd have to remember numbers for every website.

This module wraps Python's `socket.getaddrinfo` in a simple
educational interface.
"""

import socket

from pynet.errors import DNSError


def resolve(hostname: str) -> str:
    """Look up the IP address for a hostname.

    Like looking up a name in a phone book to find the number.

    Args:
        hostname: The hostname to resolve (e.g., "example.com").

    Returns:
        The IPv4 address as a string (e.g., "93.184.216.34").

    Raises:
        DNSError: If the hostname cannot be resolved.

    """
    try:
        results = socket.getaddrinfo(hostname, None, socket.AF_INET)
    except socket.gaierror as exc:
        msg = f"Could not resolve hostname: {hostname!r}"
        raise DNSError(msg) from exc

    if not results:
        msg = f"No results for hostname: {hostname!r}"
        raise DNSError(msg)

    # Return the first IPv4 address found.
    return results[0][4][0]


def is_valid_ipv4(address: str) -> bool:
    """Check whether a string is a valid IPv4 address.

    Args:
        address: The string to check.

    Returns:
        True if the string looks like a valid IPv4 address.

    """
    parts = address.split(".")
    expected_parts = 4
    if len(parts) != expected_parts:
        return False
    max_octet = 255
    return all(p.isdigit() and 0 <= int(p) <= max_octet for p in parts)
