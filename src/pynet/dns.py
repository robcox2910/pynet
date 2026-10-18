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
    return str(results[0][4][0])


def is_valid_ipv4(address: str) -> bool:
    """Check whether a string is a valid IPv4 address.

    Like checking a house number is written properly: it needs four
    numbers split by dots, each from 0 to 255, and no sneaky leading
    zeros ("192.168.001.1" is not a real address).

    Args:
        address: The string to check.

    Returns:
        True if the string looks like a valid IPv4 address.

    """
    parts = address.split(".")
    expected_parts = 4
    if len(parts) != expected_parts:
        return False
    return all(_is_valid_octet(p) for p in parts)


def _is_valid_octet(part: str) -> bool:
    """Check whether one dotted piece of an IPv4 address is valid."""
    max_octet = 255
    max_digits = 3
    # Reject anything but the ten digits 0-9 (Python's isdigit also says
    # yes to "²" and to digits from other alphabets, like "٣"), leading
    # zeros like "01" or "001", and more than 3 digits (thousands of them
    # would even make int() itself refuse).
    if not (part.isascii() and part.isdigit()) or (len(part) > 1 and part[0] == "0"):
        return False
    if len(part) > max_digits:
        return False
    return 0 <= int(part) <= max_octet
