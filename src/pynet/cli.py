"""PyNet CLI -- demonstrate networking concepts."""

import sys

from pynet.dns import resolve
from pynet.url import parse_url


def main() -> None:
    """Run the PyNet demo showing URL parsing and DNS resolution."""
    sys.stdout.write("PyNet -- Networking from scratch\n\n")

    # Demo URL parsing.
    demo_url = "https://example.com:8080/api/users?page=2#results"
    sys.stdout.write(f"Parsing URL: {demo_url}\n")
    parsed = parse_url(demo_url)
    sys.stdout.write(f"  Scheme:   {parsed.scheme}\n")
    sys.stdout.write(f"  Host:     {parsed.host}\n")
    sys.stdout.write(f"  Port:     {parsed.port}\n")
    sys.stdout.write(f"  Path:     {parsed.path}\n")
    sys.stdout.write(f"  Query:    {parsed.query}\n")
    sys.stdout.write(f"  Fragment: {parsed.fragment}\n\n")

    # Demo DNS resolution.
    hostname = "localhost"
    sys.stdout.write(f"Resolving {hostname!r}...\n")
    ip_address = resolve(hostname)
    sys.stdout.write(f"  {hostname} -> {ip_address}\n")
