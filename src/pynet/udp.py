"""Shouting across a room -- fast but unreliable.

Model UDP concepts without making real network calls. UDP sends
individual datagrams with no guarantee of delivery, ordering, or
duplicate protection. Think of shouting across a crowded room: your
message might get lost in the noise, but it arrives instantly if it
does get through.

UDP is perfect for video calls, games, and DNS lookups where speed
matters more than perfection.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SOURCE_PORT = 0


@dataclass(frozen=True)
class UDPDatagram:
    """Represent a single UDP datagram.

    Attributes:
        source_port: Port the datagram originates from.
        dest_port: Port the datagram is addressed to.
        data: Payload carried by this datagram.

    """

    source_port: int
    dest_port: int
    data: str


def create_datagram(
    data: str,
    dest_port: int,
    source_port: int = DEFAULT_SOURCE_PORT,
) -> UDPDatagram:
    """Create a UDP datagram ready to send.

    Like writing a message on a slip of paper and tossing it across
    the room -- no setup, no confirmation, just fire and forget.

    Args:
        data: The payload to carry.
        dest_port: The destination port number.
        source_port: The source port number (0 lets the OS pick one).

    Returns:
        A new UDPDatagram.

    """
    return UDPDatagram(source_port=source_port, dest_port=dest_port, data=data)
