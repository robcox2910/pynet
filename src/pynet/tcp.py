"""Registered mail -- reliable, ordered delivery.

Model TCP concepts without making real network calls. TCP guarantees
that every byte arrives in order by numbering each segment and requiring
the receiver to acknowledge it. Think of registered mail: you get a
receipt for every letter, and missing ones are re-sent.

The three-way handshake (SYN → SYN-ACK → ACK) is like calling someone
on the telephone: you say "hello" (SYN), they say "hello back" (SYN-ACK),
and you confirm "great, let's talk" (ACK).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class TCPFlag(StrEnum):
    """Represent a TCP control flag."""

    SYN = "SYN"
    ACK = "ACK"
    SYN_ACK = "SYN-ACK"
    FIN = "FIN"


class TCPState(StrEnum):
    """Represent a state in the TCP connection state machine."""

    CLOSED = "CLOSED"
    SYN_SENT = "SYN_SENT"
    ESTABLISHED = "ESTABLISHED"
    FIN_WAIT = "FIN_WAIT"


INITIAL_SEQUENCE_NUMBER = 0
HANDSHAKE_PACKET_COUNT = 3


@dataclass(frozen=True)
class TCPPacket:
    """Represent a single TCP segment.

    Attributes:
        sequence_number: Position of this segment in the data stream.
        ack_number: Next sequence number the sender expects to receive.
        flags: Control flag (SYN, ACK, SYN-ACK, or FIN).
        data: Payload carried by this segment.

    """

    sequence_number: int
    ack_number: int
    flags: TCPFlag
    data: str = ""


class TCPConnection:
    """Model the TCP three-way handshake state machine.

    Track the connection through CLOSED → SYN_SENT → ESTABLISHED →
    FIN_WAIT → CLOSED, just like a real TCP socket.
    """

    def __init__(self) -> None:
        """Create a new connection in the CLOSED state."""
        self.state = TCPState.CLOSED

    def handshake(self) -> tuple[TCPPacket, TCPPacket, TCPPacket]:
        """Perform the three-way handshake and return the three packets.

        Walk through the state machine:
        1. Client sends SYN            (CLOSED → SYN_SENT)
        2. Server replies SYN-ACK      (SYN_SENT → SYN_SENT)
        3. Client sends ACK            (SYN_SENT → ESTABLISHED)

        Returns:
            A tuple of (SYN, SYN-ACK, ACK) packets.

        Raises:
            RuntimeError: If the connection is not in the CLOSED state.

        """
        if self.state != TCPState.CLOSED:
            msg = f"Cannot handshake from state {self.state}"
            raise RuntimeError(msg)

        self.state = TCPState.SYN_SENT

        syn = TCPPacket(
            sequence_number=INITIAL_SEQUENCE_NUMBER,
            ack_number=0,
            flags=TCPFlag.SYN,
        )
        syn_ack = TCPPacket(
            sequence_number=INITIAL_SEQUENCE_NUMBER,
            ack_number=syn.sequence_number + 1,
            flags=TCPFlag.SYN_ACK,
        )
        ack = TCPPacket(
            sequence_number=syn_ack.ack_number,
            ack_number=syn_ack.sequence_number + 1,
            flags=TCPFlag.ACK,
        )

        self.state = TCPState.ESTABLISHED
        return syn, syn_ack, ack

    def close(self) -> TCPPacket:
        """Close the connection by sending a FIN packet.

        Returns:
            The FIN packet.

        Raises:
            RuntimeError: If the connection is not ESTABLISHED.

        """
        if self.state != TCPState.ESTABLISHED:
            msg = f"Cannot close from state {self.state}"
            raise RuntimeError(msg)

        self.state = TCPState.FIN_WAIT
        fin = TCPPacket(
            sequence_number=0,
            ack_number=0,
            flags=TCPFlag.FIN,
        )
        self.state = TCPState.CLOSED
        return fin
