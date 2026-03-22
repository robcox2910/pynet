"""Knocking on the door -- are you there.

Model ICMP ping concepts without making real network calls. Ping
sends a tiny "echo request" packet to a host and waits for an
"echo reply." Think of it as knocking on someone's door: if they
answer, you know they're home and how long it took them to open up
(the round-trip time).

Ping is the simplest network diagnostic tool -- it tells you whether
a host is reachable and how fast the connection is.
"""

from __future__ import annotations

from dataclasses import dataclass

ECHO_REQUEST_TYPE = 8
ECHO_REPLY_TYPE = 0


@dataclass(frozen=True)
class PingMessage:
    """Represent an ICMP ping message.

    Attributes:
        msg_type: Message type (8 = echo request, 0 = echo reply).
        sequence: Sequence number to match requests with replies.
        rtt_ms: Round-trip time in milliseconds (only set on replies).

    """

    msg_type: int
    sequence: int
    rtt_ms: float | None = None

    @classmethod
    def echo_request(cls, sequence: int) -> PingMessage:
        """Create an echo request (type 8).

        Like knocking on the door and waiting for an answer.

        Args:
            sequence: The sequence number for this request.

        Returns:
            A PingMessage with type ECHO_REQUEST.

        """
        return cls(msg_type=ECHO_REQUEST_TYPE, sequence=sequence)

    @classmethod
    def echo_reply(cls, sequence: int, rtt_ms: float) -> PingMessage:
        """Create an echo reply (type 0) with a measured round-trip time.

        Like the person on the other side opening the door and saying
        "I'm here!"

        Args:
            sequence: The sequence number matching the original request.
            rtt_ms: The measured round-trip time in milliseconds.

        Returns:
            A PingMessage with type ECHO_REPLY.

        """
        return cls(msg_type=ECHO_REPLY_TYPE, sequence=sequence, rtt_ms=rtt_ms)
