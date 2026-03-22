"""Test TCP connection modelling."""

import pytest

from pynet.tcp import (
    HANDSHAKE_PACKET_COUNT,
    INITIAL_SEQUENCE_NUMBER,
    TCPConnection,
    TCPFlag,
    TCPPacket,
    TCPState,
)

EXPECTED_SYN_ACK_NUMBER = 1
EXPECTED_ACK_SEQ_NUMBER = 1
EXPECTED_ACK_ACK_NUMBER = 1
SEQUENCE_ONE = 1
ACK_TWO = 2


class TestTCPPacket:
    """Test the TCPPacket dataclass."""

    def test_packet_stores_fields(self) -> None:
        """Store sequence number, ack number, flags, and data."""
        pkt = TCPPacket(
            sequence_number=SEQUENCE_ONE, ack_number=ACK_TWO, flags=TCPFlag.SYN, data="hi"
        )

        assert pkt.sequence_number == SEQUENCE_ONE
        assert pkt.ack_number == ACK_TWO
        assert pkt.flags == TCPFlag.SYN
        assert pkt.data == "hi"

    def test_packet_default_data_is_empty(self) -> None:
        """Default data field to an empty string."""
        pkt = TCPPacket(sequence_number=0, ack_number=0, flags=TCPFlag.ACK)

        assert pkt.data == ""

    def test_packet_is_frozen(self) -> None:
        """Prevent mutation of packet fields."""
        pkt = TCPPacket(sequence_number=0, ack_number=0, flags=TCPFlag.SYN)

        with pytest.raises(AttributeError):
            pkt.sequence_number = 5  # type: ignore[misc]


class TestTCPConnection:
    """Test the TCP connection state machine."""

    def test_initial_state_is_closed(self) -> None:
        """Start a new connection in the CLOSED state."""
        conn = TCPConnection()

        assert conn.state == TCPState.CLOSED

    def test_handshake_returns_three_packets(self) -> None:
        """Return exactly three packets for the three-way handshake."""
        conn = TCPConnection()
        packets = conn.handshake()

        assert len(packets) == HANDSHAKE_PACKET_COUNT

    def test_handshake_packet_flags(self) -> None:
        """Produce SYN, SYN-ACK, and ACK packets in order."""
        conn = TCPConnection()
        syn, syn_ack, ack = conn.handshake()

        assert syn.flags == TCPFlag.SYN
        assert syn_ack.flags == TCPFlag.SYN_ACK
        assert ack.flags == TCPFlag.ACK

    def test_handshake_sequence_numbers(self) -> None:
        """Set correct sequence and ack numbers across the handshake."""
        conn = TCPConnection()
        syn, syn_ack, ack = conn.handshake()

        assert syn.sequence_number == INITIAL_SEQUENCE_NUMBER
        assert syn_ack.ack_number == EXPECTED_SYN_ACK_NUMBER
        assert ack.sequence_number == EXPECTED_ACK_SEQ_NUMBER
        assert ack.ack_number == EXPECTED_ACK_ACK_NUMBER

    def test_handshake_transitions_to_established(self) -> None:
        """Move the connection to ESTABLISHED after the handshake."""
        conn = TCPConnection()
        conn.handshake()

        assert conn.state == TCPState.ESTABLISHED

    def test_handshake_from_non_closed_raises(self) -> None:
        """Raise RuntimeError when handshake is called on an active connection."""
        conn = TCPConnection()
        conn.handshake()

        with pytest.raises(RuntimeError, match="Cannot handshake"):
            conn.handshake()

    def test_close_returns_fin_packet(self) -> None:
        """Return a FIN packet when closing an established connection."""
        conn = TCPConnection()
        conn.handshake()
        fin = conn.close()

        assert fin.flags == TCPFlag.FIN

    def test_close_transitions_to_closed(self) -> None:
        """Move the connection back to CLOSED after closing."""
        conn = TCPConnection()
        conn.handshake()
        conn.close()

        assert conn.state == TCPState.CLOSED

    def test_close_from_closed_raises(self) -> None:
        """Raise RuntimeError when closing a connection that is not established."""
        conn = TCPConnection()

        with pytest.raises(RuntimeError, match="Cannot close"):
            conn.close()
