"""Test ICMP ping modelling."""

from pynet.ping import ECHO_REPLY_TYPE, ECHO_REQUEST_TYPE, PingMessage

SEQUENCE_ONE = 1
SEQUENCE_FIVE = 5
RTT_FAST = 1.5
RTT_SLOW = 250.0


class TestPingMessage:
    """Test the PingMessage dataclass and class methods."""

    def test_echo_request_type(self) -> None:
        """Create an echo request with type 8."""
        msg = PingMessage.echo_request(sequence=SEQUENCE_ONE)

        assert msg.msg_type == ECHO_REQUEST_TYPE

    def test_echo_request_sequence(self) -> None:
        """Set the sequence number on an echo request."""
        msg = PingMessage.echo_request(sequence=SEQUENCE_FIVE)

        assert msg.sequence == SEQUENCE_FIVE

    def test_echo_request_has_no_rtt(self) -> None:
        """Leave rtt_ms as None on an echo request."""
        msg = PingMessage.echo_request(sequence=SEQUENCE_ONE)

        assert msg.rtt_ms is None

    def test_echo_reply_type(self) -> None:
        """Create an echo reply with type 0."""
        msg = PingMessage.echo_reply(sequence=SEQUENCE_ONE, rtt_ms=RTT_FAST)

        assert msg.msg_type == ECHO_REPLY_TYPE

    def test_echo_reply_rtt(self) -> None:
        """Store the round-trip time on an echo reply."""
        msg = PingMessage.echo_reply(sequence=SEQUENCE_ONE, rtt_ms=RTT_SLOW)

        assert msg.rtt_ms == RTT_SLOW

    def test_echo_reply_sequence_matches(self) -> None:
        """Preserve the sequence number on an echo reply."""
        msg = PingMessage.echo_reply(sequence=SEQUENCE_FIVE, rtt_ms=RTT_FAST)

        assert msg.sequence == SEQUENCE_FIVE

    def test_request_and_reply_share_sequence(self) -> None:
        """Match a request and reply by their sequence number."""
        req = PingMessage.echo_request(sequence=SEQUENCE_ONE)
        rep = PingMessage.echo_reply(sequence=SEQUENCE_ONE, rtt_ms=RTT_FAST)

        assert req.sequence == rep.sequence
