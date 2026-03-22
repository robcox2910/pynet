"""Test UDP datagram modelling."""

import pytest

from pynet.udp import DEFAULT_SOURCE_PORT, UDPDatagram, create_datagram

DNS_PORT = 53
HTTP_PORT = 80
HIGH_PORT = 9000


class TestUDPDatagram:
    """Test the UDPDatagram dataclass."""

    def test_datagram_stores_fields(self) -> None:
        """Store source port, destination port, and data."""
        dg = UDPDatagram(source_port=HIGH_PORT, dest_port=DNS_PORT, data="query")

        assert dg.source_port == HIGH_PORT
        assert dg.dest_port == DNS_PORT
        assert dg.data == "query"

    def test_datagram_is_frozen(self) -> None:
        """Prevent mutation of datagram fields."""
        dg = UDPDatagram(source_port=0, dest_port=DNS_PORT, data="x")

        with pytest.raises(AttributeError):
            dg.data = "y"  # type: ignore[misc]

    def test_datagram_equality(self) -> None:
        """Consider two datagrams with identical fields equal."""
        dg1 = UDPDatagram(source_port=HIGH_PORT, dest_port=DNS_PORT, data="q")
        dg2 = UDPDatagram(source_port=HIGH_PORT, dest_port=DNS_PORT, data="q")

        assert dg1 == dg2


class TestCreateDatagram:
    """Test the create_datagram factory function."""

    def test_create_with_explicit_source_port(self) -> None:
        """Create a datagram with a specified source port."""
        dg = create_datagram("hello", dest_port=HTTP_PORT, source_port=HIGH_PORT)

        assert dg.source_port == HIGH_PORT
        assert dg.dest_port == HTTP_PORT
        assert dg.data == "hello"

    def test_default_source_port_is_zero(self) -> None:
        """Default the source port to zero when not specified."""
        dg = create_datagram("data", dest_port=DNS_PORT)

        assert dg.source_port == DEFAULT_SOURCE_PORT

    def test_empty_data(self) -> None:
        """Allow an empty string as datagram data."""
        dg = create_datagram("", dest_port=DNS_PORT)

        assert dg.data == ""
