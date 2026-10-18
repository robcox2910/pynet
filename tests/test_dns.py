"""Test DNS resolution."""

import pytest

from pynet.dns import is_valid_ipv4, resolve
from pynet.errors import DNSError

LOCALHOST_IPV4 = "127.0.0.1"
IPV4_OCTET_COUNT = 4
MAX_OCTET_VALUE = 255


class TestResolve:
    """Test the resolve function."""

    def test_localhost_resolves_to_loopback(self) -> None:
        """Resolving 'localhost' return 127.0.0.1."""
        result = resolve("localhost")

        assert result == LOCALHOST_IPV4

    def test_unknown_host_raises_dns_error(self) -> None:
        """An unresolvable hostname raise DNSError."""
        with pytest.raises(DNSError):
            resolve("this-host-definitely-does-not-exist.invalid")


class TestIsValidIpv4:
    """Test the is_valid_ipv4 function."""

    @pytest.mark.parametrize(
        "address",
        [
            "192.168.1.1",
            "0.0.0.0",
            "255.255.255.255",
            LOCALHOST_IPV4,
            "10.0.0.1",
        ],
    )
    def test_valid_addresses_return_true(self, address: str) -> None:
        """Valid IPv4 addresses return True."""
        assert is_valid_ipv4(address) is True

    @pytest.mark.parametrize(
        "address",
        [
            "256.0.0.1",
            "1.2.3",
            "1.2.3.4.5",
            "abc.def.ghi.jkl",
            "",
            "192.168.1",
            "192.168.1.1.1",
            "192.168.001.1",
            "192.168.01.1",
            "010.0.0.1",
        ],
    )
    def test_invalid_addresses_return_false(self, address: str) -> None:
        """Invalid IPv4 addresses return False."""
        assert is_valid_ipv4(address) is False

    def test_digits_from_other_alphabets_are_not_valid(self) -> None:
        """Regression: an Arabic-Indic zero passed as an octet, found by a property test."""
        assert is_valid_ipv4("0.0.0.\u0660") is False

    def test_digit_like_symbols_are_not_valid(self) -> None:
        """Regression: a superscript two crashed with ValueError, found by a property test."""
        assert is_valid_ipv4("0.0.0.\u00b2") is False
