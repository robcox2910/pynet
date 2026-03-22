"""Test TLS handshake modelling."""

import pytest

from pynet.tls import ClientHello, ServerHello, TLSVersion, negotiate

CIPHER_AES_128 = "TLS_AES_128_GCM_SHA256"
CIPHER_AES_256 = "TLS_AES_256_GCM_SHA384"
CIPHER_CHACHA = "TLS_CHACHA20_POLY1305_SHA256"
CIPHER_UNKNOWN = "TLS_NULL_WITH_NULL_NULL"


class TestTLSVersion:
    """Test the TLSVersion enum."""

    def test_tls_1_0_value(self) -> None:
        """Represent TLS 1.0 as a string."""
        assert TLSVersion.TLS_1_0 == "TLS 1.0"

    def test_tls_1_2_value(self) -> None:
        """Represent TLS 1.2 as a string."""
        assert TLSVersion.TLS_1_2 == "TLS 1.2"

    def test_tls_1_3_value(self) -> None:
        """Represent TLS 1.3 as a string."""
        assert TLSVersion.TLS_1_3 == "TLS 1.3"


class TestClientHello:
    """Test the ClientHello dataclass."""

    def test_client_hello_stores_version(self) -> None:
        """Store the TLS version on the client hello."""
        hello = ClientHello(version=TLSVersion.TLS_1_3, cipher_suites=[CIPHER_AES_128])

        assert hello.version == TLSVersion.TLS_1_3

    def test_client_hello_stores_ciphers(self) -> None:
        """Store the list of cipher suites on the client hello."""
        ciphers = [CIPHER_AES_128, CIPHER_CHACHA]
        hello = ClientHello(version=TLSVersion.TLS_1_3, cipher_suites=ciphers)

        assert hello.cipher_suites == ciphers


class TestNegotiate:
    """Test the TLS cipher negotiation."""

    def test_pick_first_common_cipher(self) -> None:
        """Select the client's first cipher that the server also supports."""
        client_hello = ClientHello(
            version=TLSVersion.TLS_1_3,
            cipher_suites=[CIPHER_AES_256, CIPHER_AES_128],
        )
        server_ciphers = [CIPHER_AES_128, CIPHER_CHACHA]

        result = negotiate(client_hello, server_ciphers)

        assert result.chosen_cipher == CIPHER_AES_128

    def test_negotiated_version_matches_client(self) -> None:
        """Use the client's TLS version in the server hello."""
        client_hello = ClientHello(
            version=TLSVersion.TLS_1_2,
            cipher_suites=[CIPHER_AES_128],
        )

        result = negotiate(client_hello, [CIPHER_AES_128])

        assert result.version == TLSVersion.TLS_1_2

    def test_returns_server_hello(self) -> None:
        """Return a ServerHello instance."""
        client_hello = ClientHello(
            version=TLSVersion.TLS_1_3,
            cipher_suites=[CIPHER_AES_128],
        )

        result = negotiate(client_hello, [CIPHER_AES_128])

        assert isinstance(result, ServerHello)

    def test_no_common_cipher_raises(self) -> None:
        """Raise ValueError when no common cipher suite exists."""
        client_hello = ClientHello(
            version=TLSVersion.TLS_1_3,
            cipher_suites=[CIPHER_UNKNOWN],
        )

        with pytest.raises(ValueError, match="No common cipher suite"):
            negotiate(client_hello, [CIPHER_AES_128])

    def test_client_preference_order(self) -> None:
        """Respect the client's preference order when multiple ciphers match."""
        client_hello = ClientHello(
            version=TLSVersion.TLS_1_3,
            cipher_suites=[CIPHER_CHACHA, CIPHER_AES_128, CIPHER_AES_256],
        )
        server_ciphers = [CIPHER_AES_128, CIPHER_AES_256, CIPHER_CHACHA]

        result = negotiate(client_hello, server_ciphers)

        assert result.chosen_cipher == CIPHER_CHACHA
