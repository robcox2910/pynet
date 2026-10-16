"""The secret handshake -- establishing encrypted connections.

Model TLS handshake concepts without performing real cryptography.
TLS (Transport Layer Security) wraps a TCP connection in encryption
so eavesdroppers cannot read the data. Think of it as a secret
handshake between two spies: they agree on a code (cipher suite)
before exchanging any secrets.

The negotiation starts with a ClientHello listing the ciphers the
client supports, and the server replies with a ServerHello picking
the strongest one both sides understand.
"""

from dataclasses import dataclass, field
from enum import StrEnum


class TLSVersion(StrEnum):
    """Represent a TLS protocol version."""

    TLS_1_0 = "TLS 1.0"
    TLS_1_2 = "TLS 1.2"
    TLS_1_3 = "TLS 1.3"


@dataclass(frozen=True)
class ClientHello:
    """Represent the client's opening message in a TLS handshake.

    Attributes:
        version: The highest TLS version the client supports.
        cipher_suites: Cipher suites the client can use, in preference order.

    """

    version: TLSVersion
    cipher_suites: list[str] = field(default_factory=list[str])


@dataclass(frozen=True)
class ServerHello:
    """Represent the server's response in a TLS handshake.

    Attributes:
        version: The TLS version chosen for this session.
        chosen_cipher: The cipher suite selected by the server.

    """

    version: TLSVersion
    chosen_cipher: str


def negotiate(client_hello: ClientHello, available_ciphers: list[str]) -> ServerHello:
    """Negotiate a TLS session by picking the best shared cipher.

    Walk through the client's preferred cipher suites in order and
    pick the first one the server also supports -- like two people
    finding a language they both speak.

    Args:
        client_hello: The client's hello message.
        available_ciphers: Cipher suites the server supports.

    Returns:
        A ServerHello with the negotiated version and cipher.

    Raises:
        ValueError: If no common cipher suite exists.

    """
    server_set = set(available_ciphers)
    for cipher in client_hello.cipher_suites:
        if cipher in server_set:
            return ServerHello(
                version=client_hello.version,
                chosen_cipher=cipher,
            )

    msg = "No common cipher suite found"
    raise ValueError(msg)
