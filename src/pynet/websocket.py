"""A phone call -- persistent two-way connection.

Model WebSocket concepts without making real network calls. Unlike
HTTP's request-response pattern (like exchanging letters), a WebSocket
is a persistent, full-duplex channel -- like picking up the phone and
keeping the line open so both sides can talk whenever they want.

WebSocket connections start life as an HTTP request that asks the server
to "upgrade" to the WebSocket protocol.
"""

from __future__ import annotations

from dataclasses import dataclass

OPCODE_TEXT = 1
OPCODE_BINARY = 2
OPCODE_CLOSE = 8
OPCODE_PING = 9
OPCODE_PONG = 10

WEBSOCKET_VERSION = "13"
# The client's Sec-WebSocket-Key: a random nonce the client picks for each
# handshake. (Not to be confused with the WebSocket "magic string" -- the
# fixed server GUID 258EAFA5-E914-47DA-95CA-C5AB0DC85B11 that a real server
# appends to this key when computing its Sec-WebSocket-Accept reply.)
WEBSOCKET_CLIENT_KEY = "dGhlIHNhbXBsZSBub25jZQ=="


@dataclass(frozen=True)
class WebSocketFrame:
    """Represent a single WebSocket frame.

    Attributes:
        opcode: Frame type (1=text, 2=binary, 8=close, 9=ping, 10=pong).
        payload: Data carried by this frame.

    """

    opcode: int
    payload: str

    @classmethod
    def text(cls, msg: str) -> WebSocketFrame:
        """Create a text frame.

        Args:
            msg: The text message to carry.

        Returns:
            A WebSocketFrame with opcode TEXT.

        """
        return cls(opcode=OPCODE_TEXT, payload=msg)

    @classmethod
    def close(cls) -> WebSocketFrame:
        """Create a close frame to shut down the connection.

        Returns:
            A WebSocketFrame with opcode CLOSE and empty payload.

        """
        return cls(opcode=OPCODE_CLOSE, payload="")


def make_handshake(host: str, path: str = "/") -> str:
    """Build the HTTP upgrade request that initiates a WebSocket connection.

    Like dialling a phone number and waiting for the other person to
    pick up -- the upgrade request says "let's switch from letters to a
    phone call."

    Args:
        host: The server hostname.
        path: The request path (default "/").

    Returns:
        The full HTTP upgrade request as a string.

    """
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {WEBSOCKET_CLIENT_KEY}",
        f"Sec-WebSocket-Version: {WEBSOCKET_VERSION}",
        "",
        "",
    ]
    return "\r\n".join(lines)
