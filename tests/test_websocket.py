"""Test WebSocket frame and handshake modelling."""

import pytest

from pynet.websocket import (
    OPCODE_BINARY,
    OPCODE_CLOSE,
    OPCODE_PING,
    OPCODE_PONG,
    OPCODE_TEXT,
    WEBSOCKET_CLIENT_KEY,
    WEBSOCKET_VERSION,
    WebSocketFrame,
    make_handshake,
)


class TestWebSocketFrame:
    """Test the WebSocketFrame dataclass and class methods."""

    def test_text_frame_has_correct_opcode(self) -> None:
        """Create a text frame with opcode 1."""
        frame = WebSocketFrame.text("hello")

        assert frame.opcode == OPCODE_TEXT
        assert frame.payload == "hello"

    def test_close_frame_has_correct_opcode(self) -> None:
        """Create a close frame with opcode 8 and empty payload."""
        frame = WebSocketFrame.close()

        assert frame.opcode == OPCODE_CLOSE
        assert frame.payload == ""

    def test_manual_binary_frame(self) -> None:
        """Create a binary frame by passing the opcode directly."""
        frame = WebSocketFrame(opcode=OPCODE_BINARY, payload="raw")

        assert frame.opcode == OPCODE_BINARY

    def test_manual_ping_frame(self) -> None:
        """Create a ping frame by passing the opcode directly."""
        frame = WebSocketFrame(opcode=OPCODE_PING, payload="")

        assert frame.opcode == OPCODE_PING

    def test_manual_pong_frame(self) -> None:
        """Create a pong frame by passing the opcode directly."""
        frame = WebSocketFrame(opcode=OPCODE_PONG, payload="pong-data")

        assert frame.opcode == OPCODE_PONG

    def test_frame_is_frozen(self) -> None:
        """Prevent mutation of frame fields."""
        frame = WebSocketFrame.text("hi")

        with pytest.raises(AttributeError):
            frame.opcode = OPCODE_CLOSE  # type: ignore[misc]


class TestMakeHandshake:
    """Test the WebSocket handshake request builder."""

    def test_handshake_starts_with_get(self) -> None:
        """Start the upgrade request with a GET line."""
        req = make_handshake("example.com")

        assert req.startswith("GET / HTTP/1.1\r\n")

    def test_handshake_contains_upgrade_header(self) -> None:
        """Include the Upgrade: websocket header."""
        req = make_handshake("example.com")

        assert "Upgrade: websocket" in req

    def test_handshake_contains_host(self) -> None:
        """Include the Host header with the given hostname."""
        req = make_handshake("chat.example.org")

        assert "Host: chat.example.org" in req

    def test_handshake_contains_version(self) -> None:
        """Include the Sec-WebSocket-Version header."""
        req = make_handshake("example.com")

        assert f"Sec-WebSocket-Version: {WEBSOCKET_VERSION}" in req

    def test_handshake_contains_key(self) -> None:
        """Include the Sec-WebSocket-Key header."""
        req = make_handshake("example.com")

        assert f"Sec-WebSocket-Key: {WEBSOCKET_CLIENT_KEY}" in req

    def test_handshake_custom_path(self) -> None:
        """Use the specified path in the GET line."""
        req = make_handshake("example.com", path="/ws/chat")

        assert req.startswith("GET /ws/chat HTTP/1.1\r\n")
