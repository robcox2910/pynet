"""Talking on the phone -- a live WebSocket conversation, RFC 6455.

Once the call is answered (the handshake), both sides can talk whenever
they like. A ``WebSocket`` sends whole messages, glues fragments back
together, answers "are you there?" pings with pongs, and hangs up
politely: whoever wants to end says "goodbye" (a CLOSE frame with a
code), the other says "goodbye" back, and only then is the line put down.

If the other side breaks the rules -- an unmasked client frame, text
that is not UTF-8, a message too big to swallow -- we say goodbye with
the matching close code and stop.
"""

import os
from contextlib import suppress

from pynet.errors import ConnectionClosedError, FrameTooLargeError, ProtocolError
from pynet.loop import Event, Lock, timeout
from pynet.streams import StreamReader, StreamWriter
from pynet.ws.frames import (
    MASK_KEY_SIZE,
    MAX_CONTROL_PAYLOAD,
    Frame,
    FrameReader,
    Opcode,
    encode_ws_frame,
)

CLOSE_NORMAL = 1000
CLOSE_GOING_AWAY = 1001
CLOSE_PROTOCOL_ERROR = 1002
CLOSE_UNSUPPORTED_DATA = 1003
CLOSE_NO_STATUS = 1005
CLOSE_ABNORMAL = 1006
CLOSE_INVALID_UTF8 = 1007
CLOSE_TOO_BIG = 1009
CLOSE_INTERNAL_ERROR = 1011
DEFAULT_WS_MAX_MESSAGE = 1024 * 1024
CLOSE_TIMEOUT = 5.0

_CODE_SIZE = 2
_MAX_REASON_BYTES = MAX_CONTROL_PAYLOAD - _CODE_SIZE
_FIRST_DEFINED_CODE = 1000
_LAST_DEFINED_CODE = 1014
_CLOSE_RESERVED = 1004
_CLOSE_TLS_FAILED = 1015
_NEVER_SENT_CODES = frozenset({_CLOSE_RESERVED, CLOSE_NO_STATUS, CLOSE_ABNORMAL, _CLOSE_TLS_FAILED})
_FIRST_APP_CODE = 3000
_LAST_APP_CODE = 4999


class WebSocketClosed(ConnectionClosedError):  # noqa: N818 -- name fixed by the API
    """Raise when a WebSocket conversation has ended.

    Args:
        code: The close status code (1005 if the peer gave none, 1006 if
            the connection dropped without a CLOSE frame).
        reason: The human-readable reason, possibly empty.

    """

    def __init__(self, code: int, reason: str = "") -> None:
        """Remember the close ``code`` and ``reason``."""
        super().__init__(f"WebSocket closed with code {code}: {reason or '(no reason)'}")
        self.code = code
        self.reason = reason


class _Failure(Exception):  # noqa: N818 -- internal signal, never escapes
    """Carry the close code and reason for a protocol violation."""

    def __init__(self, code: int, reason: str) -> None:
        """Remember why we must hang up."""
        super().__init__(reason)
        self.code = code
        self.reason = reason


def is_valid_close_code(code: int) -> bool:
    """Return whether ``code`` may appear in a CLOSE frame on the wire."""
    if _FIRST_DEFINED_CODE <= code <= _LAST_DEFINED_CODE:
        return code not in _NEVER_SENT_CODES
    return _FIRST_APP_CODE <= code <= _LAST_APP_CODE


def _close_payload(code: int, reason: str) -> bytes:
    """Build a CLOSE payload: a 2-byte big-endian code, then the UTF-8 reason.

    Raises:
        ValueError: If ``code`` may not be sent or ``reason`` is too long.

    """
    if not is_valid_close_code(code):
        message = f"close code {code} may not be sent"
        raise ValueError(message)
    encoded = reason.encode()
    if len(encoded) > _MAX_REASON_BYTES:
        message = f"close reason is {len(encoded)} bytes, over {_MAX_REASON_BYTES}"
        raise ValueError(message)
    return code.to_bytes(_CODE_SIZE, "big") + encoded


def _parse_close(payload: bytes) -> tuple[int, str]:
    """Return the ``(code, reason)`` in a received CLOSE payload.

    Raises:
        _Failure: For a 1-byte payload, a forbidden code or a non-UTF-8 reason.

    """
    if not payload:
        return CLOSE_NO_STATUS, ""
    if len(payload) < _CODE_SIZE:
        raise _Failure(CLOSE_PROTOCOL_ERROR, "close payload of one byte")
    code = int.from_bytes(payload[:_CODE_SIZE], "big")
    if not is_valid_close_code(code):
        raise _Failure(CLOSE_PROTOCOL_ERROR, f"invalid close code {code}")
    try:
        reason = payload[_CODE_SIZE:].decode("utf-8")
    except UnicodeDecodeError:
        raise _Failure(CLOSE_INVALID_UTF8, "close reason is not UTF-8") from None
    return code, reason


class WebSocket:
    """One side of an open WebSocket conversation.

    Sends are serialised with a lock so a pong or a CLOSE can never land
    in the middle of another frame's bytes. Only one task may ``recv()``
    at a time. Once the conversation ends, every ``recv()`` and send
    raises the same ``WebSocketClosed``.

    Args:
        reader: The stream the peer's frames arrive on.
        writer: The stream our frames go out on.
        is_client: True to mask outgoing frames (and expect unmasked
            ones); False for the server side.
        max_message_size: The largest reassembled message accepted, in bytes.

    """

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        *,
        is_client: bool,
        max_message_size: int = DEFAULT_WS_MAX_MESSAGE,
    ) -> None:
        """Wrap streams whose handshake has already been done."""
        self._frames = FrameReader(reader, expect_masked=not is_client)
        self._writer = writer
        self._is_client = is_client
        self._max_message_size = max_message_size
        self._send_lock = Lock()
        self._close_sent = False
        self._receiving = False
        self._ended: WebSocketClosed | None = None
        self._done = Event()
        # The message being reassembled, kept here (not in ``recv``'s
        # locals) so a ``recv()`` cancelled between fragments loses nothing.
        self._kind: Opcode | None = None
        self._parts: list[bytes] = []
        self._size = 0

    @property
    def closed(self) -> bool:
        """Return whether the conversation is over."""
        return self._ended is not None

    @property
    def close_code(self) -> int | None:
        """Return the code the conversation ended with, or None while open."""
        return None if self._ended is None else self._ended.code

    async def send_text(self, text: str) -> None:
        """Send ``text`` as one TEXT message.

        Raises:
            WebSocketClosed: If the conversation is over or closing.

        """
        await self._send_data(Opcode.TEXT, text.encode())

    async def send_bytes(self, data: bytes) -> None:
        """Send ``data`` as one BINARY message.

        Raises:
            WebSocketClosed: If the conversation is over or closing.

        """
        await self._send_data(Opcode.BINARY, data)

    async def ping(self, data: bytes = b"") -> None:
        """Send a PING; the peer's PONG is skipped by ``recv()``.

        Raises:
            WebSocketClosed: If the conversation is over or closing.
            ProtocolError: If ``data`` is over 125 bytes.

        """
        await self._send_data(Opcode.PING, data)

    async def recv(self) -> str | bytes:
        """Return the next whole message: ``str`` for TEXT, ``bytes`` for BINARY.

        Pings are answered and pongs skipped along the way. Safe to
        cancel (for example with ``wait_for``): a frame or a fragmented
        message cut off by the timeout is finished by the next ``recv()``.

        Raises:
            WebSocketClosed: When a CLOSE arrives (after replying to it),
                the peer breaks the protocol (after closing with the
                matching code), or the connection drops (code 1006).
            RuntimeError: If another task is already in ``recv()``.

        """
        if self._ended is not None:
            raise self._ended
        if self._receiving:
            message = "recv() is already running in another task"
            raise RuntimeError(message)
        self._receiving = True
        try:
            return await self._receive_message()
        except _Failure as failure:
            raise await self._fail(failure.code, failure.reason) from None
        finally:
            self._receiving = False

    async def close(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        """Say goodbye, wait (up to ``CLOSE_TIMEOUT``) for the reply, hang up.

        Calling it again, or after the peer closed, does nothing. If the
        peer never answers, the conversation ends with code 1006.

        Raises:
            ValueError: If ``code`` may not be sent or ``reason`` is over
                123 bytes of UTF-8.

        """
        if self._ended is not None:
            return
        payload = _close_payload(code, reason)
        try:
            # One deadline covers sending our goodbye *and* hearing theirs:
            # the CLOSE may queue behind a big send the peer is not reading.
            async with timeout(CLOSE_TIMEOUT):
                if not self._close_sent:
                    self._close_sent = True
                    await self._write(Opcode.CLOSE, payload)
                await self._await_peer_close()
        except ConnectionClosedError:
            pass  # the peer is gone: we hang up below either way
        except TimeoutError:
            self._writer.abort()  # silent or stuck: drop the line (wakes a stuck drain)
        finally:
            self._finish(CLOSE_ABNORMAL, "")

    async def _await_peer_close(self) -> None:
        """Wait for the peer's CLOSE, reading (and dropping) messages if nobody else is."""
        if self._receiving:
            await self._done.wait()
            return
        while True:
            await self.recv()

    async def _receive_message(self) -> str | bytes:
        """Read frames until one whole data message is complete."""
        while True:
            room = max(self._max_message_size - self._size, MAX_CONTROL_PAYLOAD)
            frame = await self._read_frame(room)
            if frame.opcode is Opcode.CLOSE:
                raise await self._on_close(frame.payload)
            if frame.opcode is Opcode.PING:
                if not self._close_sent:
                    try:
                        await self._reply(Opcode.PONG, frame.payload)
                    except ConnectionClosedError:
                        raise self._finish(CLOSE_ABNORMAL, "") from None
                continue
            if frame.opcode is Opcode.PONG:
                continue
            if frame.opcode is Opcode.CONTINUATION:
                if self._kind is None:
                    raise _Failure(CLOSE_PROTOCOL_ERROR, "continuation with no message")
            elif self._kind is not None:
                raise _Failure(CLOSE_PROTOCOL_ERROR, "new message before the last finished")
            else:
                self._kind = frame.opcode
            self._size += len(frame.payload)
            if self._size > self._max_message_size:
                raise _Failure(CLOSE_TOO_BIG, "message too big")
            self._parts.append(frame.payload)
            if frame.fin:
                kind, data = self._kind, b"".join(self._parts)
                self._kind, self._parts, self._size = None, [], 0
                return self._assemble(kind, data)

    @staticmethod
    def _assemble(kind: Opcode, data: bytes) -> str | bytes:
        """Turn a reassembled payload into the message the caller sees."""
        if kind is Opcode.BINARY:
            return data
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            raise _Failure(CLOSE_INVALID_UTF8, "text message is not UTF-8") from None

    async def _read_frame(self, max_payload: int) -> Frame:
        """Read one frame, turning stream and protocol errors into endings."""
        try:
            return await self._frames.read(max_payload)
        except FrameTooLargeError as error:
            raise _Failure(CLOSE_TOO_BIG, str(error)) from None
        except ProtocolError as error:
            raise _Failure(CLOSE_PROTOCOL_ERROR, str(error)) from None
        except ConnectionClosedError:
            raise self._finish(CLOSE_ABNORMAL, "") from None

    async def _on_close(self, payload: bytes) -> WebSocketClosed:
        """Reply to the peer's CLOSE (unless we said goodbye first) and end."""
        code, reason = _parse_close(payload)
        echo = b"" if code == CLOSE_NO_STATUS else code.to_bytes(_CODE_SIZE, "big")
        await self._say_goodbye(echo)
        return self._finish(code, reason)

    async def _fail(self, code: int, reason: str) -> WebSocketClosed:
        """Close with ``code`` after a violation and return the ending."""
        await self._say_goodbye(_close_payload(code, ""))
        return self._finish(code, reason)

    async def _say_goodbye(self, payload: bytes) -> None:
        """Send our CLOSE frame unless one has already gone out.

        A peer that has already hung up cannot hear it, and that is fine:
        the conversation is ending either way.
        """
        if self._close_sent:
            return
        self._close_sent = True
        with suppress(ConnectionClosedError):
            await self._reply(Opcode.CLOSE, payload)

    def _finish(self, code: int, reason: str) -> WebSocketClosed:
        """End the conversation (once), close the stream, return the ending."""
        if self._ended is None:
            self._ended = WebSocketClosed(code, reason)
            self._writer.close()
            self._done.set()
        return self._ended

    async def _send_data(self, opcode: Opcode, payload: bytes) -> None:
        """Send a frame the application asked for, refusing once closing."""
        if self._ended is not None:
            raise self._ended
        if self._close_sent:
            message = "close() has already been called"
            raise WebSocketClosed(CLOSE_NORMAL, message)
        await self._write_or_end(opcode, payload)

    async def _write_or_end(self, opcode: Opcode, payload: bytes) -> None:
        """Send a frame; if the stream is gone, end with 1006 and raise."""
        try:
            await self._write(opcode, payload)
        except ConnectionClosedError:
            raise self._finish(CLOSE_ABNORMAL, "") from None

    async def _reply(self, opcode: Opcode, payload: bytes) -> None:
        """Send a control reply (PONG or CLOSE), giving up after ``CLOSE_TIMEOUT``.

        The reply may have to queue behind a big send that the peer is not
        reading. Waiting forever would freeze ``recv()``, so after
        ``CLOSE_TIMEOUT`` we drop the line (which also wakes that stuck send).

        Raises:
            ConnectionClosedError: If the stream is gone or the reply timed out.

        """
        try:
            async with timeout(CLOSE_TIMEOUT):
                await self._write(opcode, payload)
        except TimeoutError:
            self._writer.abort()
            message = f"could not send {opcode.name} within {CLOSE_TIMEOUT:g}s"
            raise ConnectionClosedError(message) from None

    async def _write(self, opcode: Opcode, payload: bytes) -> None:
        """Encode one final frame (masked if we are the client) and send it."""
        key = os.urandom(MASK_KEY_SIZE) if self._is_client else None
        data = encode_ws_frame(Frame(True, opcode, payload), mask_key=key)
        async with self._send_lock:
            self._writer.write(data)
            await self._writer.drain()
