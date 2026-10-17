"""Envelopes with the page count on the front -- know where each message ends.

TCP is a river of bytes with no gaps: two messages sent back to back can
arrive glued together, and one message can arrive in pieces. Framing
fixes that by writing the size on the front of every message, like an
envelope marked "3 pages inside". The reader first reads the 4-byte size,
then exactly that many bytes, and always knows where one message stops
and the next begins.

Wire format: ``[4-byte big-endian unsigned length][payload]``.

Rules worth knowing:

- A header claiming more than ``max_frame_size`` is refused before any of
  the body is read (otherwise a liar could make us buffer gigabytes): the
  connection is aborted and ``FrameTooLargeError`` is raised.
- The peer closing exactly between frames is a polite goodbye; closing
  halfway through a frame is a truncated message. Both raise
  ``ConnectionClosedError`` (the second says "mid-frame").
- ``recv_frame`` is safe to cancel (for example by a timeout). If the
  header has arrived but the body has not, the connection remembers the
  size it read, so the next ``recv_frame`` carries on with the body
  instead of mistaking body bytes for a new header.
- ``send_frame`` is not safe to call from several tasks at once: a frame
  that needs several writes could be interleaved with another. Callers
  that share a connection must serialise senders, for example with a
  ``Lock``.
"""

import struct

from pynet.errors import ConnectionClosedError, FrameTooLargeError
from pynet.streams import IncompleteReadError, StreamReader, StreamWriter

HEADER_SIZE = 4
MAX_FRAME_SIZE = 16 * 1024 * 1024
HEADER_FORMAT = ">I"
PEER_CLOSED_MESSAGE = "peer closed"
MID_FRAME_MESSAGE = "peer closed mid-frame"


def encode_frame(payload: bytes, *, max_size: int = MAX_FRAME_SIZE) -> bytes:
    """Return ``payload`` with its 4-byte big-endian length in front.

    Args:
        payload: The message bytes.
        max_size: The largest payload allowed.

    Returns:
        The header followed by the payload.

    Raises:
        FrameTooLargeError: If ``payload`` is longer than ``max_size``.

    """
    if len(payload) > max_size:
        raise FrameTooLargeError(len(payload), max_size)
    return struct.pack(HEADER_FORMAT, len(payload)) + payload


class FramedConnection:
    """Send and receive whole messages over a reader/writer pair.

    Args:
        reader: Where incoming bytes arrive.
        writer: Where outgoing bytes go.
        max_frame_size: The largest payload accepted or sent.

    """

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        *,
        max_frame_size: int = MAX_FRAME_SIZE,
    ) -> None:
        """Wrap ``reader`` and ``writer`` with a frame size limit."""
        self._reader = reader
        self._writer = writer
        self._max_frame_size = max_frame_size
        self._error: ConnectionClosedError | None = None
        self._pending: tuple[bytes, int] | None = None  # (header, size) awaiting a body

    @property
    def writer(self) -> StreamWriter:
        """The underlying writer."""
        return self._writer

    @property
    def max_frame_size(self) -> int:
        """The largest payload this connection sends or accepts."""
        return self._max_frame_size

    async def send_frame(self, payload: bytes) -> None:
        """Write one frame and wait for backpressure to clear.

        Not safe for concurrent callers; serialise them (for example with
        a ``Lock``).

        Args:
            payload: The message bytes.

        Raises:
            FrameTooLargeError: If ``payload`` exceeds the limit.
            ConnectionClosedError: If the connection is closed.

        """
        self._writer.write(encode_frame(payload, max_size=self._max_frame_size))
        await self._writer.drain()

    async def recv_frame(self) -> bytes:
        """Read one whole frame and return its payload.

        Safe to cancel: a frame cut off by a timeout is finished by the
        next call. Safe to call again after the connection closed: it
        keeps raising ``ConnectionClosedError``.

        Returns:
            The payload (possibly empty).

        Raises:
            ConnectionClosedError: If the peer closed, at a frame boundary
                ("peer closed") or inside a frame ("mid-frame").
            FrameTooLargeError: If the header claims more than the limit;
                the connection is aborted and the body is never read.

        """
        if self._error is not None:
            raise self._error
        if self._pending is None:
            try:
                header = await self._reader.read_exactly(HEADER_SIZE)
            except IncompleteReadError as error:
                raise self._remember(error.partial) from error
            (size,) = struct.unpack(HEADER_FORMAT, header)
            if size > self._max_frame_size:
                self._writer.abort()
                self._error = ConnectionClosedError(f"connection aborted: frame of {size} bytes")
                raise FrameTooLargeError(size, self._max_frame_size)
            # Remember the header: if we are cancelled while waiting for
            # the body, the next call must carry on with this frame.
            self._pending = (header, size)
        header, size = self._pending
        if size == 0:
            self._pending = None
            return b""
        try:
            payload = await self._reader.read_exactly(size)
        except IncompleteReadError as error:
            raise self._remember(error.partial or header) from error
        self._pending = None
        return payload

    def _remember(self, partial: bytes) -> ConnectionClosedError:
        """Record and return the error for an EOF after ``partial`` bytes."""
        message = MID_FRAME_MESSAGE if partial else PEER_CLOSED_MESSAGE
        self._error = ConnectionClosedError(message)
        return self._error

    def close(self) -> None:
        """Close the connection once buffered bytes are sent."""
        self._writer.close()

    def abort(self) -> None:
        """Close at once, throwing away anything not yet sent (safe to repeat)."""
        self._writer.abort()

    async def wait_closed(self) -> None:
        """Wait until the connection is fully closed."""
        await self._writer.wait_closed()
