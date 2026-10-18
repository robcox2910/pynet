"""A pretend pipe -- bytes arrive only in the chunks the test hands over.

On a real connection the network decides where the byte river is cut:
one ``recv`` might bring half a header, the next three whole frames and a
bit. Property tests want to choose those cuts themselves, and must never
use real sockets, threads or sleeps.

``PretendPipe`` stands in for both the file descriptor and the event loop
behind a ``StreamReader``. Each time the reader asks for more bytes it
gets exactly one chunk from the queue, so a test can split the same bytes
in any way it likes. Nothing ever waits: if the reader wants bytes the
test hasn't handed over yet, its coroutine simply stops there, and
``FrameInbox`` resumes it after the next ``hand_over``.

The real ``StreamReader`` and ``FramedConnection`` code runs unchanged;
only the pipe underneath is pretend.
"""

from collections import deque
from collections.abc import Callable, Coroutine
from typing import Any, cast

from pynet.errors import ConnectionClosedError
from pynet.framing import FramedConnection
from pynet.loop import Future
from pynet.streams import StreamReader, StreamWriter, _Transport

PRETEND_FD = -1


class PretendPipe:
    """Hand a ``StreamReader`` one queued chunk each time it asks for bytes."""

    closed = False
    close_on_eof = False
    fd = PRETEND_FD

    def __init__(self) -> None:
        """Start with no chunks and the far end still open."""
        self.reader: StreamReader | None = None
        self._chunks: deque[bytes] = deque()
        self._hung_up = False
        self._on_readable: Callable[[], None] | None = None

    # --- What the test calls ---------------------------------------------

    def hand_over(self, chunk: bytes) -> None:
        """Queue one non-empty chunk, waking the reader if it is waiting."""
        self._chunks.append(chunk)
        self._wake_reader()

    def hang_up(self) -> None:
        """Close the far end: once the queue is empty, the reader sees EOF."""
        self._hung_up = True
        self._wake_reader()

    def _wake_reader(self) -> None:
        """Tell a waiting reader that something has arrived."""
        callback, self._on_readable = self._on_readable, None
        if callback is not None:
            callback()

    # --- What the StreamReader calls (the fd and the event loop) ---------

    def add_reader(self, fd: int, callback: Callable[[], None]) -> None:
        """Deliver a chunk now if one is queued, otherwise remember who to wake."""
        del fd
        if self._chunks or self._hung_up:
            callback()
        else:
            self._on_readable = callback

    def remove_reader(self, fd: int) -> None:
        """Stop waking the reader."""
        del fd
        self._on_readable = None

    def recv(self, size: int) -> bytes:
        """Return the next chunk whole, or ``b""`` (EOF) once hung up and empty."""
        del size
        return self._chunks.popleft() if self._chunks else b""

    @property
    def loop(self) -> PretendPipe:
        """Act as the reader's event loop too."""
        return self


class _NoWriter:
    """A writer for an inbox that only ever reads; ``abort`` does nothing."""

    def abort(self) -> None:
        """Ignore the request: there is no connection to cut."""


def pretend_reader(pipe: PretendPipe) -> StreamReader:
    """Return a real ``StreamReader`` that reads from ``pipe``."""
    return StreamReader(cast("_Transport", pipe))


def run_now[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """Run ``coroutine`` to the end, which must happen without waiting.

    Raises:
        AssertionError: If it stopped to wait for bytes that were never handed over.

    """
    try:
        coroutine.send(None)
    except StopIteration as finished:
        return cast("T", finished.value)
    coroutine.close()
    message = "the reader is still waiting for bytes"
    raise AssertionError(message)


class FrameInbox:
    """The receiving end of a framed connection, fed through a ``PretendPipe``.

    Args:
        max_frame_size: The largest frame the connection accepts.

    """

    def __init__(self, max_frame_size: int) -> None:
        """Wire a real ``FramedConnection`` to a fresh pretend pipe."""
        self.pipe = PretendPipe()
        self.connection = FramedConnection(
            pretend_reader(self.pipe),
            cast("StreamWriter", _NoWriter()),
            max_frame_size=max_frame_size,
        )
        self._receiving: Coroutine[Any, Any, bytes] | None = None
        self._waiting_on: Future[Any] | None = None
        self.goodbye: str | None = None

    def frames(self) -> list[bytes]:
        """Return every whole frame the bytes handed over so far complete.

        A frame cut off at the end stays half-read inside the connection,
        and is finished by a later call once the rest is handed over. Once
        the pipe has hung up and every frame is read, the connection's
        ``ConnectionClosedError`` message is kept in ``goodbye`` (it says
        whether the pipe closed between frames or in the middle of one).
        """
        frames: list[bytes] = []
        while self.goodbye is None and (self._waiting_on is None or self._waiting_on.done()):
            if self._receiving is None:
                self._receiving = self.connection.recv_frame()
            try:
                self._waiting_on = self._receiving.send(None)
            except StopIteration as finished:
                frames.append(cast("bytes", finished.value))
                self._receiving = self._waiting_on = None
            except ConnectionClosedError as closed:
                self.goodbye = str(closed)
        return frames


def read_until_hang_up(inbox: FrameInbox) -> tuple[list[bytes], str | None]:
    """Hang up the pipe, then return every frame left and the connection's goodbye."""
    inbox.pipe.hang_up()
    frames = inbox.frames()
    return frames, inbox.goodbye
