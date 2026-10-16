"""Water pipes with a tap -- bytes flow in, and you only fill what you can carry.

A stream is a water pipe between two programs. The ``StreamReader`` is
the tap at your end: you open it (``read``) when you want some water,
and the loop only watches the pipe while the tap is open. The
``StreamWriter`` is the hose you pour into: the kernel takes what it can
right away, the rest waits in a bucket (our buffer), and ``drain()`` is
the rule "don't overfill the bucket" -- it makes you wait while the
bucket is above the high-water line until it has emptied below the
low-water line. That pause is called **backpressure**.

The same reader and writer work for TCP sockets (``open_connection``,
``start_server``) and for OS pipes (``pipe_reader``, ``pipe_writer``).

Rules worth knowing:

- One fd, one transport: a TCP socket's reader and writer share a single
  selector registration. Closing the writer closes the whole socket (a
  full close; half-close / ``write_eof`` is not in scope), and the reader
  then sees EOF.
- A peer reset (``ECONNRESET``/``EPIPE``) is recorded by the transport.
  The ``write`` that discovers it returns quietly; the next ``drain()``
  or ``write()`` raises ``ConnectionClosedError``. A reader sees a reset
  as EOF (bytes already buffered are still delivered).
- A pipe reader owns its fd and closes it when it reaches EOF (or on
  ``close()``); a pipe writer owns its fd and closes it on ``close()``.
"""

import contextlib
import errno
import ipaddress
import os
import socket
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any

from pynet.errors import ConnectionClosedError, DNSError, NetConnectionError, PyNetError
from pynet.loop.futures import Cancelled, Future
from pynet.loop.reactor import ErrorContext, EventLoop
from pynet.loop.tasks import Task, get_running_loop
from pynet.loop.threads import run_in_thread

DEFAULT_LIMIT = 64 * 1024
DEFAULT_HIGH_WATER = 64 * 1024
DEFAULT_LOW_WATER = 16 * 1024
READ_CHUNK = 64 * 1024
LISTEN_BACKLOG = 100
IPV6 = 6
ACCEPT_BATCH = 16
CONNECT_IN_PROGRESS = frozenset({errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EAGAIN})
HANDLER_ERROR_MESSAGE = "Exception in connection handler"
ACCEPT_ERROR_MESSAGE = "Error accepting a connection"
CLOSED_MESSAGE = "connection is closed"
LOST_MESSAGE = "connection closed before the buffered data was sent"

type Handler = Callable[[StreamReader, StreamWriter], Awaitable[None]]
type _AddrInfo = tuple[int, int, int, tuple[Any, ...]]


class IncompleteReadError(PyNetError):
    """Raise when EOF arrives before a read got all the bytes it needed.

    Attributes:
        partial: The bytes that did arrive before EOF.
        expected: How many bytes would have completed the read (for
            ``read_until`` that is the partial bytes plus the separator).

    """

    def __init__(self, partial: bytes, expected: int) -> None:
        """Record the ``partial`` bytes and the ``expected`` total."""
        super().__init__(f"{len(partial)} bytes read before EOF, {expected} expected")
        self.partial = partial
        self.expected = expected


class LimitOverrunError(PyNetError):
    """Raise when ``read_until`` would need more than ``limit`` bytes.

    The buffered bytes are left in place, so the caller decides what to do.
    """


class _Transport:
    """Own one file descriptor, shared by its reader and writer.

    It knows how to move bytes (``recv``/``send`` for sockets,
    ``os.read``/``os.write`` for pipes), records a fatal connection
    error, and closes the fd exactly once.
    """

    def __init__(
        self,
        loop: EventLoop,
        fd: int,
        *,
        sock: socket.socket | None = None,
        close_on_eof: bool = False,
        extra: dict[str, object] | None = None,
    ) -> None:
        """Wrap ``fd`` (or ``sock``) for ``loop``."""
        self.loop = loop
        self.fd = fd
        self.sock = sock
        self.close_on_eof = close_on_eof
        self.extra: dict[str, object] = {"fd": fd, **(extra or {})}
        self.closed = False
        self.error: OSError | None = None
        self.reader: StreamReader | None = None
        self._close_waiters: list[Future[None]] = []
        self._close_callbacks: list[Callable[[], None]] = []

    def add_close_callback(self, callback: Callable[[], None]) -> None:
        """Call ``callback`` (once) when the transport closes, by any path."""
        self._close_callbacks.append(callback)

    def recv(self, size: int) -> bytes:
        """Read up to ``size`` bytes without blocking."""
        if self.sock is not None:
            return self.sock.recv(size)
        return os.read(self.fd, size)

    def send(self, data: memoryview) -> int:
        """Write as much of ``data`` as the kernel takes; return how much."""
        if self.sock is not None:
            return self.sock.send(data)
        return os.write(self.fd, data)

    def fail(self, error: OSError) -> None:
        """Record a connection-ending ``error`` and close."""
        if self.error is None:
            self.error = error
        self.close()

    def closed_error(self) -> ConnectionClosedError:
        """Build the error to raise when someone uses a dead connection."""
        if self.error is not None:
            error = ConnectionClosedError(f"connection lost: {self.error}")
            error.__cause__ = self.error
            return error
        return ConnectionClosedError(CLOSED_MESSAGE)

    def close(self) -> None:
        """Stop watching the fd, close it, and tell the reader and waiters."""
        if self.closed:
            return
        self.closed = True
        self.loop.remove_reader(self.fd)
        self.loop.remove_writer(self.fd)
        if self.sock is not None:
            self.sock.close()
        else:
            os.close(self.fd)
        if self.reader is not None:
            self.reader.feed_eof()
        callbacks, self._close_callbacks = self._close_callbacks, []
        for callback in callbacks:
            callback()
        waiters, self._close_waiters = self._close_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self) -> None:
        """Wait until ``close`` has run (each caller gets its own future)."""
        if self.closed:
            return
        waiter: Future[None] = Future(self.loop)
        self._close_waiters.append(waiter)
        await waiter


class StreamReader:
    """Read bytes from a transport, asking the loop to watch only while waiting.

    Args:
        transport: The fd this reader drains.

    """

    def __init__(self, transport: _Transport) -> None:
        """Start with an empty buffer, attached to ``transport``."""
        self._transport = transport
        self._buffer = bytearray()
        self._eof = False
        self._error: OSError | None = None
        self._waiter: Future[None] | None = None
        transport.reader = self

    def at_eof(self) -> bool:
        """Return whether EOF was seen and the buffer is empty."""
        return self._eof and not self._buffer

    def feed_eof(self) -> None:
        """Mark the end of the stream and wake a waiting read."""
        self._eof = True
        self._wake()

    def close(self) -> None:
        """Turn the tap off for good: stop watching and close the fd.

        Use it for a pipe nobody is going to read to the end (for example
        a child's stderr). A waiting read wakes up and sees EOF; bytes
        already buffered can still be read. For a TCP socket this closes
        the whole socket, writer included. Closing twice is harmless.
        """
        self._transport.close()

    async def read(self, n: int = -1) -> bytes:
        """Return up to ``n`` bytes, or everything up to EOF when ``n`` is negative.

        Returns ``b""`` at EOF (and for ``n == 0``).
        """
        if n == 0:
            return b""
        if n < 0:
            while not self._eof:
                await self._wait_for_data()
            return self._take(len(self._buffer))
        while not self._buffer and not self._eof:
            await self._wait_for_data()
        return self._take(n)

    async def read_exactly(self, n: int) -> bytes:
        """Return exactly ``n`` bytes.

        Raises:
            IncompleteReadError: If EOF arrives first; ``.partial`` holds
                what did arrive.

        """
        while len(self._buffer) < n:
            if self._eof:
                raise IncompleteReadError(self._take(len(self._buffer)), n)
            await self._wait_for_data()
        return self._take(n)

    async def read_until(self, sep: bytes = b"\n", *, limit: int = DEFAULT_LIMIT) -> bytes:
        """Return bytes up to and including ``sep``.

        Raises:
            LimitOverrunError: If the answer would be longer than ``limit``
                bytes; the buffer is left untouched.
            IncompleteReadError: If EOF arrives before ``sep``.

        """
        start = 0
        while True:
            index = self._buffer.find(sep, start)
            if index >= 0:
                end = index + len(sep)
                if end > limit:
                    message = f"separator found after {end} bytes, over the limit of {limit}"
                    raise LimitOverrunError(message)
                return self._take(end)
            if len(self._buffer) > limit:
                message = f"no separator within the limit of {limit} bytes"
                raise LimitOverrunError(message)
            if self._eof:
                partial = self._take(len(self._buffer))
                raise IncompleteReadError(partial, len(partial) + len(sep))
            start = max(0, len(self._buffer) - len(sep) + 1)
            await self._wait_for_data()

    def _take(self, n: int) -> bytes:
        """Remove and return up to ``n`` bytes from the buffer (raising a read error)."""
        if not self._buffer and self._error is not None:
            error, self._error = self._error, None
            raise error
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data

    async def _wait_for_data(self) -> None:
        """Watch the fd until more bytes (or EOF) arrive.

        The watch is added here and removed in ``finally`` -- so a read
        that is cancelled (for example by a timeout) never leaves a stale
        registration in the selector.

        Raises:
            RuntimeError: If another read is already waiting.

        """
        if self._waiter is not None:
            message = "StreamReader: another read is already waiting"
            raise RuntimeError(message)
        transport = self._transport
        if transport.closed:
            self._eof = True
            return
        waiter: Future[None] = Future(transport.loop)
        self._waiter = waiter
        transport.loop.add_reader(transport.fd, self._on_readable)
        try:
            await waiter
        finally:
            self._waiter = None
            if not transport.closed:
                transport.loop.remove_reader(transport.fd)

    def _on_readable(self) -> None:
        """Pull one chunk into the buffer and wake the waiting read."""
        transport = self._transport
        try:
            data = transport.recv(READ_CHUNK)
        except BlockingIOError, InterruptedError:
            return
        except ConnectionError as error:
            transport.fail(error)  # a reset reads as EOF; the writer raises
            return
        except OSError as error:
            self._error = error
            self._eof = True
            self._wake()
            return
        if data:
            self._buffer.extend(data)
            self._wake()
            return
        self.feed_eof()
        if transport.close_on_eof:
            transport.close()

    def _wake(self) -> None:
        """Fill the waiting read's future and stop watching the fd."""
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
            if not self._transport.closed:
                self._transport.loop.remove_reader(self._transport.fd)


class StreamWriter:
    """Write bytes to a transport, buffering what the kernel cannot take yet.

    Args:
        transport: The fd this writer fills.
        high_water: ``drain()`` waits while more than this is buffered...
        low_water: ...until the buffer falls to this or below.

    """

    def __init__(
        self,
        transport: _Transport,
        *,
        high_water: int = DEFAULT_HIGH_WATER,
        low_water: int = DEFAULT_LOW_WATER,
    ) -> None:
        """Start with an empty bucket, attached to ``transport``."""
        self._transport = transport
        self._high_water = high_water
        self._low_water = low_water
        self._buffer = bytearray()
        self._closing = False
        self._watching = False
        self._drain_waiters: list[Future[None]] = []
        self._lost = False
        transport.add_close_callback(self._on_closed)

    def write(self, data: bytes) -> None:
        """Send ``data`` now if possible and buffer the rest.

        Raises:
            ConnectionClosedError: If the writer is closed, or an earlier
                write found that the peer had reset the connection.

        """
        transport = self._transport
        if self._closing or transport.closed:
            raise transport.closed_error()
        if not data:
            return
        if self._buffer:
            self._buffer.extend(data)
            return
        try:
            sent = transport.send(memoryview(data))
        except BlockingIOError, InterruptedError:
            sent = 0
        except OSError as error:
            self._fail(error)
            return
        if sent < len(data):
            self._buffer.extend(memoryview(data)[sent:])
            self._watch()

    async def drain(self) -> None:
        """Wait while the buffer is over the high-water mark.

        Once over, waits until the buffer falls to the low-water mark.

        Raises:
            ConnectionClosedError: If the peer reset the connection, or the
                connection closed (for example by ``abort()``) while
                buffered bytes were still waiting to be sent.

        """
        transport = self._transport
        if transport.error is not None:
            raise transport.closed_error()
        if len(self._buffer) <= self._high_water or transport.closed:
            return
        waiter: Future[None] = Future(transport.loop)
        self._drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            if waiter in self._drain_waiters:
                self._drain_waiters.remove(waiter)
        if transport.error is not None:
            raise transport.closed_error()
        if self._lost:
            raise ConnectionClosedError(LOST_MESSAGE)

    def close(self) -> None:
        """Close once the buffer has been flushed (closes the whole socket)."""
        if self._closing:
            return
        self._closing = True
        if not self._buffer:
            self._transport.close()

    def is_closing(self) -> bool:
        """Return whether ``close`` was called or the transport is gone."""
        return self._closing or self._transport.closed

    async def wait_closed(self) -> None:
        """Wait until the underlying fd has been closed."""
        await self._transport.wait_closed()

    def get_extra_info(self, name: str, default: object = None) -> object:
        """Return ``"peername"``, ``"sockname"`` or ``"fd"``, else ``default``."""
        return self._transport.extra.get(name, default)

    def abort(self) -> None:
        """Throw away anything still buffered and close the fd immediately."""
        self._closing = True
        self._transport.close()

    def _watch(self) -> None:
        """Ask the loop to tell us when the fd can take more bytes."""
        if not self._watching:
            self._watching = True
            self._transport.loop.add_writer(self._transport.fd, self._on_writable)

    def _unwatch(self) -> None:
        """Stop the writable watch."""
        if self._watching:
            self._watching = False
            if not self._transport.closed:
                self._transport.loop.remove_writer(self._transport.fd)

    def _on_writable(self) -> None:
        """Send more of the buffer; close or wake drainers as it empties."""
        transport = self._transport
        if transport.closed:
            self._unwatch()
            return
        try:
            with memoryview(self._buffer) as view:
                sent = transport.send(view)
        except BlockingIOError, InterruptedError:
            return
        except OSError as error:
            self._fail(error)
            return
        del self._buffer[:sent]
        if len(self._buffer) <= self._low_water:
            self._wake_drainers()
        if not self._buffer:
            self._unwatch()
            if self._closing:
                transport.close()

    def _fail(self, error: OSError) -> None:
        """Record a broken connection (``_on_closed`` then tidies up)."""
        self._transport.fail(error)

    def _on_closed(self) -> None:
        """Drop the buffer and wake drainers, whoever closed the transport.

        The reader may be the one that finds a reset, or ``abort()`` may
        close mid-drain: either way a pending ``drain()`` must not hang.
        The transport has already removed the writable watch.
        """
        self._watching = False
        if self._buffer:
            self._lost = True
            self._buffer.clear()
        self._wake_drainers()

    def _wake_drainers(self) -> None:
        """Wake every task waiting in ``drain``."""
        waiters, self._drain_waiters = self._drain_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)


def _socket_streams(loop: EventLoop, sock: socket.socket) -> tuple[StreamReader, StreamWriter]:
    """Build a reader and writer sharing one transport for ``sock``."""
    sock.setblocking(False)
    if sock.family in {socket.AF_INET, socket.AF_INET6}:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    extra: dict[str, object] = {"sockname": sock.getsockname()}
    with contextlib.suppress(OSError):  # not connected: no peer name to report
        extra["peername"] = sock.getpeername()
    transport = _Transport(loop, sock.fileno(), sock=sock, extra=extra)
    return StreamReader(transport), StreamWriter(transport)


async def _resolve(host: str, port: int) -> list[_AddrInfo]:
    """Return ``(family, type, proto, sockaddr)`` choices for ``host:port``.

    IP literals are used as they are; names are looked up with
    ``getaddrinfo`` on a helper thread so the loop never blocks.

    Raises:
        DNSError: If the name cannot be resolved.

    """
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        family = socket.AF_INET6 if address.version == IPV6 else socket.AF_INET
        return [(family, socket.SOCK_STREAM, socket.IPPROTO_TCP, (host, port))]
    try:
        infos = await run_in_thread(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        message = f"Could not resolve {host!r}: {error}"
        raise DNSError(message) from error
    return [(family, kind, proto, address) for family, kind, proto, _, address in infos]


async def _connect(loop: EventLoop, sock: socket.socket, address: tuple[Any, ...]) -> None:
    """Connect ``sock`` without blocking: start, wait until writable, check.

    Raises:
        NetConnectionError: If the connection fails (for example, refused).

    """
    code = sock.connect_ex(address)
    if code in CONNECT_IN_PROGRESS:
        writable: Future[None] = Future(loop)

        def on_writable() -> None:
            """Fill the future once the handshake has finished."""
            if not writable.done():
                writable.set_result(None)

        loop.add_writer(sock.fileno(), on_writable)
        try:
            await writable
        finally:
            loop.remove_writer(sock.fileno())
        code = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if code != 0:
        message = f"Could not connect to {address}: {os.strerror(code)}"
        raise NetConnectionError(message)


async def open_connection(host: str, port: int) -> tuple[StreamReader, StreamWriter]:
    """Connect to ``host:port`` over TCP and return its reader and writer.

    Each address the name resolves to is tried in turn.

    Raises:
        DNSError: If ``host`` cannot be resolved.
        NetConnectionError: If no address accepts the connection.

    """
    loop = get_running_loop()
    last_error: NetConnectionError | None = None
    for family, kind, proto, address in await _resolve(host, port):
        sock = socket.socket(family, kind, proto)
        sock.setblocking(False)
        try:
            await _connect(loop, sock, address)
        except NetConnectionError as error:
            sock.close()
            last_error = error
            continue
        except BaseException:
            sock.close()
            raise
        return _socket_streams(loop, sock)
    message = f"Could not connect to {host}:{port}: {last_error}"
    raise NetConnectionError(message) from last_error


class Server:
    """Accept TCP connections and run a handler task for each one.

    ``close()`` stops accepting; connections already being handled carry
    on. ``wait_closed()`` waits only for the listening socket to close,
    not for handlers to finish.

    Args:
        loop: The loop to accept on.
        sock: A bound, listening, non-blocking socket.
        handler: The coroutine function to run for each connection.

    """

    def __init__(self, loop: EventLoop, sock: socket.socket, handler: Handler) -> None:
        """Start accepting on ``sock``."""
        self._loop = loop
        self._sock = sock
        self._handler = handler
        self.port: int = sock.getsockname()[1]
        self._transport = _Transport(loop, sock.fileno(), sock=sock)
        loop.add_reader(sock.fileno(), self._on_acceptable)

    def close(self) -> None:
        """Stop accepting and close the listening socket."""
        self._transport.close()

    async def wait_closed(self) -> None:
        """Wait until the listening socket has been closed."""
        await self._transport.wait_closed()

    async def __aenter__(self) -> Server:
        """Return the server for ``async with``."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the server and wait for the listening socket to close."""
        self.close()
        await self.wait_closed()

    def _on_acceptable(self) -> None:
        """Accept waiting connections and start a handler task for each."""
        for _ in range(ACCEPT_BATCH):
            try:
                conn, _address = self._sock.accept()
            except BlockingIOError, InterruptedError:
                return
            except OSError as error:
                self._loop.report_error(ErrorContext(ACCEPT_ERROR_MESSAGE, error))
                return
            reader, writer = _socket_streams(self._loop, conn)
            Task(self._serve(reader, writer), self._loop)

    async def _serve(self, reader: StreamReader, writer: StreamWriter) -> None:
        """Run the handler; report its exception; always close the connection."""
        try:
            await self._handler(reader, writer)
        except Cancelled:
            writer.abort()
            raise
        except Exception as error:  # noqa: BLE001 -- reported, not swallowed
            peer = writer.get_extra_info("peername")
            self._loop.report_error(ErrorContext(f"{HANDLER_ERROR_MESSAGE} for {peer}", error))
            writer.abort()
        except BaseException:
            writer.abort()
            raise
        else:
            writer.close()


async def start_server(handler: Handler, host: str, port: int) -> Server:
    """Listen on ``host:port`` and run ``handler(reader, writer)`` per connection.

    Port 0 lets the kernel choose; ``Server.port`` reports the real port.
    ``SO_REUSEADDR`` is set so a restarted server can rebind at once.

    Raises:
        DNSError: If ``host`` cannot be resolved.
        NetConnectionError: If the address cannot be bound.

    """
    loop = get_running_loop()
    family, kind, proto, address = (await _resolve(host, port))[0]
    sock = socket.socket(family, kind, proto)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.listen(LISTEN_BACKLOG)
        sock.setblocking(False)
    except OSError as error:
        sock.close()
        message = f"Could not listen on {host}:{port}: {error}"
        raise NetConnectionError(message) from error
    return Server(loop, sock, handler)


def pipe_reader(fd: int) -> StreamReader:
    """Wrap the read end of an OS pipe; the reader closes ``fd`` at EOF.

    Raises:
        RuntimeError: If no loop is running in this thread.

    """
    loop = get_running_loop()
    os.set_blocking(fd, False)
    return StreamReader(_Transport(loop, fd, close_on_eof=True))


def pipe_writer(fd: int) -> StreamWriter:
    """Wrap the write end of an OS pipe; ``close()`` closes ``fd``.

    Raises:
        RuntimeError: If no loop is running in this thread.

    """
    loop = get_running_loop()
    os.set_blocking(fd, False)
    return StreamWriter(_Transport(loop, fd))
