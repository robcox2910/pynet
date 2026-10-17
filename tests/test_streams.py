"""Test non-blocking TCP and pipe streams."""

import errno
import importlib.util
import os
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from pynet.errors import ConnectionClosedError, NetConnectionError
from pynet.loop import ErrorContext, Event, get_running_loop, run, sleep, spawn, wait_for
from pynet.loop.testing import FakeClock
from pynet.streams import (
    DEFAULT_HIGH_WATER,
    IncompleteReadError,
    LimitOverrunError,
    Server,
    StreamReader,
    StreamWriter,
    open_connection,
    pipe_reader,
    pipe_writer,
    start_server,
)

type Handler = Callable[[StreamReader, StreamWriter], Awaitable[None]]

HOST = "127.0.0.1"
HOSTNAME = "localhost"
ANY_PORT = 0
SHORT = 0.05
LONG = 2.0
TICK = 0.01
MAX_ATTEMPTS = 50
MESSAGE = b"hello\n"
PARTIAL = b"abc"
EXPECTED = 5
FIRST_LINE = b"one\n"
LONG_LINE = b"too-long-for-the-limit\n"
SMALL_LIMIT = 4
CUSTOM_SEP = b"||"
CUSTOM_PAYLOAD = b"left||right"
TAIL = b"no-separator"
GO = b"go\n"
LATE_BYTE = b"z"
CHUNK = b"x" * 1024
SMALL_BUFFER = 4096
BACKPRESSURE_FACTOR = 4
FIRST_CALL = 1
UNKNOWN_KEY = "no-such-key"
SENTINEL = "default"
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "echo_server.py"


class BoomError(Exception):
    """Signal a failure inside a connection handler."""


async def _echo_line(reader: StreamReader, writer: StreamWriter) -> None:
    """Read one line and send it back."""
    line = await reader.read_until()
    writer.write(line)
    await writer.drain()


def _serve_and[T](
    handler: Handler,
    client: Callable[[Server], Awaitable[T]],
    clock: Callable[[], float] | None = None,
) -> T:
    """Start a server running ``handler``, run ``client`` against it, return its result."""

    async def main() -> T:
        """Serve on an ephemeral port for the duration of the client."""
        async with await start_server(handler, HOST, ANY_PORT) as server:
            return await wait_for(client(server), LONG)

    return run(main(), clock=clock)


def _fd(writer: StreamWriter) -> int:
    """Return the file descriptor behind ``writer``."""
    fd = writer.get_extra_info("fd")
    assert isinstance(fd, int)
    return fd


def _shrink(writer: StreamWriter, option: int) -> None:
    """Set a small kernel buffer (``SO_SNDBUF``/``SO_RCVBUF``) on ``writer``'s socket."""
    dup = socket.socket(fileno=os.dup(_fd(writer)))
    try:
        dup.setsockopt(socket.SOL_SOCKET, option, SMALL_BUFFER)
    finally:
        dup.close()


async def _close(writer: StreamWriter) -> None:
    """Close ``writer`` and wait until its socket is closed."""
    writer.close()
    await writer.wait_closed()


class TestTcpStreams:
    """Test TCP clients and servers built on streams."""

    def test_echo_roundtrip(self) -> None:
        """Send a line to an echo server and read the same line back."""

        async def client(server: Server) -> bytes:
            """Write one line and read the echo."""
            reader, writer = await open_connection(HOST, server.port)
            writer.write(MESSAGE)
            await writer.drain()
            line = await reader.read_until()
            await _close(writer)
            return line

        assert _serve_and(_echo_line, client) == MESSAGE

    def test_server_port_reports_bound_port(self) -> None:
        """Report the port the kernel chose for port 0."""

        async def client(server: Server) -> int:
            """Return the server's port."""
            return server.port

        assert _serve_and(_echo_line, client) != ANY_PORT

    def test_open_connection_resolves_hostname(self) -> None:
        """Look up a host name in a helper thread, then connect."""

        async def client(server: Server) -> bytes:
            """Connect by name and round-trip a line."""
            reader, writer = await open_connection(HOSTNAME, server.port)
            writer.write(MESSAGE)
            line = await reader.read_until()
            await _close(writer)
            return line

        assert _serve_and(_echo_line, client) == MESSAGE

    def test_hostname_lookup_under_autojump_clock(self) -> None:
        """Wait for the helper-thread lookup instead of jumping past the deadline."""

        async def client(server: Server) -> bytes:
            """Connect by name and round-trip a line."""
            reader, writer = await open_connection(HOSTNAME, server.port)
            writer.write(MESSAGE)
            line = await reader.read_until()
            await _close(writer)
            return line

        assert _serve_and(_echo_line, client, FakeClock(autojump=True)) == MESSAGE

    def test_open_connection_refused_raises(self) -> None:
        """Raise NetConnectionError when nothing is listening."""
        with socket.socket() as probe:
            probe.bind((HOST, ANY_PORT))
            port = probe.getsockname()[1]

        async def main() -> None:
            """Connect to a closed port."""
            await open_connection(HOST, port)

        with pytest.raises(NetConnectionError, match="refused"):
            run(main())

    def test_server_close_stops_accepting(self) -> None:
        """Refuse new connections once the server is closed."""

        async def main() -> None:
            """Close the server, then try to connect."""
            server = await start_server(_echo_line, HOST, ANY_PORT)
            server.close()
            await server.wait_closed()
            await open_connection(HOST, server.port)

        with pytest.raises(NetConnectionError):
            run(main())

    def test_start_server_port_in_use_raises(self) -> None:
        """Raise NetConnectionError when another server holds the port."""

        async def client(server: Server) -> None:
            """Try to listen on the same port again."""
            with pytest.raises(NetConnectionError, match="Could not listen"):
                await start_server(_echo_line, HOST, server.port)

        _serve_and(_echo_line, client)

    def test_handler_exception_reported_server_survives(self) -> None:
        """Report a handler's exception, close its connection, keep accepting."""
        seen: list[ErrorContext] = []
        calls: list[int] = []

        async def handler(reader: StreamReader, writer: StreamWriter) -> None:
            """Fail on the first connection, echo on later ones."""
            calls.append(1)
            if len(calls) == FIRST_CALL:
                raise BoomError
            await _echo_line(reader, writer)

        async def client(server: Server) -> tuple[bytes, bytes]:
            """Connect twice; the first connection is dropped."""
            get_running_loop().set_exception_handler(seen.append)
            first_reader, first_writer = await open_connection(HOST, server.port)
            dropped = await first_reader.read()
            await _close(first_writer)
            reader, writer = await open_connection(HOST, server.port)
            writer.write(MESSAGE)
            line = await reader.read_until()
            await _close(writer)
            return dropped, line

        assert _serve_and(handler, client) == (b"", MESSAGE)
        assert [type(ctx.exception) for ctx in seen] == [BoomError]

    def test_get_extra_info(self) -> None:
        """Report the peer and local addresses and the file descriptor."""

        async def client(server: Server) -> None:
            """Inspect a fresh connection."""
            _, writer = await open_connection(HOST, server.port)
            assert writer.get_extra_info("peername") == (HOST, server.port)
            sockname = writer.get_extra_info("sockname")
            assert isinstance(sockname, tuple)
            assert sockname[0] == HOST
            assert _fd(writer) >= 0
            assert writer.get_extra_info(UNKNOWN_KEY, SENTINEL) == SENTINEL
            await _close(writer)

        _serve_and(_echo_line, client)

    def test_close_and_wait_closed(self) -> None:
        """Mark the writer closing and let the peer see EOF."""
        peer_saw: list[bytes] = []

        async def handler(reader: StreamReader, _writer: StreamWriter) -> None:
            """Record what arrives before EOF."""
            peer_saw.append(await reader.read())

        async def client(server: Server) -> bool:
            """Write, close, wait."""
            _, writer = await open_connection(HOST, server.port)
            assert not writer.is_closing()
            writer.write(MESSAGE)
            writer.close()
            writer.close()  # closing twice is harmless
            assert writer.is_closing()
            await writer.wait_closed()
            await writer.wait_closed()  # and so is waiting twice
            while not peer_saw:
                await sleep(TICK)
            return writer.is_closing()

        assert _serve_and(handler, client)
        assert peer_saw == [MESSAGE]


class TestReading:
    """Test the reader's read, read_exactly and read_until."""

    def test_read_exactly_incomplete_raises(self) -> None:
        """Raise IncompleteReadError carrying the partial bytes at EOF."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send three bytes then hang up."""
            writer.write(PARTIAL)

        async def client(server: Server) -> IncompleteReadError:
            """Ask for five bytes."""
            reader, writer = await open_connection(HOST, server.port)
            with pytest.raises(IncompleteReadError) as info:
                await reader.read_exactly(EXPECTED)
            await _close(writer)
            return info.value

        error = _serve_and(handler, client)
        assert error.partial == PARTIAL
        assert error.expected == EXPECTED

    def test_read_exactly_returns_exact_bytes(self) -> None:
        """Return exactly ``n`` bytes, leaving the rest buffered."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send a line."""
            writer.write(MESSAGE)

        async def client(server: Server) -> tuple[bytes, bytes]:
            """Read in two pieces."""
            reader, writer = await open_connection(HOST, server.port)
            head = await reader.read_exactly(EXPECTED)
            rest = await reader.read()
            await _close(writer)
            return head, rest

        assert _serve_and(handler, client) == (MESSAGE[:EXPECTED], MESSAGE[EXPECTED:])

    def test_read_until_and_limit(self) -> None:
        """Return lines including the separator; refuse lines over the limit."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send a short line, a long line and a custom-separated payload."""
            writer.write(FIRST_LINE + LONG_LINE + CUSTOM_PAYLOAD)

        async def client(server: Server) -> None:
            """Read the lines back."""
            reader, writer = await open_connection(HOST, server.port)
            assert await reader.read_until() == FIRST_LINE
            with pytest.raises(LimitOverrunError):
                await reader.read_until(limit=SMALL_LIMIT)
            assert await reader.read_until() == LONG_LINE  # data was kept
            assert await reader.read_until(CUSTOM_SEP) == b"left||"
            await _close(writer)

        _serve_and(handler, client)

    def test_read_until_limit_without_separator(self) -> None:
        """Raise LimitOverrunError when the buffer outgrows the limit unseparated."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send bytes with no newline and keep the line open."""
            writer.write(TAIL)
            await writer.drain()
            await sleep(LONG)

        async def client(server: Server) -> None:
            """Ask for a short line."""
            reader, writer = await open_connection(HOST, server.port)
            with pytest.raises(LimitOverrunError):
                await reader.read_until(limit=SMALL_LIMIT)
            await _close(writer)

        _serve_and(handler, client)

    def test_read_until_eof_without_separator(self) -> None:
        """Raise IncompleteReadError when EOF comes before the separator."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send a tail with no newline then hang up."""
            writer.write(TAIL)

        async def client(server: Server) -> bytes:
            """Ask for a line."""
            reader, writer = await open_connection(HOST, server.port)
            with pytest.raises(IncompleteReadError) as info:
                await reader.read_until()
            await _close(writer)
            return info.value.partial

        assert _serve_and(handler, client) == TAIL

    def test_read_returns_empty_at_eof(self) -> None:
        """Read everything with read(), then b"" once the peer has gone."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send a line then hang up."""
            writer.write(MESSAGE)

        async def client(server: Server) -> None:
            """Read to EOF and beyond."""
            reader, writer = await open_connection(HOST, server.port)
            assert await reader.read(0) == b""
            assert not reader.at_eof()
            assert await reader.read() == MESSAGE
            assert reader.at_eof()
            assert await reader.read(1) == b""
            assert await reader.read() == b""
            await _close(writer)

        _serve_and(handler, client)

    def test_read_n_returns_at_most_n(self) -> None:
        """Return no more than ``n`` bytes per read."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Send three bytes."""
            writer.write(PARTIAL)

        async def client(server: Server) -> list[bytes]:
            """Read one byte at a time."""
            reader, writer = await open_connection(HOST, server.port)
            pieces = [await reader.read(1) for _ in PARTIAL]
            await _close(writer)
            return pieces

        assert _serve_and(handler, client) == [bytes([byte]) for byte in PARTIAL]

    def test_cancelled_read_leaves_stream_usable(self) -> None:
        """Drop the fd watch when a read times out; the next read still works."""

        async def handler(reader: StreamReader, writer: StreamWriter) -> None:
            """Wait for the go signal, then send one byte."""
            await reader.read_until()
            writer.write(LATE_BYTE)
            await writer.drain()
            await reader.read()

        async def client(server: Server) -> bytes:
            """Time out a read, check the selector, then read for real."""
            reader, writer = await open_connection(HOST, server.port)
            with pytest.raises(TimeoutError):
                await wait_for(reader.read(1), SHORT)
            with pytest.raises(KeyError):
                get_running_loop()._selector.get_key(_fd(writer))
            writer.write(GO)
            await writer.drain()
            data = await reader.read(1)
            await _close(writer)
            return data

        assert _serve_and(handler, client) == LATE_BYTE

    def test_concurrent_reads_rejected(self) -> None:
        """Refuse a second read while one is already waiting."""

        async def client(server: Server) -> None:
            """Start two reads at once."""
            reader, writer = await open_connection(HOST, server.port)
            task = spawn(wait_for(reader.read(1), SHORT))
            await sleep(TICK)
            with pytest.raises(RuntimeError, match="already"):
                await reader.read(1)
            with pytest.raises(TimeoutError):
                await task
            await _close(writer)

        _serve_and(_echo_line, client)


class TestWriting:
    """Test the writer's buffering, backpressure and failure handling."""

    def test_drain_applies_backpressure(self) -> None:
        """Keep drain() waiting while a peer that never reads blocks the flow."""
        shrunk = Event()

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Shrink the receive buffer, then never read."""
            _shrink(writer, socket.SO_RCVBUF)
            shrunk.set()
            await sleep(LONG)

        async def client(server: Server) -> None:
            """Write far more than the high-water mark."""
            _, writer = await open_connection(HOST, server.port)
            _shrink(writer, socket.SO_SNDBUF)
            await shrunk.wait()
            writer.write(bytes(BACKPRESSURE_FACTOR * DEFAULT_HIGH_WATER))
            with pytest.raises(TimeoutError):
                await wait_for(writer.drain(), SHORT)
            writer.abort()
            await writer.wait_closed()

        _serve_and(handler, client)

    def test_drain_resumes_when_peer_reads(self) -> None:
        """Finish drain() once the peer reads the backlog."""
        total = BACKPRESSURE_FACTOR * DEFAULT_HIGH_WATER

        async def handler(reader: StreamReader, writer: StreamWriter) -> None:
            """Read everything and report how much arrived."""
            data = await reader.read_exactly(total)
            writer.write(str(len(data)).encode() + b"\n")
            await writer.drain()

        async def client(server: Server) -> bytes:
            """Write a big payload and drain it."""
            reader, writer = await open_connection(HOST, server.port)
            _shrink(writer, socket.SO_SNDBUF)
            writer.write(bytes(total))
            await writer.drain()
            writer.write(b"")  # writing nothing is a no-op
            line = await reader.read_until()
            await _close(writer)
            return line

        assert _serve_and(handler, client) == str(total).encode() + b"\n"

    def test_reset_found_by_reader_fails_pending_drain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Raise from a pending drain() when the reader is the one that sees the reset."""
        shrunk = Event()

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Shrink the receive buffer, send one byte, then never read."""
            _shrink(writer, socket.SO_RCVBUF)
            shrunk.set()
            writer.write(LATE_BYTE)
            await sleep(LONG)

        def reset(_size: int) -> bytes:
            """Pretend the peer reset the connection."""
            raise ConnectionResetError(errno.ECONNRESET, os.strerror(errno.ECONNRESET))

        async def client(server: Server) -> bytes:
            """Leave a drain pending, then let a read discover the reset."""
            reader, writer = await open_connection(HOST, server.port)
            _shrink(writer, socket.SO_SNDBUF)
            await shrunk.wait()
            writer.write(bytes(BACKPRESSURE_FACTOR * DEFAULT_HIGH_WATER))
            draining = spawn(writer.drain())
            await sleep(TICK)
            assert not draining.done()
            monkeypatch.setattr(reader._transport, "recv", reset)
            eof = await reader.read(1)
            with pytest.raises(ConnectionClosedError, match="reset"):
                await wait_for(draining, LONG)
            return eof

        assert _serve_and(handler, client) == b""

    def test_abort_fails_pending_drain(self) -> None:
        """Raise from a pending drain() when abort() throws the buffer away."""
        shrunk = Event()

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Shrink the receive buffer, then never read."""
            _shrink(writer, socket.SO_RCVBUF)
            shrunk.set()
            await sleep(LONG)

        async def client(server: Server) -> None:
            """Abort while a drain is waiting."""
            _, writer = await open_connection(HOST, server.port)
            _shrink(writer, socket.SO_SNDBUF)
            await shrunk.wait()
            writer.write(bytes(BACKPRESSURE_FACTOR * DEFAULT_HIGH_WATER))
            draining = spawn(writer.drain())
            await sleep(TICK)
            assert not draining.done()
            writer.abort()
            with pytest.raises(ConnectionClosedError, match="buffered"):
                await wait_for(draining, LONG)

        _serve_and(handler, client)

    def test_write_after_close_raises(self) -> None:
        """Raise ConnectionClosedError when writing to a closed writer."""

        async def client(server: Server) -> None:
            """Close then write."""
            _, writer = await open_connection(HOST, server.port)
            await _close(writer)
            with pytest.raises(ConnectionClosedError):
                writer.write(MESSAGE)

        _serve_and(_echo_line, client)

    def test_drain_after_peer_reset_raises(self) -> None:
        """Raise ConnectionClosedError from drain() once the peer has reset."""

        async def handler(_reader: StreamReader, writer: StreamWriter) -> None:
            """Hang up straight away."""
            writer.close()

        async def client(server: Server) -> bool:
            """Keep writing to a peer that has gone until drain() objects."""
            reader, writer = await open_connection(HOST, server.port)
            assert await reader.read() == b""
            for _ in range(MAX_ATTEMPTS):
                writer.write(CHUNK)
                try:
                    await writer.drain()
                except ConnectionClosedError:
                    break
                await sleep(TICK)
            else:
                return False
            with pytest.raises(ConnectionClosedError):
                writer.write(CHUNK)
            await writer.wait_closed()
            return writer.is_closing()

        assert _serve_and(handler, client)


class TestPipes:
    """Test streams over OS pipes."""

    def test_pipe_reader_reads_os_pipe(self) -> None:
        """Read bytes written to the other end of an OS pipe, then EOF."""
        read_fd, write_fd = os.pipe()
        os.write(write_fd, MESSAGE)
        os.close(write_fd)

        async def main() -> tuple[bytes, bool]:
            """Read the pipe to the end."""
            reader = pipe_reader(read_fd)
            data = await reader.read()
            return data, reader.at_eof()

        assert run(main()) == (MESSAGE, True)

    def test_pipe_writer_writes_os_pipe(self) -> None:
        """Write through a pipe writer and read from a pipe reader."""
        read_fd, write_fd = os.pipe()

        async def main() -> bytes:
            """Send a line down the pipe."""
            reader = pipe_reader(read_fd)
            writer = pipe_writer(write_fd)
            assert writer.get_extra_info("fd") == write_fd
            writer.write(MESSAGE)
            await writer.drain()
            await _close(writer)
            return await reader.read()

        assert run(main()) == MESSAGE

    def test_pipe_writer_broken_pipe(self) -> None:
        """Raise ConnectionClosedError when the pipe's reader has gone."""
        read_fd, write_fd = os.pipe()
        os.close(read_fd)

        async def main() -> None:
            """Write into a pipe nobody reads."""
            writer = pipe_writer(write_fd)
            writer.write(MESSAGE)
            await writer.drain()

        with pytest.raises(ConnectionClosedError):
            run(main())

    def test_reader_close_closes_fd_and_wakes_read(self) -> None:
        """Close the fd behind a reader, waking a waiting read with EOF."""
        read_fd, write_fd = os.pipe()

        async def main() -> tuple[bytes, bool]:
            """Close the reader while a read is waiting on an idle pipe."""
            reader = pipe_reader(read_fd)
            waiting = spawn(reader.read())
            await sleep(SHORT)
            reader.close()
            reader.close()  # a second close is harmless
            data = await wait_for(waiting, LONG)
            return data, reader.at_eof()

        try:
            assert run(main()) == (b"", True)
            with pytest.raises(OSError, match="Bad file descriptor"):
                os.fstat(read_fd)
        finally:
            os.close(write_fd)


class TestExample:
    """Test the echo server example."""

    def test_echo_server_self_test(self) -> None:
        """Run the example's self-test round trip."""
        spec = importlib.util.spec_from_file_location("echo_server", EXAMPLE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.self_test() == MESSAGE
