"""Test the messaging Peer: request/response, push, errors and closing."""

import importlib.util
import json
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from pynet.errors import ConnectionClosedError, FrameTooLargeError, RemoteError
from pynet.framing import FramedConnection
from pynet.loop import Cancelled, ErrorContext, Event, get_running_loop, run, sleep, spawn, wait_for
from pynet.messaging import (
    BAD_MESSAGE,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MAX_MISSED_PONGS,
    DEFAULT_REQUEST_TIMEOUT,
    HANDLER_ERROR,
    KIND_JSON,
    UNKNOWN_METHOD,
    UNSUPPORTED_VERSION,
    JsonValue,
    Message,
    MessageDecoder,
    MessageType,
    Peer,
    Reply,
    connect,
    encode_message,
    serve,
)
from pynet.streams import Server, StreamReader, StreamWriter, open_connection, start_server

HOST = "127.0.0.1"
ANY_PORT = 0
SHORT = 1.0
LONG = 3.0
SLOW = 0.2
TINY_TIMEOUT = 0.05
BODY: dict[str, JsonValue] = {"x": 1}
BLOB = b"\x00\xff"
OTHER_BLOB = b"second"
BOOM = "boom"
CUSTOM_CODE = "not_found"
CUSTOM_MESSAGE = "no such job"
REQUEST_ID = 41
BAD_VERSION = 99
PING_BODY = "are you there?"
SENDERS = 3
BIG = 2_000_000
HUGE = [b"h" * 10_000_000] * 5
FRAME_LIMIT = 1024
EXPECTED_REQUEST_TIMEOUT = 30.0
EXPECTED_HEARTBEAT_INTERVAL = 5.0
EXPECTED_MAX_MISSED_PONGS = 3
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "chat_room.py"

type Setup = Callable[[Peer], None]


async def echo(msg: Message) -> JsonValue:
    """Return the request body unchanged."""
    return msg.body


async def slow(msg: Message) -> JsonValue:
    """Wait a little, then return the request body."""
    await sleep(SLOW)
    return msg.body


def echo_setup(peer: Peer) -> None:
    """Register the echo handler."""
    peer.on("echo", echo)


async def start_pair(
    server_setup: Setup = echo_setup,
    client_setup: Setup | None = None,
) -> tuple[Peer, Peer, Server]:
    """Start a server and connect a client; return client, server-side peer, server."""
    accepted: list[Peer] = []
    ready = Event()

    def setup(peer: Peer) -> None:
        """Run the test's setup, then hand the peer to the test."""
        server_setup(peer)
        accepted.append(peer)
        ready.set()

    server = await serve(HOST, ANY_PORT, setup, heartbeat_interval=None)
    client = await connect(HOST, server.port, setup=client_setup, heartbeat_interval=None)
    await wait_for(ready.wait(), SHORT)
    return client, accepted[0], server


async def stop_pair(client: Peer, server_peer: Peer, server: Server) -> None:
    """Close both peers and the server."""
    await wait_for(client.close(), SHORT)
    await wait_for(server_peer.close(), SHORT)
    server.close()
    await server.wait_closed()


def run_pair[T](
    body: Callable[[Peer, Peer], Awaitable[T]],
    server_setup: Setup = echo_setup,
    client_setup: Setup | None = None,
) -> T:
    """Run ``body(client, server_peer)`` against a fresh pair, then tidy up."""

    async def main() -> T:
        """Start the pair, run the body, close everything."""
        client, server_peer, server = await start_pair(server_setup, client_setup)
        try:
            return await body(client, server_peer)
        finally:
            await stop_pair(client, server_peer, server)

    return run(main())


class TestConstants:
    """Test the published defaults."""

    def test_defaults(self) -> None:
        """Expose the request and heartbeat defaults from the brief."""
        assert DEFAULT_REQUEST_TIMEOUT == EXPECTED_REQUEST_TIMEOUT
        assert DEFAULT_HEARTBEAT_INTERVAL == EXPECTED_HEARTBEAT_INTERVAL
        assert DEFAULT_MAX_MISSED_PONGS == EXPECTED_MAX_MISSED_PONGS

    def test_reply_defaults(self) -> None:
        """Default a Reply to a null body and no attachments."""
        assert Reply() == Reply(None, ())

    def test_remote_error_fields(self) -> None:
        """Keep the code and message on a RemoteError."""
        error = RemoteError(CUSTOM_CODE, CUSTOM_MESSAGE)
        assert error.code == CUSTOM_CODE
        assert error.remote_message == CUSTOM_MESSAGE
        assert CUSTOM_CODE in str(error)


class TestRequestResponse:
    """Test requests and their answers."""

    def test_request_response(self) -> None:
        """Get the echo handler's return value back as the response body."""

        async def body(client: Peer, _server: Peer) -> Message:
            """Send one echo request."""
            return await client.request("echo", BODY, timeout=SHORT)

        reply = run_pair(body)
        assert reply.type is MessageType.RESPONSE
        assert reply.body == BODY
        assert reply.attachments == ()

    def test_reply_with_attachments(self) -> None:
        """Send attachments both ways and get a Reply's attachments back."""

        async def flip(msg: Message) -> Reply:
            """Return the attachments reversed, with their count as the body."""
            return Reply(len(msg.attachments), tuple(reversed(msg.attachments)))

        def setup(peer: Peer) -> None:
            """Register the flip handler."""
            peer.on("flip", flip)

        async def body(client: Peer, _server: Peer) -> Message:
            """Send two attachments."""
            return await client.request("flip", attachments=[BLOB, OTHER_BLOB], timeout=SHORT)

        reply = run_pair(body, setup)
        assert reply.body == len((BLOB, OTHER_BLOB))
        assert reply.attachments == (OTHER_BLOB, BLOB)

    def test_concurrent_requests_on_one_connection(self) -> None:
        """Let a fast request finish while a slow one is still running."""
        finished: list[str] = []

        def setup(peer: Peer) -> None:
            """Register a slow and a fast handler."""
            peer.on("slow", slow)
            peer.on("fast", echo)

        async def ask(client: Peer, method: str) -> None:
            """Send one request and record when it finishes."""
            await client.request(method, method, timeout=LONG)
            finished.append(method)

        async def body(client: Peer, _server: Peer) -> None:
            """Start the slow request first, then the fast one."""
            slow_task = spawn(ask(client, "slow"))
            await sleep(0)
            await ask(client, "fast")
            await wait_for(slow_task, LONG)

        run_pair(body, setup)
        assert finished == ["fast", "slow"]

    def test_request_ids_increase(self) -> None:
        """Number each peer's requests with increasing ids."""
        seen: list[int] = []

        async def record(msg: Message) -> JsonValue:
            """Remember the id of each request."""
            assert msg.id is not None
            seen.append(msg.id)
            return None

        def setup(peer: Peer) -> None:
            """Register the recording handler."""
            peer.on("record", record)

        async def body(client: Peer, _server: Peer) -> None:
            """Send three requests in turn."""
            for _ in range(len(BLOB) + 1):
                await client.request("record", timeout=SHORT)

        run_pair(body, setup)
        assert seen == sorted(seen)
        assert len(set(seen)) == len(seen)

    def test_request_timeout(self) -> None:
        """Raise TimeoutError, then ignore the late response and keep working."""

        def setup(peer: Peer) -> None:
            """Register slow and echo handlers."""
            peer.on("slow", slow)
            peer.on("echo", echo)

        async def body(client: Peer, _server: Peer) -> Message:
            """Time out, wait for the late answer, then ask again."""
            with pytest.raises(TimeoutError):
                await client.request("slow", BODY, timeout=TINY_TIMEOUT)
            await sleep(SLOW * 2)
            assert not client.closed
            return await client.request("echo", BODY, timeout=SHORT)

        assert run_pair(body, setup).body == BODY


class TestErrors:
    """Test error replies."""

    def test_unknown_method_gives_remote_error(self) -> None:
        """Answer a request for an unregistered method with UNKNOWN_METHOD."""

        async def body(client: Peer, _server: Peer) -> RemoteError:
            """Ask for a method nobody handles."""
            with pytest.raises(RemoteError) as info:
                await client.request("nope", timeout=SHORT)
            return info.value

        error = run_pair(body)
        assert error.code == UNKNOWN_METHOD
        assert "nope" in error.remote_message

    def test_handler_error_then_connection_still_works(self) -> None:
        """Turn a handler exception into HANDLER_ERROR and keep the connection usable."""
        reported: list[ErrorContext] = []

        async def explode(_msg: Message) -> JsonValue:
            """Fail on purpose."""
            raise ValueError(BOOM)

        def setup(peer: Peer) -> None:
            """Register a failing and an echo handler."""
            peer.on("explode", explode)
            peer.on("echo", echo)

        async def body(client: Peer, _server: Peer) -> tuple[RemoteError, Message]:
            """Hit the failing handler, then the echo handler."""
            get_running_loop().set_exception_handler(reported.append)
            with pytest.raises(RemoteError) as info:
                await client.request("explode", timeout=SHORT)
            after = await client.request("echo", BODY, timeout=SHORT)
            return info.value, after

        error, after = run_pair(body, setup)
        assert error.code == HANDLER_ERROR
        assert error.remote_message == BOOM
        assert after.body == BODY
        assert [type(ctx.exception) for ctx in reported] == [ValueError]

    def test_handler_raising_remote_error_sends_its_code(self) -> None:
        """Send a handler's own RemoteError code and message."""

        async def missing(_msg: Message) -> JsonValue:
            """Fail with a structured error."""
            raise RemoteError(CUSTOM_CODE, CUSTOM_MESSAGE)

        def setup(peer: Peer) -> None:
            """Register the structured-error handler."""
            peer.on("missing", missing)

        async def body(client: Peer, _server: Peer) -> RemoteError:
            """Hit the handler."""
            with pytest.raises(RemoteError) as info:
                await client.request("missing", timeout=SHORT)
            return info.value

        error = run_pair(body, setup)
        assert (error.code, error.remote_message) == (CUSTOM_CODE, CUSTOM_MESSAGE)

    def test_handler_returning_non_json_gives_handler_error(self) -> None:
        """Turn an unencodable return value into HANDLER_ERROR."""
        reported: list[ErrorContext] = []

        async def bad(_msg: Message) -> JsonValue:
            """Return something JSON cannot hold."""
            return float("nan")

        def setup(peer: Peer) -> None:
            """Register the bad handler."""
            peer.on("bad", bad)

        async def body(client: Peer, _server: Peer) -> RemoteError:
            """Hit the handler."""
            get_running_loop().set_exception_handler(reported.append)
            with pytest.raises(RemoteError) as info:
                await client.request("bad", timeout=SHORT)
            return info.value

        assert run_pair(body, setup).code == HANDLER_ERROR
        assert len(reported) == 1


class TestPush:
    """Test one-way pushes."""

    def test_push_both_directions(self) -> None:
        """Push from client to server and from server back to client."""
        heard: list[JsonValue] = []
        greeted = Event()
        welcomed = Event()

        async def hello(msg: Message) -> None:
            """Record the hello and its attachment."""
            heard.append(msg.body)
            assert msg.attachments == (BLOB,)
            greeted.set()

        async def welcome(msg: Message) -> None:
            """Record the welcome and wake the test."""
            heard.append(msg.body)
            welcomed.set()

        def server_setup(peer: Peer) -> None:
            """Register the server's hello handler."""
            peer.on("hello", hello)

        def client_setup(peer: Peer) -> None:
            """Register the client's welcome handler."""
            peer.on("welcome", welcome)

        async def body(client: Peer, server: Peer) -> None:
            """Push one way, then the other."""
            await client.push("hello", "from client", attachments=[BLOB])
            await server.push("welcome", "from server")
            await wait_for(greeted.wait(), SHORT)
            await wait_for(welcomed.wait(), SHORT)

        run_pair(body, server_setup, client_setup)
        assert sorted(map(str, heard)) == ["from client", "from server"]

    def test_pushes_are_handled_one_at_a_time_in_order(self) -> None:
        """Finish the first push's handler before the second push's handler starts."""
        events: list[str] = []
        both_done = Event()

        async def note(msg: Message) -> None:
            """Record start and end; the first push dawdles."""
            events.append(f"start {msg.body}")
            if msg.body == "first":
                await sleep(SLOW)
            events.append(f"end {msg.body}")
            if msg.body == "second":
                both_done.set()

        def server_setup(peer: Peer) -> None:
            """Register the note handler."""
            peer.on("note", note)

        async def body(client: Peer, _server: Peer) -> None:
            """Push twice, back to back."""
            await client.push("note", "first")
            await client.push("note", "second")
            await wait_for(both_done.wait(), SHORT)

        run_pair(body, server_setup)
        assert events == ["start first", "end first", "start second", "end second"]

    def test_concurrent_large_sends_never_interleave(self) -> None:
        """Keep each message's attachment frames together under backpressure."""
        received: list[Message] = []
        all_in = Event()

        async def collect(msg: Message) -> None:
            """Record each push; wake the test when all have arrived."""
            received.append(msg)
            if len(received) == SENDERS:
                all_in.set()

        def setup(peer: Peer) -> None:
            """Register the collector."""
            peer.on("blob", collect)

        def blobs(index: int) -> list[bytes]:
            """Return two big attachments made of ``index``'s own byte."""
            return [bytes([index]) * BIG, bytes([index + SENDERS]) * BIG]

        async def body(client: Peer, _server: Peer) -> None:
            """Push from several tasks at once."""
            tasks = [
                spawn(client.push("blob", index, attachments=blobs(index)))
                for index in range(SENDERS)
            ]
            for task in tasks:
                await wait_for(task, LONG)
            await wait_for(all_in.wait(), LONG)

        run_pair(body, setup)
        for msg in received:
            assert isinstance(msg.body, int)
            assert list(msg.attachments) == blobs(msg.body)

    def test_cancelled_sender_never_leaves_half_a_message(self) -> None:
        """Finish sending a message even when the sending task is cancelled."""
        received: list[Message] = []

        async def collect(msg: Message) -> None:
            """Record each push."""
            received.append(msg)

        def setup(peer: Peer) -> None:
            """Register the collector and echo."""
            peer.on("blob", collect)
            peer.on("echo", echo)

        async def body(client: Peer, _server: Peer) -> Message:
            """Cancel a big push mid-flight, then make a request."""
            sender = spawn(client.push("blob", attachments=[BLOB * BIG, BLOB * BIG]))
            await sleep(0)
            sender.cancel()
            with pytest.raises(Cancelled):
                await sender
            return await client.request("echo", BODY, timeout=LONG)

        assert run_pair(body, setup).body == BODY
        assert len(received) == 1

    def test_push_without_handler_is_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log and drop a push nobody handles; send no reply and stay open."""

        async def body(client: Peer, _server: Peer) -> Message:
            """Push to nowhere, then make a normal request."""
            with caplog.at_level(logging.WARNING):
                await client.push("nowhere", BODY)
                return await client.request("echo", BODY, timeout=SHORT)

        assert run_pair(body).body == BODY
        assert "nowhere" in caplog.text

    def test_push_handler_error_is_reported(self) -> None:
        """Report a push handler's exception and keep the connection open."""
        reported: list[ErrorContext] = []

        async def explode(_msg: Message) -> None:
            """Fail on purpose."""
            raise ValueError(BOOM)

        def setup(peer: Peer) -> None:
            """Register the failing push handler and echo."""
            peer.on("explode", explode)
            peer.on("echo", echo)

        async def body(client: Peer, _server: Peer) -> Message:
            """Push to the failing handler, then make a request."""
            get_running_loop().set_exception_handler(reported.append)
            await client.push("explode")
            while not reported:
                await sleep(TINY_TIMEOUT)
            return await client.request("echo", BODY, timeout=SHORT)

        assert run_pair(body, setup).body == BODY
        assert [type(ctx.exception) for ctx in reported] == [ValueError]


class TestClosing:
    """Test what happens when a connection ends."""

    def test_peer_drop_fails_pending_requests(self) -> None:
        """Fail an in-flight request promptly when the other side goes away."""
        started = Event()

        async def stall(_msg: Message) -> JsonValue:
            """Signal the test, then wait far longer than it will."""
            started.set()
            await sleep(LONG * 2)
            return None

        def setup(peer: Peer) -> None:
            """Register the stalling handler."""
            peer.on("stall", stall)

        async def body(client: Peer, server: Peer) -> float:
            """Start a request, drop the server side, time the failure."""
            pending = spawn(client.request("stall", timeout=LONG * 2))
            await wait_for(started.wait(), SHORT)
            began = time.monotonic()
            await server.close()
            with pytest.raises(ConnectionClosedError):
                await wait_for(pending, SHORT)
            return time.monotonic() - began

        assert run_pair(body, setup) < SHORT

    def test_close_with_stalled_remote_fails_inflight_send(self) -> None:
        """Close promptly and fail a still-sending request when the remote never reads."""

        async def never_read(_reader: StreamReader, _writer: StreamWriter) -> None:
            """Accept the connection and ignore it."""
            await sleep(LONG)

        async def main() -> tuple[float, bool]:
            """Start a huge request, then close the client peer."""
            server = await start_server(never_read, HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                pending = spawn(client.request("x", attachments=HUGE, timeout=None))
                await sleep(TINY_TIMEOUT * 2)
                stuck = not pending.done()
                began = time.monotonic()
                await wait_for(client.close(), SHORT)
                with pytest.raises(ConnectionClosedError):
                    await wait_for(pending, SHORT)
                return time.monotonic() - began, stuck
            finally:
                server.close()
                await server.wait_closed()

        elapsed, stuck = run(main())
        assert stuck
        assert elapsed < SHORT

    def test_request_timeout_covers_a_stalled_send(self) -> None:
        """Time out a request whose send is stuck behind a remote that never reads."""

        async def never_read(_reader: StreamReader, _writer: StreamWriter) -> None:
            """Accept the connection and ignore it."""
            await sleep(LONG)

        async def main() -> float:
            """Send a huge request with a short timeout and no heartbeat."""
            server = await start_server(never_read, HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                began = time.monotonic()
                with pytest.raises(TimeoutError):
                    await wait_for(
                        client.request("x", attachments=HUGE, timeout=TINY_TIMEOUT), LONG
                    )
                return time.monotonic() - began
            finally:
                await wait_for(client.close(), SHORT)
                server.close()
                await server.wait_closed()

        assert run(main()) < SHORT  # about TINY_TIMEOUT, not the LONG guard

    def test_remote_drop_fails_stalled_send(self) -> None:
        """Fail a still-sending request when the stalled remote hangs up."""

        async def read_nothing_then_hang_up(_reader: StreamReader, writer: StreamWriter) -> None:
            """Accept, ignore everything, then close."""
            await sleep(TINY_TIMEOUT * 2)
            writer.abort()

        async def main() -> BaseException | None:
            """Start a huge request and wait for the remote to go."""
            server = await start_server(read_nothing_then_hang_up, HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            causes: list[BaseException | None] = []
            client.on_close(causes.append)
            try:
                with pytest.raises(ConnectionClosedError):
                    await wait_for(client.request("x", attachments=HUGE), SHORT)
                await wait_for(client.wait_closed(), SHORT)
                return causes[0]
            finally:
                server.close()
                await server.wait_closed()

        assert isinstance(run(main()), ConnectionClosedError)

    def test_oversize_attachment_sends_nothing(self) -> None:
        """Refuse an over-limit attachment before writing, and keep working."""

        async def main() -> Message:
            """Send an oversize request, then a normal one."""
            server = await serve(HOST, ANY_PORT, echo_setup, heartbeat_interval=None)
            reader, writer = await open_connection(HOST, server.port)
            client = Peer(
                FramedConnection(reader, writer, max_frame_size=FRAME_LIMIT),
                heartbeat_interval=None,
            )
            client.start()
            try:
                with pytest.raises(FrameTooLargeError):
                    await client.request("echo", attachments=[b"o" * (FRAME_LIMIT + 1)])
                return await client.request("echo", BODY, timeout=SHORT)
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        assert run(main()).body == BODY

    def test_on_close_called_once_with_cause(self) -> None:
        """Run on_close callbacks once: with the error remotely, with None locally."""
        client_causes: list[BaseException | None] = []
        server_causes: list[BaseException | None] = []

        def server_setup(peer: Peer) -> None:
            """Record the server side's close cause."""
            peer.on_close(server_causes.append)

        def client_setup(peer: Peer) -> None:
            """Record the client side's close cause."""
            peer.on_close(client_causes.append)

        async def body(client: Peer, server: Peer) -> None:
            """Close the server side and let the client notice."""
            await server.close()
            await server.close()
            await wait_for(client.wait_closed(), SHORT)
            await client.close()
            assert client.closed
            assert server.closed

        run_pair(body, server_setup, client_setup)
        assert server_causes == [None]
        assert len(client_causes) == 1
        assert isinstance(client_causes[0], ConnectionClosedError)

    def test_on_close_after_close_runs_at_once(self) -> None:
        """Call a callback registered after closing straight away."""
        causes: list[BaseException | None] = []

        async def body(client: Peer, _server: Peer) -> None:
            """Close, then register."""
            await client.close()
            client.on_close(causes.append)

        run_pair(body)
        assert causes == [None]

    def test_failing_close_callback_is_reported(self) -> None:
        """Report a close callback's exception and still run the others."""
        reported: list[ErrorContext] = []
        causes: list[BaseException | None] = []

        def broken(_cause: BaseException | None) -> None:
            """Fail on purpose."""
            raise ValueError(BOOM)

        async def body(client: Peer, _server: Peer) -> None:
            """Register both callbacks and close."""
            get_running_loop().set_exception_handler(reported.append)
            client.on_close(broken)
            client.on_close(causes.append)
            await client.close()

        run_pair(body)
        assert causes == [None]
        assert [type(ctx.exception) for ctx in reported] == [ValueError]

    def test_request_and_push_after_close_raise(self) -> None:
        """Refuse to send on a closed peer."""

        async def body(client: Peer, _server: Peer) -> None:
            """Close, then try to send."""
            await client.close()
            with pytest.raises(ConnectionClosedError):
                await client.request("echo", timeout=SHORT)
            with pytest.raises(ConnectionClosedError):
                await client.push("echo")

        run_pair(body)

    def test_peername_is_the_remote_address(self) -> None:
        """Report the address of the other end."""

        async def body(client: Peer, _server: Peer) -> object:
            """Return the client's peername."""
            return client.peername

        peername = run_pair(body)
        assert isinstance(peername, tuple)
        assert peername[0] == HOST


class TestServe:
    """Test the serve helper."""

    def test_async_setup(self) -> None:
        """Await an async setup before starting the peer."""

        async def setup(peer: Peer) -> None:
            """Register echo after yielding once."""
            await sleep(0)
            peer.on("echo", echo)

        async def main() -> Message:
            """Serve with an async setup and make one request."""
            server = await serve(HOST, ANY_PORT, setup, heartbeat_interval=None)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                return await client.request("echo", BODY, timeout=SHORT)
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        assert run(main()).body == BODY


async def raw_client(port: int) -> tuple[FramedConnection, MessageDecoder]:
    """Open a bare framed connection for sending hand-made frames."""
    reader, writer = await open_connection(HOST, port)
    return FramedConnection(reader, writer), MessageDecoder()


async def receive(conn: FramedConnection, decoder: MessageDecoder) -> Message:
    """Read frames until one whole message has arrived."""
    while True:
        msg = decoder.feed(await wait_for(conn.recv_frame(), SHORT))
        if msg is not None:
            return msg


def run_raw[T](body: Callable[[FramedConnection, MessageDecoder], Awaitable[T]]) -> T:
    """Run ``body`` with a raw connection to an echo server."""

    async def main() -> T:
        """Start the server, run the body, tidy up."""
        server = await serve(HOST, ANY_PORT, echo_setup, heartbeat_interval=None)
        conn, decoder = await raw_client(server.port)
        try:
            return await body(conn, decoder)
        finally:
            conn.close()
            await conn.wait_closed()
            server.close()
            await server.wait_closed()

    return run(main())


async def send_message(conn: FramedConnection, msg: Message) -> None:
    """Send every frame of ``msg``."""
    for frame in encode_message(msg):
        await conn.send_frame(frame)


class TestProtocolErrors:
    """Test how a peer reacts to frames that break the protocol."""

    def test_bad_version_with_id_gets_error_reply(self) -> None:
        """Answer a wrong-version request that has an id with UNSUPPORTED_VERSION."""

        async def body(conn: FramedConnection, decoder: MessageDecoder) -> Message:
            """Send a version-99 request."""
            envelope = {"v": BAD_VERSION, "type": "request", "id": REQUEST_ID, "method": "echo"}
            await conn.send_frame(KIND_JSON + json.dumps(envelope).encode())
            return await receive(conn, decoder)

        reply = run_raw(body)
        assert reply.type is MessageType.ERROR
        assert reply.id == REQUEST_ID
        assert isinstance(reply.body, dict)
        assert reply.body["code"] == UNSUPPORTED_VERSION

    def test_bad_message_with_id_gets_bad_message_reply(self) -> None:
        """Answer a malformed request that has an id with BAD_MESSAGE."""

        async def body(conn: FramedConnection, decoder: MessageDecoder) -> Message:
            """Send a request with no method."""
            envelope = {"v": 1, "type": "request", "id": REQUEST_ID}
            await conn.send_frame(KIND_JSON + json.dumps(envelope).encode())
            return await receive(conn, decoder)

        reply = run_raw(body)
        assert reply.id == REQUEST_ID
        assert isinstance(reply.body, dict)
        assert reply.body["code"] == BAD_MESSAGE

    def test_bad_frame_without_id_is_reported_and_connection_stays_open(self) -> None:
        """Report a frame with no usable id, then keep serving."""
        reported: list[ErrorContext] = []

        async def body(conn: FramedConnection, decoder: MessageDecoder) -> Message:
            """Send junk, then a good request."""
            get_running_loop().set_exception_handler(reported.append)
            await conn.send_frame(b"junk")
            await conn.send_frame(KIND_JSON + b"not json")
            await send_message(conn, Message(MessageType.REQUEST, REQUEST_ID, "echo", BODY))
            return await receive(conn, decoder)

        reply = run_raw(body)
        assert reply.type is MessageType.RESPONSE
        assert reply.body == BODY
        expected_reports = 2
        assert len(reported) == expected_reports

    def test_ping_gets_pong(self) -> None:
        """Answer a ping with a pong carrying the same id and body."""

        async def body(conn: FramedConnection, decoder: MessageDecoder) -> Message:
            """Send a ping."""
            await send_message(conn, Message(MessageType.PING, REQUEST_ID, body=PING_BODY))
            return await receive(conn, decoder)

        reply = run_raw(body)
        assert reply == Message(MessageType.PONG, REQUEST_ID, body=PING_BODY)

    def test_unknown_response_id_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log and ignore a response nobody asked for."""

        async def body(conn: FramedConnection, decoder: MessageDecoder) -> Message:
            """Send a stray response, then a good request."""
            with caplog.at_level(logging.WARNING):
                await send_message(conn, Message(MessageType.RESPONSE, REQUEST_ID, body=BODY))
                await send_message(conn, Message(MessageType.REQUEST, REQUEST_ID, "echo", BODY))
                return await receive(conn, decoder)

        assert run_raw(body).body == BODY
        assert str(REQUEST_ID) in caplog.text

    def test_malformed_error_body_still_raises_remote_error(self) -> None:
        """Turn an error reply with an odd body into a BAD_MESSAGE RemoteError."""

        async def main() -> RemoteError:
            """Serve raw, answer the client's request with a bad error body."""
            answered = Event()

            async def handle(conn: FramedConnection) -> None:
                """Read one request and send a junk error reply."""
                decoder = MessageDecoder()
                request = await receive(conn, decoder)
                await send_message(conn, Message(MessageType.ERROR, request.id, body="oops"))
                answered.set()

            async def raw_handler(reader: StreamReader, writer: StreamWriter) -> None:
                """Answer with a junk error, then hold the connection open."""
                await handle(FramedConnection(reader, writer))
                await sleep(SHORT)

            server = await start_server(raw_handler, HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                with pytest.raises(RemoteError) as info:
                    await client.request("anything", timeout=SHORT)
                await wait_for(answered.wait(), SHORT)
                return info.value
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        error = run(main())
        assert error.code == BAD_MESSAGE
        assert "oops" in error.remote_message


class TestExample:
    """Test the chat room example."""

    def test_chat_room_main(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Run the demo and see every message pushed to every member."""
        spec = importlib.util.spec_from_file_location("chat_room", EXAMPLE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.main()
        out = capsys.readouterr().out
        assert "alice hears bob: hi alice" in out
        assert "bob hears alice: hello room" in out
