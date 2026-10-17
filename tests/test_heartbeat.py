"""Test heartbeats: pings every interval, pongs reset the count, silence means dead.

Time is fake (``FakeClock(autojump=True)``): the sockets are real, but
whenever the loop has nothing to do it jumps straight to the next timer,
so many heartbeat intervals pass in a few milliseconds.
"""

import importlib.util
import json
import math
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from pynet.errors import ConnectionClosedError, HeartbeatTimeoutError
from pynet.framing import FramedConnection
from pynet.loop import ErrorContext, Event, get_running_loop, run, sleep, spawn, wait_for
from pynet.loop.testing import FakeClock
from pynet.messaging import (
    DEFAULT_HEARTBEAT_INTERVAL,
    KIND_JSON,
    PROTOCOL_VERSION,
    JsonValue,
    Message,
    MessageDecoder,
    MessageType,
    Peer,
    connect,
    encode_message,
    serve,
)
from pynet.streams import StreamReader, StreamWriter, open_connection, start_server

HOST = "127.0.0.1"
ANY_PORT = 0
INTERVAL = 1.0
MAX_MISSED = 3
PINGS_TO_SEE = 4
SURVIVE_PINGS = 4 * MAX_MISSED
BAD_VERSION = 99
REQUEST_ID = 7
BODY: dict[str, JsonValue] = {"x": 1}
HUGE = [b"h" * 10_000_000] * 5
# Fake seconds: generous, since autojump makes them free.
FAKE_DEADLINE = 1000.0
QUIET_SPELL = 10 * INTERVAL
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "heartbeat_demo.py"

type RawHandler = Callable[[FramedConnection], Awaitable[None]]


def fake_run[T](main: Callable[[], Awaitable[T]]) -> T:
    """Run ``main()`` on a loop whose clock jumps whenever it is idle."""

    async def wrapper() -> T:
        """Await the test body."""
        return await main()

    return run(wrapper(), clock=FakeClock(autojump=True))


async def receive(conn: FramedConnection, decoder: MessageDecoder) -> Message:
    """Read frames until one whole message has arrived."""
    while True:
        msg = decoder.feed(await conn.recv_frame())
        if msg is not None:
            return msg


async def send_message(conn: FramedConnection, msg: Message) -> None:
    """Send every frame of ``msg``."""
    for frame in encode_message(msg):
        await conn.send_frame(frame)


def raw_server(handle: RawHandler) -> Callable[[StreamReader, StreamWriter], Awaitable[None]]:
    """Wrap ``handle`` as a stream handler that ends quietly when the client goes."""

    async def handler(reader: StreamReader, writer: StreamWriter) -> None:
        """Run ``handle`` on a framed connection until the client hangs up."""
        conn = FramedConnection(reader, writer)
        try:
            await handle(conn)
        except ConnectionClosedError:
            pass
        finally:
            conn.abort()

    return handler


def now() -> float:
    """Return the running loop's (fake) time."""
    return get_running_loop().time()


class TestSendingPings:
    """Test that a peer pings on schedule."""

    def test_pings_sent_each_interval(self) -> None:
        """Ping at once, then every DEFAULT_HEARTBEAT_INTERVAL (the default)."""
        seen: list[tuple[float, Message]] = []
        enough = Event()

        async def answer_pings(conn: FramedConnection) -> None:
            """Record each ping's arrival time and answer it."""
            decoder = MessageDecoder()
            while True:
                msg = await receive(conn, decoder)
                seen.append((now(), msg))
                await send_message(conn, Message(MessageType.PONG, msg.id, body=msg.body))
                if len(seen) == PINGS_TO_SEE:
                    enough.set()

        async def main() -> tuple[float, bool]:
            """Connect with the default heartbeat and wait for a few pings."""
            server = await start_server(raw_server(answer_pings), HOST, ANY_PORT)
            client = await connect(HOST, server.port)
            began = now()
            try:
                await wait_for(enough.wait(), FAKE_DEADLINE)
                return began, client.closed
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        began, closed = fake_run(main)
        assert not closed
        times = [when - began for when, _msg in seen[:PINGS_TO_SEE]]
        expected = [tick * DEFAULT_HEARTBEAT_INTERVAL for tick in range(PINGS_TO_SEE)]
        assert len(times) == len(expected)
        assert all(math.isclose(got, want) for got, want in zip(times, expected, strict=True))
        assert all(msg.type is MessageType.PING for _when, msg in seen)
        assert all(msg.id is None for _when, msg in seen)
        assert [msg.body for _when, msg in seen[:PINGS_TO_SEE]] == list(range(1, PINGS_TO_SEE + 1))

    def test_server_side_pings_by_default(self) -> None:
        """Ping from the serve() side too: both ends heartbeat by default."""

        async def main() -> list[Message]:
            """Connect a raw client to a default server and collect pings."""
            server = await serve(HOST, ANY_PORT, lambda _peer: None)
            reader, writer = await open_connection(HOST, server.port)
            conn, decoder = FramedConnection(reader, writer), MessageDecoder()
            try:
                pings: list[Message] = []
                while len(pings) < PINGS_TO_SEE:
                    msg = await wait_for(receive(conn, decoder), FAKE_DEADLINE)
                    pings.append(msg)
                    await send_message(conn, Message(MessageType.PONG, msg.id, body=msg.body))
                return pings
            finally:
                conn.close()
                await conn.wait_closed()
                server.close()
                await server.wait_closed()

        pings = fake_run(main)
        assert [msg.type for msg in pings] == [MessageType.PING] * PINGS_TO_SEE

    def test_heartbeat_disabled(self) -> None:
        """Send no pings at all when heartbeat_interval is None."""
        seen: list[Message] = []

        async def record(conn: FramedConnection) -> None:
            """Record anything that arrives."""
            decoder = MessageDecoder()
            while True:
                seen.append(await receive(conn, decoder))

        async def main() -> bool:
            """Stay connected for many intervals of fake time."""
            server = await start_server(raw_server(record), HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                await sleep(QUIET_SPELL)
                return client.closed
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        assert not fake_run(main)
        assert seen == []

    def test_bad_settings_are_refused(self) -> None:
        """Refuse a non-positive interval or a negative missed-pong limit."""

        async def main() -> None:
            """Build peers with bad heartbeat settings."""
            server = await start_server(raw_server(lambda _conn: sleep(0)), HOST, ANY_PORT)
            reader, writer = await open_connection(HOST, server.port)
            conn = FramedConnection(reader, writer)
            try:
                with pytest.raises(ValueError, match="heartbeat_interval"):
                    Peer(conn, heartbeat_interval=0)
                with pytest.raises(ValueError, match="max_missed_pongs"):
                    Peer(conn, max_missed_pongs=-1)
            finally:
                conn.abort()
                server.close()
                await server.wait_closed()

        fake_run(main)


class TestMissedPongs:
    """Test counting unanswered pings and declaring the other end dead."""

    def test_pong_resets_missed_count(self) -> None:
        """Stay alive when only every MAX_MISSED-th ping is answered."""
        pings = 0
        enough = Event()

        async def answer_some(conn: FramedConnection) -> None:
            """Answer only every MAX_MISSED-th ping."""
            nonlocal pings
            decoder = MessageDecoder()
            while True:
                msg = await receive(conn, decoder)
                pings += 1
                if pings % MAX_MISSED == 0:
                    await send_message(conn, Message(MessageType.PONG, msg.id, body=msg.body))
                if pings == SURVIVE_PINGS:
                    enough.set()

        async def main() -> tuple[float, bool]:
            """Run for well past the point a silent peer would be dead."""
            server = await start_server(raw_server(answer_some), HOST, ANY_PORT)
            client = await connect(
                HOST, server.port, heartbeat_interval=INTERVAL, max_missed_pongs=MAX_MISSED
            )
            began = now()
            try:
                await wait_for(enough.wait(), FAKE_DEADLINE)
                return now() - began, client.closed
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        elapsed, closed = fake_run(main)
        assert not closed
        assert elapsed > (MAX_MISSED + 1) * INTERVAL

    def test_silent_peer_declared_dead(self) -> None:
        """Close with HeartbeatTimeoutError after (MAX_MISSED + 1) intervals of silence."""
        closes: list[tuple[float, BaseException | None]] = []

        async def never_answer(conn: FramedConnection) -> None:
            """Read everything, answer nothing."""
            while True:
                await conn.recv_frame()

        async def main() -> tuple[float, BaseException | None]:
            """Connect, make a request that never gets answered, wait for death."""
            server = await start_server(raw_server(never_answer), HOST, ANY_PORT)
            client = await connect(
                HOST,
                server.port,
                heartbeat_interval=INTERVAL,
                max_missed_pongs=MAX_MISSED,
                setup=lambda peer: peer.on_close(lambda cause: closes.append((now(), cause))),
            )
            began = now()
            pending = spawn(client.request("anything", timeout=None))
            try:
                await wait_for(client.wait_closed(), FAKE_DEADLINE)
                with pytest.raises(ConnectionClosedError) as info:
                    await pending
                return began, info.value.__cause__
            finally:
                server.close()
                await server.wait_closed()

        began, request_cause = fake_run(main)
        assert len(closes) == 1
        when, cause = closes[0]
        assert isinstance(cause, HeartbeatTimeoutError)
        assert isinstance(cause, ConnectionClosedError)
        assert request_cause is cause
        assert math.isclose(when - began, (MAX_MISSED + 1) * INTERVAL)

    def test_stuck_send_does_not_stop_heartbeat(self) -> None:
        """Declare a remote that stopped reading dead even while a send is stuck."""

        async def read_nothing(_conn: FramedConnection) -> None:
            """Accept the connection and never read from it."""
            await sleep(FAKE_DEADLINE)

        async def main() -> BaseException | None:
            """Push a huge message the remote never reads; wait for death."""
            server = await start_server(raw_server(read_nothing), HOST, ANY_PORT)
            client = await connect(
                HOST, server.port, heartbeat_interval=INTERVAL, max_missed_pongs=MAX_MISSED
            )
            causes: list[BaseException | None] = []
            client.on_close(causes.append)
            try:
                with pytest.raises(ConnectionClosedError):
                    await wait_for(client.push("big", attachments=HUGE), FAKE_DEADLINE)
                await wait_for(client.wait_closed(), FAKE_DEADLINE)
                return causes[0]
            finally:
                server.close()
                await server.wait_closed()

        assert isinstance(fake_run(main), HeartbeatTimeoutError)


class TestPingsNeverAnswerRequests:
    """Test that heartbeat traffic is kept apart from request ids."""

    def test_pong_with_request_id_does_not_settle_request(self) -> None:
        """Ignore a pong that carries a pending request's id; wait for the real answer."""

        async def pong_then_answer(conn: FramedConnection) -> None:
            """Answer a request with a decoy pong first, then the real response."""
            decoder = MessageDecoder()
            while True:
                msg = await receive(conn, decoder)
                if msg.type is MessageType.REQUEST:
                    await send_message(conn, Message(MessageType.PONG, msg.id))
                    await send_message(conn, Message(MessageType.RESPONSE, msg.id, body=BODY))

        async def main() -> Message:
            """Make one request."""
            server = await start_server(raw_server(pong_then_answer), HOST, ANY_PORT)
            client = await connect(HOST, server.port, heartbeat_interval=None)
            try:
                return await client.request("anything", timeout=FAKE_DEADLINE)
            finally:
                await client.close()
                server.close()
                await server.wait_closed()

        reply = fake_run(main)
        assert reply.type is MessageType.RESPONSE
        assert reply.body == BODY

    def test_malformed_ping_and_push_with_id_get_no_error_reply(self) -> None:
        """Report, never answer, a broken ping or push even if it carries an id."""
        reported: list[ErrorContext] = []

        async def main() -> Message:
            """Send a broken ping and push, then a good request; return the first reply."""
            get_running_loop().set_exception_handler(reported.append)
            server = await serve(HOST, ANY_PORT, lambda _peer: None, heartbeat_interval=None)
            reader, writer = await open_connection(HOST, server.port)
            conn, decoder = FramedConnection(reader, writer), MessageDecoder()
            try:
                for kind in ("ping", "push"):
                    envelope = {"v": BAD_VERSION, "type": kind, "id": REQUEST_ID, "method": "m"}
                    await conn.send_frame(KIND_JSON + json.dumps(envelope).encode())
                good = {"v": PROTOCOL_VERSION, "type": "request", "id": REQUEST_ID + 1}
                await conn.send_frame(KIND_JSON + json.dumps(good | {"method": "nope"}).encode())
                return await wait_for(receive(conn, decoder), FAKE_DEADLINE)
            finally:
                conn.close()
                await conn.wait_closed()
                server.close()
                await server.wait_closed()

        reply = fake_run(main)
        assert reply.type is MessageType.ERROR
        assert reply.id == REQUEST_ID + 1
        expected_reports = 2
        assert len(reported) == expected_reports


class TestExample:
    """Test the heartbeat demo."""

    def test_heartbeat_demo_main(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Run the demo: pings flow, then the dead server is noticed."""
        spec = importlib.util.spec_from_file_location("heartbeat_demo", EXAMPLE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.main()
        out = capsys.readouterr().out
        assert "pong" in out
        assert "HeartbeatTimeoutError" in out
