"""Test the reconnecting client: redial with growing, jittered waits.

Time is fake (``FakeClock(autojump=True)``): the sockets are real, but
whenever the loop has nothing to do it jumps straight to the next timer,
so a client that backs off for many seconds is tested in milliseconds.
Servers that "appear later" or "restart" reuse one port: we bind to port
0, note the port the kernel picked, close it, and listen there again.
"""

import random
import socket
from collections.abc import Awaitable, Callable

import pytest

from pynet.errors import NetConnectionError, NotConnectedError
from pynet.framing import FramedConnection
from pynet.loop import ErrorContext, Event, get_running_loop, run, sleep, spawn
from pynet.loop.testing import FakeClock
from pynet.messaging import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_INITIAL_BACKOFF,
    DEFAULT_JITTER,
    DEFAULT_MAX_BACKOFF,
    ConnectionEvent,
    JsonValue,
    Message,
    Peer,
    ReconnectingClient,
    backoff_delay,
    reconnect,
    serve,
)
from pynet.streams import Server, StreamReader, StreamWriter, _socket_streams, start_server

HOST = "127.0.0.1"
ANY_PORT = 0
SEED = 1234
NO_JITTER = 0.0
BODY: dict[str, JsonValue] = {"x": 1}
HELLO = "hi from the client"
# With the defaults and no jitter: 0.5, 1, 2, 4, 8, 16, then capped at 30.
EXPECTED_DELAYS = [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]
JITTER_SAMPLES = 500
JITTER_ATTEMPTS = 12
APPEAR_AT = 2.0
DOWNTIME = 3.0
# The scripted dials: three refusals, a connection that lasts UP_FOR
# seconds, two more refusals, then a connection again. Backoff (no
# jitter) puts the dials at 0, 0.5, 1.5, 3.5; the line drops at 13.5;
# the counter has reset, so the redials are at 14, 15 and 17.
SCRIPT = (False, False, False, True, False, False, True)
UP_FOR = 10.0
EXPECTED_DIALS = [0.0, 0.5, 1.5, 3.5, 14.0, 15.0, 17.0]
EXPECTED_EVENT_TIMES = [3.5, 13.5, 17.0]
STOP_AFTER = 2.0
LONG_WAIT = 100.0
SHORT_TIMEOUT = 1.0
BAD_BACKOFF = 0.0
BAD_JITTER = 1.0
SMALL_MAX = 0.1
HUGE_ATTEMPT = 5000  # 2**5000 does not fit in a float


def fake_run[T](main: Callable[[], Awaitable[T]]) -> T:
    """Run ``main()`` on a loop whose clock jumps whenever it is idle."""

    async def wrapper() -> T:
        """Await the test body."""
        return await main()

    return run(wrapper(), clock=FakeClock(autojump=True))


def now() -> float:
    """Return the running loop's (fake) time."""
    return get_running_loop().time()


async def free_port() -> int:
    """Return a port that nothing is listening on (but that we may reuse)."""

    async def unused(_reader: StreamReader, writer: StreamWriter) -> None:
        """Never called: nobody connects before we close."""
        writer.close()

    server = await start_server(unused, HOST, ANY_PORT)
    port = server.port
    server.close()
    await server.wait_closed()
    return port


async def echo(msg: Message) -> JsonValue:
    """Answer a request with its own body."""
    return msg.body


async def hello(_msg: Message) -> JsonValue:
    """Answer the server's call to the client."""
    return HELLO


class EchoServer:
    """A ``serve()`` server that records every peer it accepts."""

    def __init__(
        self, port: int, heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL
    ) -> None:
        """Remember the ``port``; call ``start`` to listen."""
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.peers: list[Peer] = []
        self.server: Server | None = None

    def setup(self, peer: Peer) -> None:
        """Register the echo handler and remember ``peer``."""
        peer.on("echo", echo)
        self.peers.append(peer)

    async def start(self) -> None:
        """Listen on ``port``."""
        self.server = await serve(
            HOST, self.port, self.setup, heartbeat_interval=self.heartbeat_interval
        )

    async def stop(self) -> None:
        """Stop listening and hang up on every peer."""
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for peer in self.peers:
            await peer.close()


class Recorder:
    """Collect connection events and let a test wait for one."""

    def __init__(self) -> None:
        """Start with no events seen."""
        self.events: list[ConnectionEvent] = []
        self.times: list[float] = []
        self.changed = Event()

    def __call__(self, event: ConnectionEvent) -> None:
        """Record ``event`` and its (fake) time."""
        self.events.append(event)
        self.times.append(now())
        self.changed.set()

    async def wait_for(self, event: ConnectionEvent) -> None:
        """Wait until ``event`` has been seen."""
        while event not in self.events:
            self.changed.clear()
            await self.changed.wait()


def seeded_rng() -> random.Random:
    """Return a seeded random source, so every run sees the same jitter."""
    return random.Random(SEED)  # noqa: S311 -- jitter, not cryptography


def make_client(
    port: int,
    setup: Callable[[Peer], None] | None = None,
    heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
) -> ReconnectingClient:
    """Return a client for ``port`` with a seeded, jitter-free backoff."""
    return ReconnectingClient(
        HOST,
        port,
        setup=setup,
        jitter=NO_JITTER,
        rng=seeded_rng(),
        heartbeat_interval=heartbeat_interval,
    )


class TestBackoffDelay:
    """Test the backoff formula."""

    def test_backoff_doubles_and_caps(self) -> None:
        """Double from 0.5 each attempt and never pass 30 (no jitter)."""
        rng = seeded_rng()
        delays = [
            backoff_delay(
                attempt,
                initial=DEFAULT_INITIAL_BACKOFF,
                maximum=DEFAULT_MAX_BACKOFF,
                jitter=NO_JITTER,
                rng=rng,
            )
            for attempt in range(len(EXPECTED_DELAYS))
        ]
        assert delays == EXPECTED_DELAYS

    def test_backoff_jitter_bounds(self) -> None:
        """Stay within +/- jitter of the base delay, and actually vary."""
        rng = seeded_rng()
        spread: set[float] = set()
        for sample in range(JITTER_SAMPLES):
            attempt = sample % JITTER_ATTEMPTS
            base = min(DEFAULT_MAX_BACKOFF, DEFAULT_INITIAL_BACKOFF * 2**attempt)
            delay = backoff_delay(
                attempt,
                initial=DEFAULT_INITIAL_BACKOFF,
                maximum=DEFAULT_MAX_BACKOFF,
                jitter=DEFAULT_JITTER,
                rng=rng,
            )
            assert base * (1 - DEFAULT_JITTER) <= delay <= base * (1 + DEFAULT_JITTER)
            spread.add(delay / base)
        assert min(spread) < 1 < max(spread)

    def test_backoff_survives_huge_attempt(self) -> None:
        """Return the cap, not OverflowError, after thousands of failures."""
        delay = backoff_delay(
            HUGE_ATTEMPT,
            initial=DEFAULT_INITIAL_BACKOFF,
            maximum=DEFAULT_MAX_BACKOFF,
            jitter=NO_JITTER,
            rng=seeded_rng(),
        )
        assert delay == DEFAULT_MAX_BACKOFF


class TestConnecting:
    """Test that the client connects, retrying until the server is there."""

    def test_connects_when_server_appears_later(self) -> None:
        """Keep redialling with backoff; connect on the first try after it appears."""

        async def main() -> tuple[list[ConnectionEvent], float, Message, bool]:
            """Start the client first, the server two seconds later."""
            port = await free_port()
            server = EchoServer(port)
            recorder = Recorder()
            client = make_client(port)
            client.on_event(recorder)
            began = now()
            client.start()
            await sleep(APPEAR_AT)
            assert not client.connected
            await server.start()
            await client.wait_connected()
            reply = await client.request("echo", BODY)
            connected = client.connected and client.peer is not None
            events = list(recorder.events)
            await client.stop()
            await server.stop()
            return events, recorder.times[0] - began, reply, connected

        events, connected_at, reply, connected = fake_run(main)
        assert events == [ConnectionEvent.CONNECTED]
        assert connected_at >= APPEAR_AT
        assert reply.body == BODY
        assert connected

    def test_reconnects_after_server_restart(self) -> None:
        """Redial a restarted server on the same port; setup's handlers work again."""

        async def main() -> tuple[list[ConnectionEvent], list[JsonValue], int, Message]:
            """Connect late, restart the server on the same port, and connect again."""
            port = await free_port()
            server = EchoServer(port)
            setups: list[Peer] = []

            def setup(peer: Peer) -> None:
                """Register the client-side handler on every new peer."""
                setups.append(peer)
                peer.on("client.hello", hello)

            recorder = Recorder()
            client = make_client(port, setup=setup)
            client.on_event(recorder)
            client.start()
            await sleep(APPEAR_AT)
            await server.start()
            await client.wait_connected()
            await client.request("echo", BODY)  # the server's peer is set up now
            answers = [(await server.peers[-1].request("client.hello")).body]

            await server.stop()
            await recorder.wait_for(ConnectionEvent.DISCONNECTED)
            assert not client.connected
            assert client.peer is None
            await sleep(DOWNTIME)  # the client's redials are refused meanwhile
            await server.start()
            await recorder.wait_for(ConnectionEvent.RECONNECTED)
            reply = await client.request("echo", BODY)
            answers.append((await server.peers[-1].request("client.hello")).body)
            events = list(recorder.events)
            await client.stop()
            await server.stop()
            return events, answers, len(setups), reply

        events, answers, setups, reply = fake_run(main)
        assert events == [
            ConnectionEvent.CONNECTED,
            ConnectionEvent.DISCONNECTED,
            ConnectionEvent.RECONNECTED,
        ]
        assert answers == [HELLO, HELLO]
        assert setups == len(answers)
        assert reply.body == BODY

    def test_backoff_schedule_resets_after_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dial on the backoff schedule, and start it over after a good connection.

        Real sockets cannot pin exact times here: while a real connect or
        close is in flight, an autojump clock may leap to the next timer.
        So ``connect`` is swapped for a script: a refusal raises at once,
        and a success is a real ``Peer`` over a socket pair with no
        heartbeat, which the test closes after ``UP_FOR`` seconds. Only
        timers move the clock, so every time is exact.
        """
        dials: list[float] = []
        script = list(SCRIPT)

        async def scripted_connect(
            _host: str, _port: int, *, setup: Callable[[Peer], None] | None = None, **_kw: object
        ) -> Peer:
            """Refuse or connect, as the script says, recording the time."""
            dials.append(now())
            if not script.pop(0):
                message = "refused (scripted)"
                raise NetConnectionError(message)
            ours, theirs = socket.socketpair()
            sockets.append(theirs)
            reader, writer = _socket_streams(get_running_loop(), ours)
            peer = Peer(FramedConnection(reader, writer), heartbeat_interval=None)
            if setup is not None:
                setup(peer)
            peer.start()
            get_running_loop().call_later(UP_FOR, lambda: spawn(peer.close()))
            return peer

        sockets: list[socket.socket] = []
        monkeypatch.setattr(reconnect, "connect", scripted_connect)

        async def main() -> Recorder:
            """Run the client until the script's second connection."""
            recorder = Recorder()
            client = make_client(ANY_PORT)
            client.on_event(recorder)
            client.start()
            await recorder.wait_for(ConnectionEvent.RECONNECTED)
            await client.stop()
            return recorder

        try:
            recorder = fake_run(main)
        finally:
            for sock in sockets:
                sock.close()
        assert dials == EXPECTED_DIALS
        assert recorder.events == [
            ConnectionEvent.CONNECTED,
            ConnectionEvent.DISCONNECTED,
            ConnectionEvent.RECONNECTED,
            ConnectionEvent.DISCONNECTED,  # from stop()
        ]
        assert recorder.times[: len(EXPECTED_EVENT_TIMES)] == EXPECTED_EVENT_TIMES

    def test_wait_connected_times_out(self) -> None:
        """Raise TimeoutError when no connection comes within the timeout."""

        async def main() -> None:
            """Wait for a client whose server never appears."""
            client = make_client(await free_port())
            client.start()
            try:
                await client.wait_connected(SHORT_TIMEOUT)
            finally:
                await client.stop()

        with pytest.raises(TimeoutError):
            fake_run(main)

    def test_setup_error_reported_and_retried(self) -> None:
        """Report a setup that raises, then redial and connect next time."""
        reported: list[ErrorContext] = []

        async def main() -> list[ConnectionEvent]:
            """Fail setup once, then let it succeed."""
            get_running_loop().set_exception_handler(reported.append)
            port = await free_port()
            server = EchoServer(port)
            await server.start()
            calls: list[Peer] = []

            def setup(peer: Peer) -> None:
                """Raise on the first call only."""
                calls.append(peer)
                if len(calls) == 1:
                    message = "setup broke"
                    raise RuntimeError(message)

            recorder = Recorder()
            client = make_client(port, setup=setup)
            client.on_event(recorder)
            client.start()
            await client.wait_connected()
            events = list(recorder.events)
            await client.stop()
            await server.stop()
            return events

        events = fake_run(main)
        assert events == [ConnectionEvent.CONNECTED]
        assert [type(ctx.exception) for ctx in reported] == [RuntimeError]

    def test_event_callback_error_reported(self) -> None:
        """Report a callback that raises; later callbacks still run."""
        reported: list[ErrorContext] = []

        async def main() -> list[ConnectionEvent]:
            """Register a broken callback before a working one."""
            get_running_loop().set_exception_handler(reported.append)
            port = await free_port()
            server = EchoServer(port)
            await server.start()

            def broken(_event: ConnectionEvent) -> None:
                """Raise every time."""
                message = "callback broke"
                raise RuntimeError(message)

            recorder = Recorder()
            client = make_client(port)
            client.on_event(broken)
            client.on_event(recorder)
            client.start()
            await client.wait_connected()
            events = list(recorder.events)
            await client.stop()
            await server.stop()
            return events

        events = fake_run(main)
        assert events == [ConnectionEvent.CONNECTED]
        assert reported
        assert all(isinstance(ctx.exception, RuntimeError) for ctx in reported)

    def test_rejects_bad_settings(self) -> None:
        """Refuse a non-positive backoff, a max below it, or jitter of 1 or more."""
        with pytest.raises(ValueError, match="initial_backoff"):
            ReconnectingClient(HOST, ANY_PORT, initial_backoff=BAD_BACKOFF)
        with pytest.raises(ValueError, match="max_backoff"):
            ReconnectingClient(HOST, ANY_PORT, max_backoff=SMALL_MAX)
        with pytest.raises(ValueError, match="jitter"):
            ReconnectingClient(HOST, ANY_PORT, jitter=BAD_JITTER)


class TestWhileDown:
    """Test that calls fail fast while there is no connection."""

    def test_request_while_down_raises_not_connected(self) -> None:
        """Raise NotConnectedError from request and push: nothing is queued."""

        async def main() -> list[type[BaseException]]:
            """Call the client before start and while its server is missing."""
            client = make_client(await free_port())
            raised: list[type[BaseException]] = []
            for started in (False, True):
                if started:
                    client.start()
                    await sleep(STOP_AFTER)
                try:
                    await client.request("echo", BODY)
                except NotConnectedError as error:
                    raised.append(type(error))
                try:
                    await client.push("echo", BODY)
                except NotConnectedError as error:
                    raised.append(type(error))
            await client.stop()
            return raised

        assert fake_run(main) == [NotConnectedError] * 4


class TestStopping:
    """Test that stop() ends everything."""

    def test_stop_cancels_retry_loop(self) -> None:
        """Stop redialling: a server that appears after stop() never sees us."""

        async def main() -> tuple[list[ConnectionEvent], int, bool]:
            """Stop while backing off, then start a server and wait."""
            port = await free_port()
            recorder = Recorder()
            client = make_client(port)
            client.on_event(recorder)
            client.start()
            await sleep(STOP_AFTER)
            await client.stop()
            await client.stop()  # idempotent
            server = EchoServer(port)
            await server.start()
            await sleep(LONG_WAIT)
            accepted = len(server.peers)
            await server.stop()
            return recorder.events, accepted, client.connected

        events, accepted, connected = fake_run(main)
        assert events == []
        assert accepted == 0
        assert not connected

    def test_stop_while_connected(self) -> None:
        """Close the peer, send one DISCONNECTED, and never redial."""

        async def main() -> tuple[list[ConnectionEvent], int, bool, bool]:
            """Connect, stop, and see the server's peer close."""
            port = await free_port()
            server = EchoServer(port)
            await server.start()
            recorder = Recorder()
            client = make_client(port)
            client.on_event(recorder)
            client.start()
            await client.wait_connected()
            await client.request("echo", BODY)
            peer = client.peer
            await client.stop()
            await server.peers[0].wait_closed()
            await sleep(LONG_WAIT)
            accepted = len(server.peers)
            await server.stop()
            return recorder.events, accepted, peer is not None and peer.closed, client.connected

        events, accepted, peer_closed, connected = fake_run(main)
        assert events == [ConnectionEvent.CONNECTED, ConnectionEvent.DISCONNECTED]
        assert accepted == 1
        assert peer_closed
        assert not connected

    def test_cannot_restart_after_stop(self) -> None:
        """Refuse start() and wait_connected() once stopped."""

        async def main() -> list[type[BaseException]]:
            """Stop, then try to use the client again."""
            client = make_client(await free_port())
            client.start()
            client.start()  # a second start is ignored
            await client.stop()
            raised: list[type[BaseException]] = []
            try:
                client.start()
            except RuntimeError as error:
                raised.append(type(error))
            try:
                await client.wait_connected()
            except NotConnectedError as error:
                raised.append(type(error))
            return raised

        assert fake_run(main) == [RuntimeError, NotConnectedError]

    def test_stop_wakes_waiters(self) -> None:
        """Wake a pending wait_connected() with NotConnectedError on stop()."""

        async def main() -> type[BaseException] | None:
            """Wait in one task while another stops the client."""
            client = make_client(await free_port())
            client.start()
            outcome: list[type[BaseException]] = []

            async def waiter() -> None:
                """Wait for a connection that never comes."""
                try:
                    await client.wait_connected()
                except NotConnectedError as error:
                    outcome.append(type(error))

            task = spawn(waiter())
            await sleep(STOP_AFTER)
            await client.stop()
            await task
            return outcome[0] if outcome else None

        assert fake_run(main) is NotConnectedError
