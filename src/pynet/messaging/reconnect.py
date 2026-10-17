"""Redial when the line drops -- like a phone that calls back by itself.

A ``ReconnectingClient`` keeps one ``Peer`` connected to a server. When
the connection is lost (the server restarts, the network blips, the
heartbeat declares the other end dead) it waits a little and dials
again, waiting longer after each failure:

- **Exponential backoff.** Wait ``initial_backoff`` seconds after the
  first failure, then twice that, then twice again, never more than
  ``max_backoff``. A server that is down for a long time is not hammered.
- **Jitter.** Each wait is nudged up or down by up to ``jitter`` (a
  fraction, 0.1 means +/-10%), so a hundred agents that lost the same
  server do not all redial at the same instant.
- **Reset on success.** After a good connection the next wait starts
  from ``initial_backoff`` again.

The retry loop is a single task: dial; on success announce ``CONNECTED``
(the first time) or ``RECONNECTED`` and wait for the peer to close,
announcing ``DISCONNECTED`` when it does; sleep the backoff; dial again.
A dial that never connects just sleeps and retries -- there was no
connection, so there is no ``DISCONNECTED``.

While the connection is down, ``request`` and ``push`` raise
``NotConnectedError`` at once. Nothing is queued or replayed: the caller
knows best whether a call is safe to repeat.
"""

import logging
import random
from collections.abc import Callable, Sequence
from enum import StrEnum
from typing import Any

from pynet.errors import DNSError, NetConnectionError, NotConnectedError
from pynet.loop.control import wait_for
from pynet.loop.reactor import ErrorContext
from pynet.loop.sync import Event
from pynet.loop.tasks import Task, get_running_loop, sleep, spawn
from pynet.messaging.codec import JsonValue, Message
from pynet.messaging.peer import DEFAULT_REQUEST_TIMEOUT, Peer, connect

logger = logging.getLogger("pynet.messaging")

DEFAULT_INITIAL_BACKOFF = 0.5
DEFAULT_MAX_BACKOFF = 30.0
DEFAULT_JITTER = 0.1
BACKOFF_GROWTH = 2
MAX_JITTER = 1.0

EVENT_CALLBACK_MESSAGE = "Exception in connection event callback"
DIAL_ERROR_MESSAGE = "Unexpected error while connecting"
STOPPED_MESSAGE = "the reconnecting client is stopped"

type EventCallback = Callable[[ConnectionEvent], None]


class ConnectionEvent(StrEnum):
    """What just happened to a ``ReconnectingClient``'s connection."""

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTED = "reconnected"


def backoff_delay(
    attempt: int,
    *,
    initial: float,
    maximum: float,
    jitter: float,
    rng: random.Random,
) -> float:
    """Return how long to wait before redial number ``attempt`` (from 0).

    The delay is ``min(maximum, initial * 2**attempt)``, then multiplied
    by a random factor between ``1 - jitter`` and ``1 + jitter``.

    Args:
        attempt: How many redials in a row have already failed.
        initial: The first delay, in seconds.
        maximum: The largest delay before jitter, in seconds.
        jitter: The fraction to spread each delay by (0 for none).
        rng: Where the random factor comes from (seed it in tests).

    """
    try:
        base = min(maximum, initial * BACKOFF_GROWTH**attempt)
    except OverflowError:  # 2**attempt is too big for a float: long past the cap
        base = maximum
    return base * (1 + rng.uniform(-jitter, jitter))


class ReconnectingClient:
    """Keep a ``Peer`` connected to ``host:port``, redialling when it drops.

    Call ``start`` to begin dialling and ``stop`` to give up for good.
    ``setup`` runs on every new ``Peer`` (before it starts reading), so
    the handlers it registers are there again after each reconnect.

    Args:
        host: The server's host name or address.
        port: The server's port.
        setup: Called with each new peer, the place to register handlers.
        initial_backoff: Seconds to wait after the first failure.
        max_backoff: The longest wait between redials (before jitter).
        jitter: The fraction each wait is randomly spread by, from 0 up
            to (but not including) 1.
        rng: The random source for jitter; a fresh ``random.Random()``
            if not given.
        **peer_kwargs: Passed on to every ``Peer`` (for example
            ``heartbeat_interval``).

    Raises:
        ValueError: If ``initial_backoff`` is not above zero,
            ``max_backoff`` is below it, or ``jitter`` is outside [0, 1).

    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        setup: Callable[[Peer], None] | None = None,
        initial_backoff: float = DEFAULT_INITIAL_BACKOFF,
        max_backoff: float = DEFAULT_MAX_BACKOFF,
        jitter: float = DEFAULT_JITTER,
        rng: random.Random | None = None,
        **peer_kwargs: Any,
    ) -> None:
        """Remember where to dial; nothing happens until ``start``."""
        if initial_backoff <= 0:
            message = f"initial_backoff must be above zero, not {initial_backoff}"
            raise ValueError(message)
        if max_backoff < initial_backoff:
            message = f"max_backoff ({max_backoff}) must not be below initial_backoff"
            raise ValueError(message)
        if not 0 <= jitter < MAX_JITTER:
            message = f"jitter must be at least 0 and below {MAX_JITTER}, not {jitter}"
            raise ValueError(message)
        self._host = host
        self._port = port
        self._setup = setup
        self._initial = initial_backoff
        self._maximum = max_backoff
        self._jitter = jitter
        # Jitter only spreads redials out; it needs no cryptographic strength.
        self._rng = rng if rng is not None else random.Random()  # noqa: S311
        self._peer_kwargs = peer_kwargs
        self._callbacks: list[EventCallback] = []
        self._peer: Peer | None = None
        self._task: Task[None] | None = None
        self._ever_connected = False
        self._stopped = False
        self._changed = Event()  # set while connected, and once stopped

    @property
    def connected(self) -> bool:
        """Whether there is a live connection right now."""
        return self._peer is not None and not self._peer.closed

    @property
    def peer(self) -> Peer | None:
        """The current ``Peer``, or ``None`` while disconnected."""
        return self._peer if self.connected else None

    def on_event(self, cb: EventCallback) -> None:
        """Call ``cb(event)`` every time the connection comes or goes.

        Callbacks run synchronously, in the order they were added. One
        that raises is reported to the loop and the others still run.
        """
        self._callbacks.append(cb)

    def start(self) -> None:
        """Start dialling in the background (a second call does nothing).

        Raises:
            RuntimeError: If the client has been stopped.

        """
        if self._stopped:
            raise RuntimeError(STOPPED_MESSAGE)
        if self._task is None:
            self._task = spawn(self._run(), name=f"reconnect-{self._host}:{self._port}")

    async def stop(self) -> None:
        """Stop redialling and close the connection (safe to call twice).

        The retry loop and any backoff sleep are cancelled. If a peer was
        connected it is closed, which announces one last ``DISCONNECTED``;
        after that no more events come. Anyone in ``wait_connected`` gets
        ``NotConnectedError``.
        """
        if self._stopped:
            return
        self._stopped = True
        task, self._task = self._task, None
        if task is not None:
            finished = Event()
            task.add_done_callback(lambda _task: finished.set())
            task.cancel()
            await finished.wait()
        peer = self._peer
        if peer is not None:
            await peer.close()
        self._changed.set()

    async def wait_connected(self, timeout: float | None = None) -> None:
        """Wait until the client is connected (at once if it already is).

        Args:
            timeout: Seconds to wait, or ``None`` to wait as long as it takes.

        Raises:
            TimeoutError: If no connection came within ``timeout``.
            NotConnectedError: If the client is (or gets) stopped.

        """
        await wait_for(self._until_connected(), timeout)

    async def request(
        self,
        method: str,
        body: JsonValue = None,
        *,
        attachments: Sequence[bytes] = (),
        timeout: float | None = DEFAULT_REQUEST_TIMEOUT,
    ) -> Message:
        """Call ``method`` over the current connection (see ``Peer.request``).

        Raises:
            NotConnectedError: If there is no connection right now.

        """
        peer = self._require_peer()
        return await peer.request(method, body, attachments=attachments, timeout=timeout)

    async def push(
        self,
        method: str,
        body: JsonValue = None,
        *,
        attachments: Sequence[bytes] = (),
    ) -> None:
        """Send a one-way message over the current connection (see ``Peer.push``).

        Raises:
            NotConnectedError: If there is no connection right now.

        """
        await self._require_peer().push(method, body, attachments=attachments)

    # -- the retry loop --

    async def _run(self) -> None:
        """Dial, wait for the line to drop, back off, and dial again -- forever."""
        attempt = 0
        while True:
            peer = await self._dial()
            if peer is not None:
                attempt = 0
                await peer.wait_closed()
            delay = backoff_delay(
                attempt,
                initial=self._initial,
                maximum=self._maximum,
                jitter=self._jitter,
                rng=self._rng,
            )
            logger.debug("redialling %s:%s in %.2fs", self._host, self._port, delay)
            await sleep(delay)
            attempt += 1

    async def _dial(self) -> Peer | None:
        """Try one connection; return the new peer, or ``None`` if it failed."""
        try:
            peer = await connect(
                self._host, self._port, setup=self._setup_peer, **self._peer_kwargs
            )
        except (NetConnectionError, DNSError, OSError) as error:
            logger.debug("could not reach %s:%s: %s", self._host, self._port, error)
            return None
        except Exception as error:  # noqa: BLE001 -- reported, then retried
            self._report(DIAL_ERROR_MESSAGE, error)
            return None
        self._peer = peer
        event = ConnectionEvent.RECONNECTED if self._ever_connected else ConnectionEvent.CONNECTED
        self._ever_connected = True
        self._changed.set()
        self._emit(event)
        return peer

    def _setup_peer(self, peer: Peer) -> None:
        """Watch ``peer`` for closing, then run the caller's setup."""
        peer.on_close(lambda cause: self._lost(peer, cause))
        if self._setup is not None:
            self._setup(peer)

    def _lost(self, peer: Peer, cause: BaseException | None) -> None:
        """Forget ``peer`` once it closes and announce ``DISCONNECTED``."""
        if self._peer is not peer:
            return  # not the peer we announced: nothing to report
        self._peer = None
        if not self._stopped:
            self._changed.clear()
        logger.info("lost connection to %s:%s: %s", self._host, self._port, cause)
        self._emit(ConnectionEvent.DISCONNECTED)

    # -- helpers --

    async def _until_connected(self) -> None:
        """Return once connected; raise ``NotConnectedError`` if stopped."""
        while not self.connected:
            if self._stopped:
                raise NotConnectedError(STOPPED_MESSAGE)
            await self._changed.wait()

    def _require_peer(self) -> Peer:
        """Return the live peer, or raise ``NotConnectedError``."""
        peer = self.peer
        if peer is None:
            message = f"not connected to {self._host}:{self._port}"
            raise NotConnectedError(message)
        return peer

    def _emit(self, event: ConnectionEvent) -> None:
        """Run every event callback, reporting any that raise."""
        for cb in list(self._callbacks):
            try:
                cb(event)
            except Exception as error:  # noqa: BLE001 -- reported, not swallowed
                self._report(f"{EVENT_CALLBACK_MESSAGE} ({event})", error)

    def _report(self, message: str, error: BaseException) -> None:
        """Send ``error`` to the loop's exception handler."""
        context = f"{message} ({self._host}:{self._port})"
        get_running_loop().report_error(ErrorContext(context, error))
