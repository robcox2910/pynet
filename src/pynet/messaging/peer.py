"""Walkie-talkies with call signs -- say who you are, what you want, and who answers.

A ``Peer`` is one end of a live connection. It sends requests and waits
for the matching answer, runs a handler task for every incoming request
(so a slow call never holds up a fast one), delivers one-way pushes one
at a time in arrival order, and turns every failure into something the
caller can see:

- A request for a method nobody registered gets an ``error`` reply with
  code ``unknown_method``; a handler that raises gets ``handler_error``
  (and the exception is reported to the loop). The connection carries on.
- A frame that breaks the protocol gets an ``error`` reply only when its
  ``type`` is ``request`` and it has a usable integer ``id``
  (``bad_message`` or ``unsupported_version``); anything else -- a broken
  push, ping, pong, response or a frame with no readable type -- is
  reported instead, so a reply never lands on an id the sender did not
  use for a call. Either way the connection stays open. Only framing
  errors (a truncated or oversized frame) end it.
- The heartbeat: every ``heartbeat_interval`` seconds (starting at once)
  the peer sends a ``ping`` with no ``id`` -- so it can never be mistaken
  for a call -- and a sequence number as its ``body``. An incoming
  ``ping`` is answered with a ``pong`` straight from the reader, echoing
  its ``id`` and ``body``. Each ``pong`` resets the count of unanswered
  pings to zero (other traffic does not count: only a pong proves the
  other end is still answering). When, at a ping time, more than
  ``max_missed_pongs`` pings in a row have gone unanswered, the peer
  closes with ``HeartbeatTimeoutError``; a silent remote is therefore
  declared dead ``(max_missed_pongs + 1) * heartbeat_interval`` seconds
  after the first ping.
- Every message goes out whole or not at all: frames over the size limit
  are refused before anything is written.
- When the connection ends -- either side closes, or the socket dies --
  every waiting ``request`` (even one still sending) fails with
  ``ConnectionClosedError`` and the ``on_close`` callbacks run once with
  the cause (``None`` when we closed). Closing never waits for a remote
  that has stopped reading.
"""

import inspect
import itertools
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Any

from pynet.errors import (
    ConnectionClosedError,
    FrameTooLargeError,
    HeartbeatTimeoutError,
    ProtocolError,
    RemoteError,
)
from pynet.framing import FramedConnection
from pynet.loop.control import wait_for
from pynet.loop.futures import Cancelled, Future
from pynet.loop.reactor import ErrorContext
from pynet.loop.sync import Event, Lock
from pynet.loop.tasks import Task, current_task, get_running_loop, sleep, spawn
from pynet.messaging.codec import (
    KIND_JSON,
    KIND_SIZE,
    PROTOCOL_VERSION,
    JsonValue,
    Message,
    MessageDecoder,
    MessageType,
    encode_message,
)
from pynet.streams import Server, StreamReader, StreamWriter, open_connection, start_server

logger = logging.getLogger("pynet.messaging")

DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_HEARTBEAT_INTERVAL = 5.0
DEFAULT_MAX_MISSED_PONGS = 3
FIRST_REQUEST_ID = 1
FIRST_PING = 1

UNKNOWN_METHOD = "unknown_method"
HANDLER_ERROR = "handler_error"
BAD_MESSAGE = "bad_message"
UNSUPPORTED_VERSION = "unsupported_version"

# On a polite local close, buffered bytes get this long to drain before
# the socket is cut anyway (a remote that stopped reading never drains).
CLOSE_GRACE = 1.0

LOCAL_CLOSE_MESSAGE = "peer is closed"
HANDLER_ERROR_MESSAGE = "Exception in message handler"
CLOSE_CALLBACK_MESSAGE = "Exception in on_close callback"
PROTOCOL_ERROR_MESSAGE = "Protocol error"
READER_ERROR_MESSAGE = "Unexpected error reading messages"


@dataclass(frozen=True)
class Reply:
    """A handler's answer when it needs attachments as well as a body.

    Args:
        body: Any JSON-compatible value for the response body.
        attachments: Raw byte payloads to send with the response.

    """

    body: JsonValue = None
    attachments: tuple[bytes, ...] = ()


type Handler = Callable[[Message], Awaitable[JsonValue | Reply]]
type CloseCallback = Callable[[BaseException | None], None]


def _error_body(code: str, message: str) -> dict[str, JsonValue]:
    """Return the body of an ``error`` message."""
    return {"code": code, "message": message}


def _remote_error(body: JsonValue) -> RemoteError:
    """Build a ``RemoteError`` from an ``error`` message's body."""
    if isinstance(body, dict):
        code, message = body.get("code"), body.get("message")
        if isinstance(code, str) and isinstance(message, str):
            return RemoteError(code, message)
    return RemoteError(BAD_MESSAGE, f"malformed error body: {body!r}")


def _answerable(frame: bytes) -> tuple[int, str] | None:
    """Return ``(id, code)`` for an error reply to a bad frame, or ``None``.

    Only a JSON envelope whose ``type`` is ``request`` and whose ``id`` is
    an integer can be answered: that id is one the sender picked for a
    call and is waiting on. Any other broken frame (a push, ping, pong,
    response, error, or one whose type cannot be read) is never answered,
    because an error with its id could fail an unrelated call.
    """
    if frame[:KIND_SIZE] != KIND_JSON:
        return None
    try:
        data: JsonValue = json.loads(frame[KIND_SIZE:].decode("utf-8"))
    except ValueError, RecursionError:
        return None
    if not isinstance(data, dict):
        return None
    msg_id = data.get("id")
    if not isinstance(msg_id, int) or isinstance(msg_id, bool):
        return None
    if data.get("type") != MessageType.REQUEST.value:
        return None
    code = UNSUPPORTED_VERSION if data.get("v") != PROTOCOL_VERSION else BAD_MESSAGE
    return msg_id, code


class Peer:
    """One end of a connection that speaks request/response and push.

    Register handlers with ``on`` and close callbacks with ``on_close``,
    then call ``start`` to begin reading. ``connect`` and ``serve`` do the
    wiring for you.

    Args:
        conn: The framed connection to talk over.
        heartbeat_interval: Seconds between heartbeat pings, or ``None``
            to send none (incoming pings are still answered).
        max_missed_pongs: How many pings in a row may go unanswered; one
            more than this and the peer closes with
            ``HeartbeatTimeoutError``.

    Raises:
        ValueError: If ``heartbeat_interval`` is not above zero, or
            ``max_missed_pongs`` is negative.

    """

    def __init__(
        self,
        conn: FramedConnection,
        *,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
        max_missed_pongs: int = DEFAULT_MAX_MISSED_PONGS,
    ) -> None:
        """Wrap ``conn``; nothing is read until ``start`` is called."""
        if heartbeat_interval is not None and heartbeat_interval <= 0:
            message = f"heartbeat_interval must be above zero, not {heartbeat_interval}"
            raise ValueError(message)
        if max_missed_pongs < 0:
            message = f"max_missed_pongs must not be negative, not {max_missed_pongs}"
            raise ValueError(message)
        self._conn = conn
        self._heartbeat_interval = heartbeat_interval
        self._max_missed_pongs = max_missed_pongs
        self._decoder = MessageDecoder()
        self._handlers: dict[str, Handler] = {}
        self._pending: dict[int, Future[Message]] = {}
        self._ids = itertools.count(FIRST_REQUEST_ID)
        self._send_lock = Lock()
        self._sends: set[Future[None]] = set()
        self._reader: Task[None] | None = None
        self._heartbeat: Task[None] | None = None
        self._unanswered = 0
        self._tasks: set[Task[None]] = set()
        self._pushes: deque[tuple[Handler, Message]] = deque()
        self._push_worker: Task[None] | None = None
        self._close_callbacks: list[CloseCallback] = []
        self._closed = False
        self._cause: BaseException | None = None
        self._closed_event = Event()
        self.peername: object = conn.writer.get_extra_info("peername")

    @property
    def closed(self) -> bool:
        """Whether this peer has closed (for any reason)."""
        return self._closed

    def on(self, method: str, handler: Handler) -> None:
        """Run ``handler`` for incoming requests and pushes named ``method``.

        For a request, the handler's return value is the response: a bare
        JSON value becomes the body, a ``Reply`` adds attachments. Raising
        ``RemoteError`` sends that code and message; any other exception
        sends ``handler_error``. For a push the return value is ignored.
        """
        self._handlers[method] = handler

    def on_close(self, cb: CloseCallback) -> None:
        """Call ``cb(cause)`` once when the peer closes.

        ``cause`` is ``None`` for a local ``close()``, otherwise the error
        that ended the connection. If the peer is already closed, ``cb``
        runs straight away.
        """
        if self._closed:
            self._run_close_callback(cb)
        else:
            self._close_callbacks.append(cb)

    def start(self) -> None:
        """Start reading, and the heartbeat if one is set (call once, after setup)."""
        if self._reader is not None or self._closed:
            return
        self._reader = spawn(self._read_loop(), name=f"peer-reader-{self.peername}")
        if self._heartbeat_interval is not None:
            self._heartbeat = spawn(
                self._heartbeat_loop(self._heartbeat_interval),
                name=f"peer-heartbeat-{self.peername}",
            )

    async def request(
        self,
        method: str,
        body: JsonValue = None,
        *,
        attachments: Sequence[bytes] = (),
        timeout: float | None = DEFAULT_REQUEST_TIMEOUT,
    ) -> Message:
        """Call ``method`` on the other end and wait for its answer.

        Args:
            method: The name of the remote handler.
            body: Any JSON-compatible value.
            attachments: Raw byte payloads to send with the request.
            timeout: Seconds to wait for the answer, or ``None`` to wait
                forever. It covers waiting for the answer, not sending.

        Returns:
            The ``response`` message.

        Raises:
            RemoteError: If the other end answered with an ``error``.
            TimeoutError: If no answer came in time (a late one is dropped).
            ConnectionClosedError: If the connection closed first.
            ProtocolError: If ``body`` cannot be encoded as JSON.

        """
        msg_id = next(self._ids)
        answer: Future[Message] = Future(get_running_loop())
        self._pending[msg_id] = answer
        try:
            await self._send(Message(MessageType.REQUEST, msg_id, method, body, tuple(attachments)))
            return await wait_for(answer, timeout)
        finally:
            self._pending.pop(msg_id, None)
            if answer.done() and not answer.cancelled():
                # If the send itself failed, close() may have failed ``answer``
                # too; the caller already has an error, so mark this one seen.
                answer.exception()

    async def push(
        self,
        method: str,
        body: JsonValue = None,
        *,
        attachments: Sequence[bytes] = (),
    ) -> None:
        """Send a one-way message; no answer comes back.

        Raises:
            ConnectionClosedError: If the connection is closed.
            ProtocolError: If ``body`` cannot be encoded as JSON.

        """
        await self._send(Message(MessageType.PUSH, None, method, body, tuple(attachments)))

    async def close(self) -> None:
        """Close the connection (safe to call more than once).

        Stops reading, cancels running handlers, fails waiting requests
        and sends with ``ConnectionClosedError`` and runs ``on_close``
        callbacks with ``None``. Bytes already buffered get ``CLOSE_GRACE``
        seconds to go out; a message still being sent is cut off at once,
        so ``close()`` returns promptly even if the remote stopped reading.
        """
        self._shutdown(None)
        await self.wait_closed()

    async def wait_closed(self) -> None:
        """Wait until the peer has closed and its socket is shut."""
        await self._closed_event.wait()
        await self._conn.wait_closed()

    # -- sending --

    async def _send(self, msg: Message) -> None:
        """Send every frame of ``msg`` back to back, even if we are cancelled.

        The frames are written by a separate task under the send lock, so
        another message's frames can never land in the middle, and
        cancelling the caller cannot leave half a message on the wire.
        Every frame is checked against the size limit before anything is
        written. If the peer closes while we wait (for example because
        the remote stopped reading), the wait ends at once.

        Raises:
            FrameTooLargeError: If a frame is over the limit (nothing sent).
            ConnectionClosedError: If the peer is closed or closes meanwhile.
            ProtocolError: If ``msg`` cannot be encoded.

        """
        frames = encode_message(msg)
        limit = self._conn.max_frame_size
        for frame in frames:
            if len(frame) > limit:
                raise FrameTooLargeError(len(frame), limit)
        if self._closed:
            raise ConnectionClosedError(LOCAL_CLOSE_MESSAGE)
        writer = spawn(self._write_frames(frames))
        done: Future[None] = Future(get_running_loop())

        def relay(task: Future[None]) -> None:
            """Copy the writer task's outcome to ``done`` (if anyone still waits)."""
            error = None if task.cancelled() else task.exception()
            if done.done():
                return
            if error is None:
                done.set_result(None)
            else:
                done.set_exception(error)

        writer.add_done_callback(relay)
        self._sends.add(done)
        try:
            await done
        finally:
            self._sends.discard(done)

    async def _write_frames(self, frames: list[bytes]) -> None:
        """Write ``frames`` while holding the send lock."""
        async with self._send_lock:
            for frame in frames:
                await self._conn.send_frame(frame)

    async def _send_quietly(self, msg: Message) -> None:
        """Send ``msg``; if the connection has gone, there is no one to tell."""
        try:
            await self._send(msg)
        except ConnectionClosedError:
            logger.debug("dropped %s to %s: connection closed", msg.type.value, self.peername)

    # -- heartbeat --

    async def _heartbeat_loop(self, interval: float) -> None:
        """Ping every ``interval`` seconds; close if too many go unanswered.

        Each ping is sent from its own task, so a send stuck behind a big
        message (or a remote that stopped reading) never stops the clock.
        """
        for number in itertools.count(FIRST_PING):
            if self._unanswered > self._max_missed_pongs:
                message = (
                    f"no pong for {self._unanswered} pings in a row "
                    f"({interval}s apart) from {self.peername}"
                )
                self._shutdown(HeartbeatTimeoutError(message))
                return
            self._unanswered += 1
            self._track(self._send_quietly(Message(MessageType.PING, body=number)))
            await sleep(interval)

    # -- receiving --

    async def _read_loop(self) -> None:
        """Read messages until the connection ends, then close the peer."""
        cause: BaseException
        try:
            while True:
                frame = await self._conn.recv_frame()
                try:
                    msg = self._decoder.feed(frame)
                except ProtocolError as error:
                    self._on_protocol_error(frame, error)
                    continue
                if msg is not None:
                    self._dispatch(msg)
        except Cancelled:
            raise
        except (ConnectionClosedError, FrameTooLargeError) as error:
            cause = error
        except Exception as error:  # noqa: BLE001 -- reported, then closes the peer
            self._report(READER_ERROR_MESSAGE, error)
            cause = error
        self._shutdown(cause)

    def _dispatch(self, msg: Message) -> None:
        """Route one finished message."""
        match msg.type:
            case MessageType.REQUEST:
                self._track(self._answer(msg))
            case MessageType.RESPONSE | MessageType.ERROR:
                self._settle(msg)
            case MessageType.PUSH:
                self._deliver(msg)
            case MessageType.PING:
                self._track(self._send_quietly(Message(MessageType.PONG, msg.id, body=msg.body)))
            case MessageType.PONG:
                self._unanswered = 0  # someone is still there

    def _settle(self, msg: Message) -> None:
        """Hand a response or error to the request waiting for it."""
        answer = self._pending.pop(msg.id, None) if msg.id is not None else None
        if answer is None or answer.done():
            logger.warning("ignoring %s for unknown request id %s", msg.type.value, msg.id)
            return
        if msg.type is MessageType.RESPONSE:
            answer.set_result(msg)
        else:
            answer.set_exception(_remote_error(msg.body))

    def _deliver(self, msg: Message) -> None:
        """Queue ``msg`` for its push handler, or log and drop it.

        Pushes on one connection are handled one at a time, in the order
        they arrived, by a single push worker: a push's handler never
        starts before the previous push's handler has finished. (Requests
        are different: each runs in its own task, so a slow call never
        holds up a fast one.)
        """
        method = msg.method or ""
        handler = self._handlers.get(method)
        if handler is None:
            logger.warning("dropping push %r from %s: no handler", method, self.peername)
            return
        self._pushes.append((handler, msg))
        if self._push_worker is None:
            self._push_worker = self._track(self._push_loop())

    async def _push_loop(self) -> None:
        """Run queued push handlers one by one until the queue is empty."""
        try:
            while self._pushes:
                handler, msg = self._pushes.popleft()
                await self._run_push(handler, msg)
        finally:
            self._push_worker = None

    async def _run_push(self, handler: Handler, msg: Message) -> None:
        """Run a push handler, reporting any exception."""
        try:
            await handler(msg)
        except Exception as error:  # noqa: BLE001 -- reported, not swallowed
            self._report(f"{HANDLER_ERROR_MESSAGE} for push {msg.method!r}", error)

    async def _answer(self, msg: Message) -> None:
        """Run the request handler and send its response or error."""
        await self._send_quietly(await self._build_reply(msg))

    async def _build_reply(self, msg: Message) -> Message:
        """Return the response (or error) for request ``msg``."""
        method = msg.method or ""
        handler = self._handlers.get(method)
        if handler is None:
            return self._error_reply(msg.id, UNKNOWN_METHOD, f"no handler for {method!r}")
        try:
            result = await handler(msg)
        except RemoteError as error:
            return self._error_reply(msg.id, error.code, error.remote_message)
        except Exception as error:  # noqa: BLE001 -- reported and sent back
            self._report(f"{HANDLER_ERROR_MESSAGE} for {method!r}", error)
            return self._error_reply(msg.id, HANDLER_ERROR, str(error))
        reply = result if isinstance(result, Reply) else Reply(result)
        response = Message(MessageType.RESPONSE, msg.id, None, reply.body, reply.attachments)
        try:
            encode_message(response)
        except ProtocolError as error:
            self._report(f"{HANDLER_ERROR_MESSAGE} for {method!r}", error)
            return self._error_reply(msg.id, HANDLER_ERROR, str(error))
        return response

    @staticmethod
    def _error_reply(msg_id: int | None, code: str, message: str) -> Message:
        """Build an ``error`` message answering call ``msg_id``."""
        return Message(MessageType.ERROR, msg_id, body=_error_body(code, message))

    def _on_protocol_error(self, frame: bytes, error: ProtocolError) -> None:
        """Answer a bad frame with an error reply if we can, else report it."""
        target = _answerable(frame)
        if target is None:
            self._report(f"{PROTOCOL_ERROR_MESSAGE} from {self.peername}", error)
            return
        msg_id, code = target
        self._track(self._send_quietly(self._error_reply(msg_id, code, str(error))))

    def _track(self, coro: Coroutine[Any, Any, None]) -> Task[None]:
        """Run ``coro`` in its own task, cancelled if the peer closes; return the task."""
        task = spawn(coro)
        self._tasks.add(task)
        task.add_done_callback(lambda _done: self._tasks.discard(task))
        return task

    # -- closing --

    def _shutdown(self, cause: BaseException | None) -> None:
        """Close once: stop tasks, fail waiters, close the socket, run callbacks."""
        if self._closed:
            return
        self._closed = True
        self._cause = cause
        me = current_task()
        for task in [self._reader, self._heartbeat, *self._tasks]:
            if task is not None and task is not me:
                task.cancel()
        reason = LOCAL_CLOSE_MESSAGE if cause is None else f"{LOCAL_CLOSE_MESSAGE}: {cause}"
        pending, self._pending = self._pending, {}
        for waiter in [*pending.values(), *self._sends]:
            if not waiter.done():
                error = ConnectionClosedError(reason)
                error.__cause__ = cause
                waiter.set_exception(error)
        if cause is not None or self._send_lock.locked():
            # The connection is broken, or a send is stuck mid-message:
            # flushing could wait forever, so cut the socket now.
            self._conn.abort()
        else:
            self._conn.close()
            get_running_loop().call_later(CLOSE_GRACE, self._conn.abort)
        callbacks, self._close_callbacks = self._close_callbacks, []
        for cb in callbacks:
            self._run_close_callback(cb)
        self._closed_event.set()

    def _run_close_callback(self, cb: CloseCallback) -> None:
        """Call ``cb`` with the close cause, reporting any exception."""
        try:
            cb(self._cause)
        except Exception as error:  # noqa: BLE001 -- reported, not swallowed
            self._report(CLOSE_CALLBACK_MESSAGE, error)

    def _report(self, message: str, error: BaseException) -> None:
        """Send ``error`` to the loop's exception handler."""
        get_running_loop().report_error(ErrorContext(f"{message} ({self.peername})", error))


async def connect(
    host: str,
    port: int,
    *,
    setup: Callable[[Peer], None] | None = None,
    **peer_kwargs: Any,
) -> Peer:
    """Connect to ``host:port`` and return a started ``Peer``.

    If ``setup`` (or building the ``Peer``) raises, the new socket is cut
    before the error is passed on, so nothing is left open.

    Args:
        host: The server's host name or address.
        port: The server's port.
        setup: Called with the peer before it starts reading, the place to
            register handlers and close callbacks.
        **peer_kwargs: Passed on to ``Peer`` (for example
            ``heartbeat_interval``).

    Raises:
        NetConnectionError: If the connection cannot be made.

    """
    reader, writer = await open_connection(host, port)
    conn = FramedConnection(reader, writer)
    try:
        peer = Peer(conn, **peer_kwargs)
        if setup is not None:
            setup(peer)
    except BaseException:
        conn.abort()
        raise
    peer.start()
    return peer


async def serve(
    host: str,
    port: int,
    setup: Callable[[Peer], None | Awaitable[None]],
    **peer_kwargs: Any,
) -> Server:
    """Listen on ``host:port`` and give every connection its own ``Peer``.

    Args:
        host: The address to listen on.
        port: The port (0 lets the kernel choose; see ``Server.port``).
        setup: Called (and awaited, if async) with each new peer before it
            starts reading.
        **peer_kwargs: Passed on to every ``Peer``.

    Returns:
        The running server; close it to stop accepting.

    """

    async def handle(reader: StreamReader, writer: StreamWriter) -> None:
        """Run one connection's peer until it closes."""
        peer = Peer(FramedConnection(reader, writer), **peer_kwargs)
        try:
            outcome = setup(peer)
            if inspect.isawaitable(outcome):
                await outcome
            peer.start()
            await peer.wait_closed()
        finally:
            if not peer.closed:
                await peer.close()

    return await start_server(handle, host, port)
