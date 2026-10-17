"""Answering and dialling -- WebSocket servers and clients.

The handshake module knows the words of "hello, it's me"; this module
picks up the actual phone. ``accept_websocket`` answers a call whose
request head someone else already read (PyWeb does this when a route
asks to upgrade). ``serve_websocket`` is a whole switchboard that reads
the head itself. ``connect_websocket`` dials out.
"""

from collections.abc import Awaitable, Callable, Mapping

from pynet.errors import HandshakeError
from pynet.streams import (
    IncompleteReadError,
    LimitOverrunError,
    Server,
    StreamReader,
    StreamWriter,
    open_connection,
    start_server,
)
from pynet.ws.connection import (
    CLOSE_INTERNAL_ERROR,
    DEFAULT_WS_MAX_MESSAGE,
    WebSocket,
    WebSocketClosed,
)
from pynet.ws.handshake import (
    client_handshake_request,
    compute_accept,
    generate_key,
    parse_http_head,
    server_handshake_response,
    validate_client_handshake,
)

type WebSocketHandler = Callable[[WebSocket], Awaitable[None]]

HEAD_END = b"\r\n\r\n"
MAX_HEAD_SIZE = 16 * 1024
SWITCHING_PROTOCOLS = "101"
BAD_REQUEST_RESPONSE = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"


async def _read_head(reader: StreamReader) -> bytes:
    """Read an HTTP head up to its blank line, leaving later bytes for frames.

    Raises:
        HandshakeError: If the head is too long or the stream ends first.

    """
    try:
        return await reader.read_until(HEAD_END, limit=MAX_HEAD_SIZE)
    except LimitOverrunError as error:
        message = f"HTTP head is over {MAX_HEAD_SIZE} bytes"
        raise HandshakeError(message) from error
    except IncompleteReadError as error:
        message = "connection closed before the HTTP head was complete"
        raise HandshakeError(message) from error


async def _reject(writer: StreamWriter) -> None:
    """Send ``400 Bad Request`` and hang up once it has gone out."""
    writer.write(BAD_REQUEST_RESPONSE)
    writer.close()
    await writer.wait_closed()


def _check_answer(head: bytes, key: str) -> None:
    """Check the server's response head answers our ``key`` with ``101``.

    Raises:
        HandshakeError: If the status is not 101 or the accept value is wrong.

    """
    _version, status, headers = parse_http_head(head)
    if status != SWITCHING_PROTOCOLS:
        message = f"server answered {status} instead of {SWITCHING_PROTOCOLS}"
        raise HandshakeError(message)
    if headers.get("sec-websocket-accept") != compute_accept(key):
        message = "wrong or missing Sec-WebSocket-Accept"
        raise HandshakeError(message)


async def accept_websocket(
    reader: StreamReader,
    writer: StreamWriter,
    *,
    method: str,
    headers: Mapping[str, str],
    max_message_size: int = DEFAULT_WS_MAX_MESSAGE,
) -> WebSocket:
    """Answer an upgrade request whose head has already been read.

    Args:
        reader: The connection's reader, positioned just after the head.
        writer: The connection's writer.
        method: The request method.
        headers: The request headers (names in any case).
        max_message_size: The largest message the WebSocket accepts.

    Returns:
        The server side of the new conversation.

    Raises:
        HandshakeError: If the request is not a valid upgrade; a
            ``400 Bad Request`` has been sent and the connection closed.

    """
    try:
        key = validate_client_handshake(method, headers)
    except HandshakeError:
        await _reject(writer)
        raise
    writer.write(server_handshake_response(key))
    await writer.drain()
    return WebSocket(reader, writer, is_client=False, max_message_size=max_message_size)


async def serve_websocket(
    handler: WebSocketHandler,
    host: str,
    port: int,
    *,
    max_message_size: int = DEFAULT_WS_MAX_MESSAGE,
) -> Server:
    """Listen on ``host:port`` and run ``handler(ws)`` for each WebSocket.

    A handler that lets ``WebSocketClosed`` escape is treated as a
    normal hang-up. When the handler returns, the WebSocket is closed
    with 1000; when it raises, with 1011, and the error is reported.
    Bad handshakes get ``400 Bad Request`` and are reported too.

    Args:
        handler: The coroutine function to run per conversation.
        host: The address to listen on.
        port: The port; 0 lets the kernel choose (see ``Server.port``).
        max_message_size: The largest message each WebSocket accepts.

    Returns:
        The running server.

    """

    async def serve(reader: StreamReader, writer: StreamWriter) -> None:
        """Do the handshake, then hand the conversation to ``handler``."""
        try:
            method, _target, headers = parse_http_head(await _read_head(reader))
        except HandshakeError:
            await _reject(writer)
            raise
        ws = await accept_websocket(
            reader, writer, method=method, headers=headers, max_message_size=max_message_size
        )
        try:
            await handler(ws)
        except WebSocketClosed:
            return
        except Exception:
            await ws.close(CLOSE_INTERNAL_ERROR)
            raise
        await ws.close()

    return await start_server(serve, host, port)


async def connect_websocket(
    host: str,
    port: int,
    path: str = "/",
    *,
    max_message_size: int = DEFAULT_WS_MAX_MESSAGE,
) -> WebSocket:
    """Dial a WebSocket server and return the client side of the conversation.

    Args:
        host: The server's name or address.
        port: The server's port.
        path: The request target, such as ``/chat``.
        max_message_size: The largest message the WebSocket accepts.

    Returns:
        The open client WebSocket.

    Raises:
        HandshakeError: If the server does not answer ``101`` with the
            right ``Sec-WebSocket-Accept``.
        NetConnectionError: If the TCP connection cannot be made.

    """
    reader, writer = await open_connection(host, port)
    key = generate_key()
    try:
        writer.write(client_handshake_request(f"{host}:{port}", path, key))
        await writer.drain()
        _check_answer(await _read_head(reader), key)
    except BaseException:
        writer.abort()
        raise
    return WebSocket(reader, writer, is_client=True, max_message_size=max_message_size)
