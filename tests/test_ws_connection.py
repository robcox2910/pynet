"""Test WebSocket connections, the server endpoint and the client endpoint."""

import importlib.util
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest

from pynet.errors import ConnectionClosedError, HandshakeError
from pynet.loop import Event, run, sleep, spawn, wait_for
from pynet.streams import StreamReader, StreamWriter, open_connection, start_server
from pynet.ws import (
    CLOSE_GOING_AWAY,
    CLOSE_INTERNAL_ERROR,
    CLOSE_INVALID_UTF8,
    CLOSE_NORMAL,
    CLOSE_PROTOCOL_ERROR,
    CLOSE_TOO_BIG,
    MASK_KEY_SIZE,
    Frame,
    Opcode,
    WebSocket,
    WebSocketClosed,
    client_handshake_request,
    connect_websocket,
    encode_ws_frame,
    generate_key,
    parse_http_head,
    read_ws_frame,
    serve_websocket,
    server_handshake_response,
    validate_client_handshake,
)
from pynet.ws import connection as connection_module

HOST = "127.0.0.1"
ANY_PORT = 0
LONG = 2.0
TINY_TIMEOUT = 0.2
HEAD_END = b"\r\n\r\n"
LIMIT = 1 << 20
SMALL_MESSAGE_LIMIT = 8
CODE_BYTES = 2
GREETING = "hello"
BINARY_DATA = bytes(range(256))
HEL = b"Hel"
LO = b"lo"
HELLO = "Hello"
PING_DATA = b"are you there?"
BYE = "bye"
BAD_UTF8 = b"\xff\xfe"
TOO_BIG = b"x" * (SMALL_MESSAGE_LIMIT + 1)
HALF_BIG = b"x" * (SMALL_MESSAGE_LIMIT // 2 + 1)
NO_STATUS = 1005
ABNORMAL = 1006
APP_CODE = 4000
RESERVED_CODE = 1004
ONE_BYTE = b"\x03"
BAD_REQUEST = b"POST / HTTP/1.1\r\nHost: x\r\n\r\n"
OK_RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
WRONG_ACCEPT_RESPONSE = (
    b"HTTP/1.1 101 Switching Protocols\r\n"
    b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
    b"Sec-WebSocket-Accept: bm90IHRoZSByaWdodCBhbnN3ZXI=\r\n\r\n"
)
LONG_REASON = "r" * 124
TICK = 0.01
OVERSIZE_HEAD = b"GET / HTTP/1.1\r\nX-Padding: " + b"p" * (16 * 1024 + 1)
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "ws_echo.py"


class Recorder:
    """Echo every message back and remember how the connection ended."""

    def __init__(self) -> None:
        """Start with nothing received."""
        self.messages: list[str | bytes] = []
        self.ws: WebSocket | None = None
        self.closed: WebSocketClosed | None = None
        self.done = Event()

    async def __call__(self, ws: WebSocket) -> None:
        """Echo until the connection closes, then record why."""
        self.ws = ws
        try:
            while True:
                message = await ws.recv()
                self.messages.append(message)
                if isinstance(message, str):
                    await ws.send_text(message)
                else:
                    await ws.send_bytes(message)
        except WebSocketClosed as closed:
            self.closed = closed
        finally:
            self.done.set()

    async def wait_closed(self) -> WebSocketClosed:
        """Wait for the server side to finish and return how it ended."""
        await self.done.wait()
        assert self.closed is not None
        return self.closed


type Client[T] = Callable[[int, Recorder], Awaitable[T]]


def serve_and[T](client: Client[T], *, max_message_size: int = LIMIT) -> T:
    """Run an echo WebSocket server and ``client(port, recorder)`` against it."""

    async def main() -> T:
        """Serve on an ephemeral port while the client runs."""
        recorder = Recorder()
        server = await serve_websocket(recorder, HOST, ANY_PORT, max_message_size=max_message_size)
        async with server:
            return await wait_for(client(server.port, recorder), LONG)

    return run(main())


async def raw_dial(port: int) -> tuple[StreamReader, StreamWriter]:
    """Open a connection and do the handshake by hand, returning raw streams."""
    reader, writer = await open_connection(HOST, port)
    writer.write(client_handshake_request(HOST, "/", generate_key()))
    await writer.drain()
    head = await reader.read_until(HEAD_END)
    assert parse_http_head(head)[1] == "101"
    return reader, writer


def send_raw(writer: StreamWriter, frame: Frame, *, masked: bool = True) -> None:
    """Write ``frame`` as a client would (masked unless told otherwise)."""
    key = os.urandom(MASK_KEY_SIZE) if masked else None
    writer.write(encode_ws_frame(frame, mask_key=key))


async def read_server_frame(reader: StreamReader) -> Frame:
    """Read one (unmasked) frame sent by the server."""
    return await read_ws_frame(reader, max_payload=LIMIT, expect_masked=False)


async def read_close_code(reader: StreamReader) -> int:
    """Read the server's CLOSE frame and return its status code."""
    frame = await read_server_frame(reader)
    assert frame.opcode is Opcode.CLOSE
    return int.from_bytes(frame.payload[:CODE_BYTES], "big")


def close_payload(code: int, reason: str = "") -> bytes:
    """Build a CLOSE frame payload."""
    return code.to_bytes(CODE_BYTES, "big") + reason.encode()


class TestMessages:
    """Test sending and receiving whole messages."""

    def test_text_echo(self) -> None:
        """Get a text message echoed back as text."""

        async def client(port: int, _recorder: Recorder) -> str | bytes:
            """Send one text message and read the reply."""
            ws = await connect_websocket(HOST, port)
            await ws.send_text(GREETING)
            reply = await ws.recv()
            await ws.close()
            return reply

        assert serve_and(client) == GREETING

    def test_binary_echo(self) -> None:
        """Get a binary message echoed back as bytes."""

        async def client(port: int, _recorder: Recorder) -> str | bytes:
            """Send one binary message and read the reply."""
            ws = await connect_websocket(HOST, port)
            await ws.send_bytes(BINARY_DATA)
            reply = await ws.recv()
            await ws.close()
            return reply

        assert serve_and(client) == BINARY_DATA

    def test_fragmented_message_reassembled(self) -> None:
        """Join a TEXT fin=0 frame and a CONTINUATION fin=1 frame into one message."""

        async def client(port: int, _recorder: Recorder) -> Frame:
            """Send two fragments and read the echo."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(False, Opcode.TEXT, HEL))
            send_raw(writer, Frame(True, Opcode.CONTINUATION, LO))
            return await read_server_frame(reader)

        assert serve_and(client) == Frame(True, Opcode.TEXT, HELLO.encode())

    def test_control_frame_between_fragments(self) -> None:
        """Answer a ping that arrives in the middle of a fragmented message."""

        async def client(port: int, _recorder: Recorder) -> tuple[Frame, Frame]:
            """Send fragment, ping, fragment; read the pong and the echo."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(False, Opcode.TEXT, HEL))
            send_raw(writer, Frame(True, Opcode.PING, PING_DATA))
            send_raw(writer, Frame(True, Opcode.CONTINUATION, LO))
            return await read_server_frame(reader), await read_server_frame(reader)

        pong, echo = serve_and(client)
        assert pong == Frame(True, Opcode.PONG, PING_DATA)
        assert echo == Frame(True, Opcode.TEXT, HELLO.encode())

    def test_ping_answered_with_pong(self) -> None:
        """Reply to a ping with a pong carrying the same bytes."""

        async def client(port: int, _recorder: Recorder) -> Frame:
            """Send a ping and read the answer."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.PING, PING_DATA))
            return await read_server_frame(reader)

        assert serve_and(client) == Frame(True, Opcode.PONG, PING_DATA)

    def test_client_ping_and_unsolicited_pong_ignored(self) -> None:
        """Skip the pong to our ping (and an unsolicited one) and get the next message."""

        async def client(port: int, _recorder: Recorder) -> str | bytes:
            """Ping, then send a message; recv must skip the pong."""
            ws = await connect_websocket(HOST, port)
            await ws.ping(PING_DATA)
            await ws.send_text(GREETING)
            reply = await ws.recv()
            await ws.close()
            return reply

        assert serve_and(client) == GREETING

    def test_client_masks_every_frame(self) -> None:
        """Send masked frames from ``connect_websocket`` (the server demands it)."""

        async def handler(reader: StreamReader, writer: StreamWriter) -> None:
            """Accept by hand and check the next frame is masked."""
            method, _target, headers = parse_http_head(await reader.read_until(HEAD_END))
            writer.write(server_handshake_response(validate_client_handshake(method, headers)))
            frame = await read_ws_frame(reader, max_payload=LIMIT, expect_masked=True)
            writer.write(encode_ws_frame(frame))

        async def main() -> str | bytes:
            """Dial the hand-made server and read its echo."""
            async with await start_server(handler, HOST, ANY_PORT) as server:
                ws = await connect_websocket(HOST, server.port)
                await ws.send_text(GREETING)
                return await wait_for(ws.recv(), LONG)

        assert run(main()) == GREETING


class TestClosing:
    """Test the closing handshake and close codes."""

    def test_close_handshake_codes(self) -> None:
        """Close with 1000 "bye" and see the server receive exactly that."""

        async def client(port: int, recorder: Recorder) -> tuple[WebSocket, WebSocketClosed]:
            """Close from the client and return both sides' views."""
            ws = await connect_websocket(HOST, port)
            await ws.close(CLOSE_NORMAL, BYE)
            return ws, await recorder.wait_closed()

        ws, closed = serve_and(client)
        assert (closed.code, closed.reason) == (CLOSE_NORMAL, BYE)
        assert ws.closed
        assert ws.close_code == CLOSE_NORMAL

    def test_server_echoes_close_code(self) -> None:
        """Echo the peer's close code back in the server's CLOSE frame."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send CLOSE 1001 by hand and read the reply."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.CLOSE, close_payload(CLOSE_GOING_AWAY)))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_GOING_AWAY

    def test_empty_close_means_no_status(self) -> None:
        """Report code 1005 for an empty CLOSE, and reply with an empty CLOSE."""

        async def client(port: int, recorder: Recorder) -> tuple[Frame, WebSocketClosed]:
            """Send an empty CLOSE and read the reply."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.CLOSE, b""))
            return await read_server_frame(reader), await recorder.wait_closed()

        reply, closed = serve_and(client)
        assert reply == Frame(True, Opcode.CLOSE, b"")
        assert closed.code == NO_STATUS

    def test_one_byte_close_is_protocol_error(self) -> None:
        """Answer a 1-byte CLOSE payload with 1002."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send a 1-byte CLOSE and read the reply."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.CLOSE, ONE_BYTE))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_PROTOCOL_ERROR

    def test_reserved_close_code_is_protocol_error(self) -> None:
        """Answer a CLOSE carrying a reserved code with 1002."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send CLOSE 1004 and read the reply."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.CLOSE, close_payload(RESERVED_CODE)))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_PROTOCOL_ERROR

    def test_bad_utf8_close_reason_closes_1007(self) -> None:
        """Answer a CLOSE whose reason is not UTF-8 with 1007."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send CLOSE with a broken reason and read the reply."""
            reader, writer = await raw_dial(port)
            payload = close_payload(CLOSE_NORMAL) + BAD_UTF8
            send_raw(writer, Frame(True, Opcode.CLOSE, payload))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_INVALID_UTF8

    def test_server_initiated_close(self) -> None:
        """See the client's recv raise with the server's code."""

        async def handler(ws: WebSocket) -> None:
            """Say goodbye straight away."""
            await ws.close(APP_CODE, BYE)

        async def main() -> WebSocketClosed:
            """Dial and wait for the goodbye."""
            async with await serve_websocket(handler, HOST, ANY_PORT) as server:
                ws = await connect_websocket(HOST, server.port)
                with pytest.raises(WebSocketClosed) as info:
                    await wait_for(ws.recv(), LONG)
                return info.value

        closed = run(main())
        assert (closed.code, closed.reason) == (APP_CODE, BYE)

    def test_closed_is_sticky(self) -> None:
        """Raise ``WebSocketClosed`` again from recv and sends after closing."""

        async def client(port: int, _recorder: Recorder) -> WebSocket:
            """Close, then try to keep using the socket."""
            ws = await connect_websocket(HOST, port)
            await ws.close()
            await ws.close()  # idempotent
            for attempt in (ws.recv(), ws.send_text(GREETING), ws.send_bytes(b""), ws.ping()):
                with pytest.raises(WebSocketClosed):
                    await attempt
            return ws

        assert serve_and(client).close_code == CLOSE_NORMAL

    def test_abrupt_eof_is_1006(self) -> None:
        """Report 1006 when the peer vanishes without a CLOSE frame."""

        async def client(port: int, recorder: Recorder) -> WebSocketClosed:
            """Hang up without saying goodbye."""
            _reader, writer = await raw_dial(port)
            writer.abort()
            return await recorder.wait_closed()

        assert serve_and(client).code == ABNORMAL

    def test_close_times_out_without_reply(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Refuse sends while closing; give up waiting for the peer after the timeout."""
        monkeypatch.setattr(connection_module, "CLOSE_TIMEOUT", TINY_TIMEOUT)

        async def handler(reader: StreamReader, writer: StreamWriter) -> None:
            """Accept, then ignore everything."""
            method, _target, headers = parse_http_head(await reader.read_until(HEAD_END))
            writer.write(server_handshake_response(validate_client_handshake(method, headers)))
            await reader.read_exactly(LIMIT)

        async def main() -> WebSocket:
            """Dial the silent server and close."""
            async with await start_server(handler, HOST, ANY_PORT) as server:
                ws = await connect_websocket(HOST, server.port)
                closer = spawn(ws.close())
                await sleep(TICK)
                with pytest.raises(WebSocketClosed, match="already been called"):
                    await ws.send_text(GREETING)
                await wait_for(closer, LONG)
                return ws

        ws = run(main())
        assert ws.closed
        assert ws.close_code == ABNORMAL

    def test_close_rejects_bad_arguments(self) -> None:
        """Refuse a reserved close code or a reason that does not fit."""

        async def client(port: int, _recorder: Recorder) -> None:
            """Try bad closes, then a good one."""
            ws = await connect_websocket(HOST, port)
            with pytest.raises(ValueError, match="close code"):
                await ws.close(NO_STATUS)
            with pytest.raises(ValueError, match="reason"):
                await ws.close(CLOSE_NORMAL, LONG_REASON)
            await ws.close()

        serve_and(client)

    def test_handler_crash_closes_1011(self) -> None:
        """Close with 1011 when the server's handler raises."""

        async def handler(_ws: WebSocket) -> None:
            """Crash at once."""
            message = "handler exploded"
            raise RuntimeError(message)

        async def main() -> int:
            """Dial and wait for the server to hang up."""
            async with await serve_websocket(handler, HOST, ANY_PORT) as server:
                ws = await connect_websocket(HOST, server.port)
                with pytest.raises(WebSocketClosed) as info:
                    await wait_for(ws.recv(), LONG)
                return info.value.code

        assert run(main()) == CLOSE_INTERNAL_ERROR

    def test_close_while_another_task_receives(self) -> None:
        """Let ``close()`` wait for a busy ``recv()``; refuse a second reader and late sends."""

        async def client(port: int, _recorder: Recorder) -> tuple[WebSocket, int]:
            """Receive in one task, close from another."""
            ws = await connect_websocket(HOST, port)
            receiver = spawn(ws.recv())
            await sleep(TICK)
            with pytest.raises(RuntimeError, match="already running"):
                await ws.recv()
            closer = spawn(ws.close())
            await sleep(TICK)
            await ws.close()  # a second close waits too, then returns
            with pytest.raises(WebSocketClosed):
                await ws.send_text(GREETING)
            await closer
            with pytest.raises(WebSocketClosed) as info:
                await receiver
            return ws, info.value.code

        ws, code = serve_and(client)
        assert ws.closed
        assert code == CLOSE_NORMAL

    def test_websocket_closed_is_a_connection_error(self) -> None:
        """Let callers catch ``WebSocketClosed`` as ``ConnectionClosedError``."""
        closed = WebSocketClosed(CLOSE_NORMAL, BYE)
        assert isinstance(closed, ConnectionClosedError)
        assert (closed.code, closed.reason) == (CLOSE_NORMAL, BYE)


class TestProtocolErrors:
    """Test that protocol violations close the connection with the right code."""

    def test_invalid_utf8_closes_1007(self) -> None:
        """Close with 1007 when a text message is not valid UTF-8."""

        async def client(port: int, recorder: Recorder) -> tuple[int, int]:
            """Send broken UTF-8 and read the CLOSE."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.TEXT, BAD_UTF8))
            return await read_close_code(reader), (await recorder.wait_closed()).code

        assert serve_and(client) == (CLOSE_INVALID_UTF8, CLOSE_INVALID_UTF8)

    def test_oversize_message_closes_1009(self) -> None:
        """Close with 1009 when a single frame is over the message limit."""

        async def client(port: int, recorder: Recorder) -> tuple[int, int]:
            """Send a too-big message and read the CLOSE."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.BINARY, TOO_BIG))
            return await read_close_code(reader), (await recorder.wait_closed()).code

        result = serve_and(client, max_message_size=SMALL_MESSAGE_LIMIT)
        assert result == (CLOSE_TOO_BIG, CLOSE_TOO_BIG)

    def test_oversize_fragments_close_1009(self) -> None:
        """Close with 1009 when fragments add up to more than the limit."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send two halves that together are too big."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(False, Opcode.BINARY, HALF_BIG))
            send_raw(writer, Frame(True, Opcode.CONTINUATION, HALF_BIG))
            return await read_close_code(reader)

        assert serve_and(client, max_message_size=SMALL_MESSAGE_LIMIT) == CLOSE_TOO_BIG

    def test_unmasked_client_frame_closes_1002(self) -> None:
        """Close with 1002 when a client forgets to mask."""

        async def client(port: int, recorder: Recorder) -> tuple[int, int]:
            """Send an unmasked frame and read the CLOSE."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.TEXT, HEL), masked=False)
            return await read_close_code(reader), (await recorder.wait_closed()).code

        assert serve_and(client) == (CLOSE_PROTOCOL_ERROR, CLOSE_PROTOCOL_ERROR)

    def test_continuation_without_start_closes_1002(self) -> None:
        """Close with 1002 for a CONTINUATION frame with nothing to continue."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Send a lonely continuation."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(True, Opcode.CONTINUATION, LO))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_PROTOCOL_ERROR

    def test_new_message_mid_fragment_closes_1002(self) -> None:
        """Close with 1002 for a new TEXT frame while a message is unfinished."""

        async def client(port: int, _recorder: Recorder) -> int:
            """Start a message, then start another."""
            reader, writer = await raw_dial(port)
            send_raw(writer, Frame(False, Opcode.TEXT, HEL))
            send_raw(writer, Frame(True, Opcode.TEXT, LO))
            return await read_close_code(reader)

        assert serve_and(client) == CLOSE_PROTOCOL_ERROR


class TestHandshake:
    """Test the endpoints' handling of bad handshakes."""

    def test_bad_handshake_gets_400(self) -> None:
        """Answer a non-upgrade request with 400 Bad Request and hang up."""

        async def client(port: int, _recorder: Recorder) -> tuple[str, dict[str, str], bytes]:
            """Send a POST and read the whole answer."""
            reader, writer = await open_connection(HOST, port)
            writer.write(BAD_REQUEST)
            await writer.drain()
            _version, status, headers = parse_http_head(await reader.read_until(HEAD_END))
            return status, headers, await reader.read()

        status, headers, rest = serve_and(client)
        assert status == "400"
        assert headers["content-length"] == "0"
        assert headers["connection"] == "close"
        assert rest == b""

    def test_oversize_head_gets_400(self) -> None:
        """Answer a request head over the size limit with 400 Bad Request."""

        async def client(port: int, _recorder: Recorder) -> str:
            """Send a huge head and read the status."""
            reader, writer = await open_connection(HOST, port)
            writer.write(OVERSIZE_HEAD)
            await writer.drain()
            return parse_http_head(await reader.read_until(HEAD_END))[1]

        assert serve_and(client) == "400"

    def test_client_rejects_non_101(self) -> None:
        """Raise ``HandshakeError`` when the server does not switch protocols."""
        with pytest.raises(HandshakeError, match="200"):
            run(_dial_fake_server(OK_RESPONSE))

    def test_client_rejects_wrong_accept(self) -> None:
        """Raise ``HandshakeError`` when the accept value does not match the key."""
        with pytest.raises(HandshakeError, match="Accept"):
            run(_dial_fake_server(WRONG_ACCEPT_RESPONSE))

    def test_client_rejects_server_hanging_up(self) -> None:
        """Raise ``HandshakeError`` when the server closes before answering."""
        with pytest.raises(HandshakeError, match="closed"):
            run(_dial_fake_server(b""))


async def _dial_fake_server(response: bytes) -> WebSocket:
    """Dial a server that answers any request with ``response`` and hangs up."""

    async def handler(reader: StreamReader, writer: StreamWriter) -> None:
        """Read the request head and send the canned answer."""
        await reader.read_until(HEAD_END)
        writer.write(response)

    async with await start_server(handler, HOST, ANY_PORT) as server:
        return await wait_for(connect_websocket(HOST, server.port), LONG)


class TestExample:
    """Test the ``examples/ws_echo.py`` handler."""

    def test_example_echoes(self) -> None:
        """Get text and bytes back from the example's ``echo`` handler."""
        spec = importlib.util.spec_from_file_location("ws_echo", EXAMPLE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        async def main() -> tuple[str | bytes, str | bytes]:
            """Serve the example handler and talk to it."""
            async with await serve_websocket(module.echo, HOST, ANY_PORT) as server:
                ws = await connect_websocket(HOST, server.port)
                await ws.send_text(GREETING)
                text = await wait_for(ws.recv(), LONG)
                await ws.send_bytes(BINARY_DATA)
                data = await wait_for(ws.recv(), LONG)
                await ws.close()
                return text, data

        assert run(main()) == (GREETING, BINARY_DATA)
