"""Test length-prefixed framing over real sockets."""

import struct

import pytest

from pynet.errors import ConnectionClosedError, FrameTooLargeError
from pynet.framing import HEADER_SIZE, MAX_FRAME_SIZE, FramedConnection, encode_frame
from pynet.loop import Event, run, sleep, wait_for
from pynet.streams import StreamReader, StreamWriter, open_connection, start_server

HOST = "127.0.0.1"
ANY_PORT = 0
LONG = 2.0
TICK = 0.01
HI = b"hi"
HI_FRAME = b"\x00\x00\x00\x02hi"
FRAMES = [b"one", b"", b"three!", b"x" * 1000]
SMALL_LIMIT = 8
BIG_PAYLOAD = b"y" * (SMALL_LIMIT + 1)
CLAIMED_SIZE = 1_000_000
SPLIT_AT = 2
BODY = b"hello world"
PARTIAL_BODY = b"hel"
LENGTH_FORMAT = ">I"
CLEAN_MESSAGE = "peer closed"


async def connected_pair(
    *,
    client_limit: int = MAX_FRAME_SIZE,
    server_limit: int = MAX_FRAME_SIZE,
) -> tuple[FramedConnection, FramedConnection, StreamWriter, StreamReader]:
    """Return a framed client, framed server end, and the client's raw halves."""
    accepted: list[tuple[StreamReader, StreamWriter]] = []
    release = Event()

    async def handler(reader: StreamReader, writer: StreamWriter) -> None:
        """Hand the server end to the test."""
        accepted.append((reader, writer))
        await release.wait()

    server = await start_server(handler, HOST, ANY_PORT)
    reader, writer = await open_connection(HOST, server.port)
    while not accepted:
        await wait_for(_tick(), LONG)
    server_reader, server_writer = accepted[0]
    client = FramedConnection(reader, writer, max_frame_size=client_limit)
    peer = FramedConnection(server_reader, server_writer, max_frame_size=server_limit)
    return client, peer, writer, reader


async def _tick() -> None:
    """Yield to the loop once."""
    await sleep(TICK)


class TestEncodeFrame:
    """Test building frame bytes."""

    def test_encode_frame_header(self) -> None:
        """Prefix the payload with a 4-byte big-endian length."""
        assert encode_frame(HI) == HI_FRAME
        assert len(struct.pack(LENGTH_FORMAT, 0)) == HEADER_SIZE

    def test_oversize_encode_raises(self) -> None:
        """Refuse a payload over the maximum size."""
        with pytest.raises(FrameTooLargeError) as info:
            encode_frame(BIG_PAYLOAD, max_size=SMALL_LIMIT)
        assert info.value.size == len(BIG_PAYLOAD)
        assert info.value.limit == SMALL_LIMIT


class TestFramedConnection:
    """Test sending and receiving frames."""

    def test_roundtrip_many_frames_in_one_write(self) -> None:
        """Split several frames that arrive in a single write."""

        async def scenario() -> list[bytes]:
            client, peer, writer, _ = await connected_pair()
            writer.write(b"".join(encode_frame(f) for f in FRAMES))
            await writer.drain()
            got = [await wait_for(peer.recv_frame(), LONG) for _ in FRAMES]
            client.close()
            await client.wait_closed()
            return got

        assert run(scenario()) == FRAMES

    def test_send_frame_roundtrip(self) -> None:
        """Deliver frames sent with ``send_frame``."""

        async def scenario() -> list[bytes]:
            client, peer, _, _ = await connected_pair()
            for frame in FRAMES:
                await client.send_frame(frame)
            got = [await wait_for(peer.recv_frame(), LONG) for _ in FRAMES]
            client.close()
            await client.wait_closed()
            return got

        assert run(scenario()) == FRAMES

    def test_frame_split_across_writes(self) -> None:
        """Wait for the rest of a frame that arrives in pieces."""
        data = encode_frame(BODY)

        async def scenario() -> bytes:
            client, peer, writer, _ = await connected_pair()
            writer.write(data[:SPLIT_AT])
            await writer.drain()
            await _tick()
            writer.write(data[SPLIT_AT : HEADER_SIZE + len(PARTIAL_BODY)])
            await writer.drain()
            await _tick()
            writer.write(data[HEADER_SIZE + len(PARTIAL_BODY) :])
            await writer.drain()
            got = await wait_for(peer.recv_frame(), LONG)
            client.close()
            return got

        assert run(scenario()) == BODY

    def test_empty_frame(self) -> None:
        """Round-trip a zero-length payload."""

        async def scenario() -> bytes:
            client, peer, _, _ = await connected_pair()
            await client.send_frame(b"")
            got = await wait_for(peer.recv_frame(), LONG)
            client.close()
            return got

        assert run(scenario()) == b""

    def test_writer_property(self) -> None:
        """Expose the underlying writer."""

        async def scenario() -> bool:
            client, peer, writer, _ = await connected_pair()
            same = client.writer is writer
            client.close()
            peer.close()
            return same

        assert run(scenario())

    def test_oversize_header_closes_connection(self) -> None:
        """Close the writer and raise on a header over the limit, body unread."""

        async def scenario() -> tuple[FrameTooLargeError, bytes]:
            client, peer, writer, reader = await connected_pair(server_limit=SMALL_LIMIT)
            writer.write(struct.pack(LENGTH_FORMAT, CLAIMED_SIZE))
            await writer.drain()
            with pytest.raises(FrameTooLargeError) as info:
                await wait_for(peer.recv_frame(), LONG)
            assert peer.writer.is_closing()
            rest = await wait_for(reader.read(), LONG)
            with pytest.raises(ConnectionClosedError):
                await peer.recv_frame()
            client.close()
            return info.value, rest

        error, rest = run(scenario())
        assert error.size == CLAIMED_SIZE
        assert error.limit == SMALL_LIMIT
        assert rest == b""

    def test_send_oversize_raises(self) -> None:
        """Refuse to send a payload over the connection's limit."""

        async def scenario() -> None:
            client, peer, _, _ = await connected_pair(client_limit=SMALL_LIMIT)
            try:
                with pytest.raises(FrameTooLargeError):
                    await client.send_frame(BIG_PAYLOAD)
            finally:
                client.close()
                peer.close()

        run(scenario())

    def test_clean_eof_vs_mid_frame_eof(self) -> None:
        """Tell a close at a boundary from a close inside a frame."""

        async def clean() -> list[str]:
            client, peer, _, _ = await connected_pair()
            await client.send_frame(HI)
            client.close()
            first = await wait_for(peer.recv_frame(), LONG)
            assert first == HI
            messages: list[str] = []
            for _ in range(2):
                with pytest.raises(ConnectionClosedError) as info:
                    await wait_for(peer.recv_frame(), LONG)
                messages.append(str(info.value))
            return messages

        async def mid_frame() -> str:
            client, peer, writer, _ = await connected_pair()
            writer.write(encode_frame(BODY)[: HEADER_SIZE + len(PARTIAL_BODY)])
            await writer.drain()
            client.close()
            with pytest.raises(ConnectionClosedError) as info:
                await wait_for(peer.recv_frame(), LONG)
            return str(info.value)

        messages = run(clean())
        assert messages == [CLEAN_MESSAGE, CLEAN_MESSAGE]
        assert "mid-frame" in run(mid_frame())

    def test_mid_header_eof(self) -> None:
        """Report a close partway through the header as mid-frame."""

        async def scenario() -> str:
            client, peer, writer, _ = await connected_pair()
            writer.write(HI_FRAME[:SPLIT_AT])
            await writer.drain()
            client.close()
            with pytest.raises(ConnectionClosedError) as info:
                await wait_for(peer.recv_frame(), LONG)
            return str(info.value)

        assert "mid-frame" in run(scenario())
