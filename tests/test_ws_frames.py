"""Test WebSocket frame encoding, masking and decoding (RFC 6455 section 5)."""

import os
import threading

import pytest

from pynet.errors import ConnectionClosedError, FrameTooLargeError, ProtocolError
from pynet.loop import run
from pynet.streams import pipe_reader
from pynet.ws import (
    MASK_KEY_SIZE,
    MAX_CONTROL_PAYLOAD,
    Frame,
    Opcode,
    apply_mask,
    encode_ws_frame,
    read_ws_frame,
)

UNMASKED_HELLO = bytes([0x81, 0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F])
MASK = bytes([0x37, 0xFA, 0x21, 0x3D])
MASKED_HELLO = bytes([0x81, 0x85, 0x37, 0xFA, 0x21, 0x3D, 0x7F, 0x9F, 0x4D, 0x51, 0x58])
HELLO = b"Hello"
LIMIT = 1 << 20
SMALL_LIMIT = 10
MEDIUM_PAYLOAD = 256
LARGE_PAYLOAD = 65536
LEN_16_MARKER = 126
LEN_64_MARKER = 127
MIN_16_LENGTH = 126
HEADER_16_SIZE = 4
HEADER_64_SIZE = 10
MASK_BIT = 0x80
BIG_MASK_PAYLOAD = 1 << 20
ODD_PAYLOAD = 7
OVER_CONTROL = MAX_CONTROL_PAYLOAD + 1
TOP_BIT_LENGTH = bytes([0x82, 127, 0x80]) + bytes(7)
HUGE_LENGTH = bytes([0x82, 127]) + (1 << 40).to_bytes(8, "big")
CLAIMED_SIZE = 1 << 40
UNKNOWN_OPCODE = 0x3
BAD_KEY = b"abc"


async def read_from(
    data: bytes,
    *,
    max_payload: int = LIMIT,
    expect_masked: bool = False,
) -> Frame:
    """Feed ``data`` through a pipe and decode one frame."""
    read_fd, write_fd = os.pipe()

    def feed() -> None:
        """Write everything, then close so the reader sees EOF."""
        with os.fdopen(write_fd, "wb") as sink:
            sink.write(data)

    threading.Thread(target=feed, daemon=True).start()
    return await read_ws_frame(
        pipe_reader(read_fd), max_payload=max_payload, expect_masked=expect_masked
    )


class TestEncode:
    """Test ``encode_ws_frame``."""

    def test_encode_unmasked_hello(self) -> None:
        """Match the RFC's unmasked example."""
        assert encode_ws_frame(Frame(True, Opcode.TEXT, HELLO)) == UNMASKED_HELLO

    def test_encode_masked_hello(self) -> None:
        """Match the RFC's masked example."""
        assert encode_ws_frame(Frame(True, Opcode.TEXT, HELLO), mask_key=MASK) == MASKED_HELLO

    def test_16_and_64_bit_lengths_roundtrip(self) -> None:
        """Use the 2-byte and 8-byte length forms and decode them back."""
        medium = encode_ws_frame(Frame(True, Opcode.BINARY, bytes(MEDIUM_PAYLOAD)))
        large = encode_ws_frame(Frame(True, Opcode.BINARY, bytes(LARGE_PAYLOAD)))
        assert medium[1] == LEN_16_MARKER
        assert len(medium) == HEADER_16_SIZE + MEDIUM_PAYLOAD
        assert large[1] == LEN_64_MARKER
        assert len(large) == HEADER_64_SIZE + LARGE_PAYLOAD
        assert run(read_from(medium)).payload == bytes(MEDIUM_PAYLOAD)
        assert run(read_from(large)).payload == bytes(LARGE_PAYLOAD)

    def test_continuation_without_fin(self) -> None:
        """Clear the FIN bit for a non-final fragment."""
        data = encode_ws_frame(Frame(False, Opcode.TEXT, HELLO))
        assert data[0] == int(Opcode.TEXT)

    @pytest.mark.parametrize("opcode", [Opcode.CLOSE, Opcode.PING, Opcode.PONG])
    def test_control_frame_too_long(self, opcode: Opcode) -> None:
        """Reject control payloads over 125 bytes."""
        with pytest.raises(ProtocolError):
            encode_ws_frame(Frame(True, opcode, bytes(OVER_CONTROL)))

    def test_control_frame_not_final(self) -> None:
        """Reject fragmented control frames."""
        with pytest.raises(ProtocolError):
            encode_ws_frame(Frame(False, Opcode.PING, b""))

    def test_bad_mask_key_length(self) -> None:
        """Reject a mask key that is not 4 bytes."""
        with pytest.raises(ValueError, match="mask"):
            encode_ws_frame(Frame(True, Opcode.TEXT, HELLO), mask_key=BAD_KEY)


class TestApplyMask:
    """Test ``apply_mask``."""

    def test_rfc_example(self) -> None:
        """Mask the RFC's example payload."""
        assert apply_mask(HELLO, MASK) == MASKED_HELLO[-len(HELLO) :]

    def test_involution(self) -> None:
        """Undo a mask by applying it again."""
        data = bytes(range(256)) * 3 + bytes(ODD_PAYLOAD)
        assert apply_mask(apply_mask(data, MASK), MASK) == data

    def test_empty(self) -> None:
        """Mask nothing into nothing."""
        assert apply_mask(b"", MASK) == b""

    def test_matches_naive_loop_on_large_payload(self) -> None:
        """Agree with the byte-by-byte definition on 1 MiB."""
        data = os.urandom(BIG_MASK_PAYLOAD + ODD_PAYLOAD)
        naive = bytes(b ^ MASK[i % MASK_KEY_SIZE] for i, b in enumerate(data))
        assert apply_mask(data, MASK) == naive

    def test_bad_key(self) -> None:
        """Reject a key that is not 4 bytes."""
        with pytest.raises(ValueError, match="mask"):
            apply_mask(HELLO, BAD_KEY)


class TestRead:
    """Test ``read_ws_frame``."""

    def test_read_masked_frame(self) -> None:
        """Unmask a client frame."""
        frame = run(read_from(MASKED_HELLO, expect_masked=True))
        assert frame == Frame(True, Opcode.TEXT, HELLO)

    def test_read_unmasked_frame(self) -> None:
        """Read a server frame."""
        assert run(read_from(UNMASKED_HELLO)) == Frame(True, Opcode.TEXT, HELLO)

    def test_mask_presence_mismatch(self) -> None:
        """Reject masked when unmasked is expected, and the reverse."""
        with pytest.raises(ProtocolError):
            run(read_from(MASKED_HELLO, expect_masked=False))
        with pytest.raises(ProtocolError):
            run(read_from(UNMASKED_HELLO, expect_masked=True))

    @pytest.mark.parametrize("bit", [0x40, 0x20, 0x10])
    def test_reserved_bits(self, bit: int) -> None:
        """Reject any RSV bit."""
        data = bytes([UNMASKED_HELLO[0] | bit]) + UNMASKED_HELLO[1:]
        with pytest.raises(ProtocolError, match="reserved"):
            run(read_from(data))

    def test_unknown_opcode(self) -> None:
        """Reject an opcode that is not defined."""
        data = bytes([0x80 | UNKNOWN_OPCODE, 0])
        with pytest.raises(ProtocolError, match="opcode"):
            run(read_from(data))

    def test_control_frame_too_long(self) -> None:
        """Reject a ping with a 126-byte payload."""
        data = bytes([0x89, LEN_16_MARKER]) + OVER_CONTROL.to_bytes(2, "big") + bytes(OVER_CONTROL)
        with pytest.raises(ProtocolError):
            run(read_from(data))

    def test_control_frame_fragmented(self) -> None:
        """Reject a ping without FIN."""
        with pytest.raises(ProtocolError):
            run(read_from(bytes([0x09, 0])))

    def test_non_minimal_16_bit_length(self) -> None:
        """Reject the 2-byte form for a short length."""
        data = bytes([0x82, LEN_16_MARKER]) + len(HELLO).to_bytes(2, "big") + HELLO
        with pytest.raises(ProtocolError, match="minimal"):
            run(read_from(data))

    def test_non_minimal_64_bit_length(self) -> None:
        """Reject the 8-byte form for a length under 65536."""
        data = (
            bytes([0x82, LEN_64_MARKER]) + MEDIUM_PAYLOAD.to_bytes(8, "big") + bytes(MEDIUM_PAYLOAD)
        )
        with pytest.raises(ProtocolError, match="minimal"):
            run(read_from(data))

    def test_top_bit_of_64_bit_length(self) -> None:
        """Reject a 64-bit length whose top bit is set."""
        with pytest.raises(ProtocolError):
            run(read_from(TOP_BIT_LENGTH))

    def test_too_large_checked_before_payload(self) -> None:
        """Raise before reading the (missing) payload."""
        with pytest.raises(FrameTooLargeError) as info:
            run(read_from(HUGE_LENGTH, max_payload=SMALL_LIMIT))
        assert info.value.size == CLAIMED_SIZE
        assert info.value.limit == SMALL_LIMIT

    def test_eof_mid_frame(self) -> None:
        """Surface a truncated frame as ``ConnectionClosedError``."""
        with pytest.raises(ConnectionClosedError):
            run(read_from(UNMASKED_HELLO[:-2]))

    def test_eof_before_frame(self) -> None:
        """Surface EOF at a frame boundary as ``ConnectionClosedError`` too."""
        with pytest.raises(ConnectionClosedError):
            run(read_from(b""))

    def test_masked_roundtrip_with_control(self) -> None:
        """Round-trip a masked ping."""
        frame = Frame(True, Opcode.PING, HELLO)
        data = encode_ws_frame(frame, mask_key=MASK)
        assert data[1] & MASK_BIT
        assert run(read_from(data, expect_masked=True)) == frame
