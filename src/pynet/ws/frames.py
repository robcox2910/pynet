"""Envelopes for words -- the WebSocket frame, RFC 6455 section 5.

Every message on a WebSocket travels in one or more *frames*: a small
header (is this the last piece? what kind? how long? scrambled?) followed
by the payload. This module turns a ``Frame`` into bytes and back.

Clients must scramble ("mask") what they send with a random 4-byte key;
servers must not. Masking is not secrecy -- the key travels in the frame
-- it just stops a client's bytes being mistaken for HTTP by a dumb proxy.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum

from pynet.errors import ConnectionClosedError, FrameTooLargeError, ProtocolError
from pynet.streams import IncompleteReadError, StreamReader

MAX_CONTROL_PAYLOAD = 125
MASK_KEY_SIZE = 4

_FIN_BIT = 0x80
_RSV_BITS = 0x70
_OPCODE_MASK = 0x0F
_MASK_BIT = 0x80
_LENGTH_MASK = 0x7F
_LEN_16 = 126
_LEN_64 = 127
_MAX_7_BIT = 125
_MAX_16_BIT = 0xFFFF
_TOP_BIT_64 = 1 << 63
_CONTROL_FLAG = 0x8


class Opcode(IntEnum):
    """The kind of data a frame carries."""

    CONTINUATION = 0x0
    TEXT = 0x1
    BINARY = 0x2
    CLOSE = 0x8
    PING = 0x9
    PONG = 0xA


@dataclass(frozen=True)
class Frame:
    """One WebSocket frame.

    Args:
        fin: Whether this is the final piece of its message.
        opcode: What kind of frame it is.
        payload: The (unmasked) application bytes.

    """

    fin: bool
    opcode: Opcode
    payload: bytes


def _is_control(opcode: int) -> bool:
    """Return whether ``opcode`` is a control opcode (high bit of the nibble set)."""
    return bool(opcode & _CONTROL_FLAG)


def _check_key(key: bytes) -> None:
    """Raise ``ValueError`` unless ``key`` is exactly ``MASK_KEY_SIZE`` bytes."""
    if len(key) != MASK_KEY_SIZE:
        message = f"mask key must be {MASK_KEY_SIZE} bytes, got {len(key)}"
        raise ValueError(message)


def apply_mask(data: bytes, key: bytes) -> bytes:
    """Return ``data`` XORed with ``key`` repeated; applying it twice undoes it.

    Byte ``i`` is XORed with ``key[i % 4]``. Instead of looping in Python,
    the key is repeated to the data's length and both are treated as one
    giant integer, so a single ``^`` does all the work in C.

    Raises:
        ValueError: If ``key`` is not 4 bytes.

    """
    _check_key(key)
    size = len(data)
    if not size:
        return b""
    repeated = (key * (size // MASK_KEY_SIZE + 1))[:size]
    value = int.from_bytes(data, "big") ^ int.from_bytes(repeated, "big")
    return value.to_bytes(size, "big")


def encode_ws_frame(frame: Frame, *, mask_key: bytes | None = None) -> bytes:
    """Return the wire bytes for ``frame``, masked when ``mask_key`` is given.

    Raises:
        ProtocolError: If a control frame is fragmented or over 125 bytes.
        ValueError: If ``mask_key`` is not 4 bytes.

    """
    size = len(frame.payload)
    if _is_control(frame.opcode) and (size > MAX_CONTROL_PAYLOAD or not frame.fin):
        message = "control frames must be final and carry at most 125 bytes"
        raise ProtocolError(message)
    if mask_key is not None:
        _check_key(mask_key)
    first = (_FIN_BIT if frame.fin else 0) | int(frame.opcode)
    mask_flag = _MASK_BIT if mask_key is not None else 0
    if size <= _MAX_7_BIT:
        header = struct.pack(">BB", first, mask_flag | size)
    elif size <= _MAX_16_BIT:
        header = struct.pack(">BBH", first, mask_flag | _LEN_16, size)
    else:
        header = struct.pack(">BBQ", first, mask_flag | _LEN_64, size)
    if mask_key is None:
        return header + frame.payload
    return header + mask_key + apply_mask(frame.payload, mask_key)


async def _read_length(reader: StreamReader, marker: int) -> int:
    """Return the payload length announced by the 7-bit ``marker``."""
    if marker < _LEN_16:
        return marker
    if marker == _LEN_16:
        (length,) = struct.unpack(">H", await reader.read_exactly(2))
        if length <= _MAX_7_BIT:
            message = "length is not minimally encoded (16-bit form for < 126)"
            raise ProtocolError(message)
        return length
    (length,) = struct.unpack(">Q", await reader.read_exactly(8))
    if length & _TOP_BIT_64:
        message = "64-bit length has its most significant bit set"
        raise ProtocolError(message)
    if length <= _MAX_16_BIT:
        message = "length is not minimally encoded (64-bit form for < 65536)"
        raise ProtocolError(message)
    return length


async def read_ws_frame(
    reader: StreamReader,
    *,
    max_payload: int,
    expect_masked: bool,
) -> Frame:
    """Read and return one frame, unmasking the payload when needed.

    The length is checked against ``max_payload`` before any payload byte
    is read, so a lying header cannot make us buffer gigabytes.

    Args:
        reader: Where the bytes come from.
        max_payload: Largest payload accepted, in bytes.
        expect_masked: Whether the peer must mask (servers expect True,
            clients False).

    Raises:
        ProtocolError: For reserved bits, an unknown opcode, a bad control
            frame, a non-minimal or invalid length, or masking that does
            not match ``expect_masked``.
        FrameTooLargeError: If the payload is over ``max_payload``.
        ConnectionClosedError: If the stream ends before the frame does.

    """
    try:
        return await _read_frame(reader, max_payload, expect_masked)
    except IncompleteReadError as error:
        message = "connection closed in the middle of a frame"
        raise ConnectionClosedError(message) from error


async def _read_frame(reader: StreamReader, max_payload: int, expect_masked: bool) -> Frame:
    """Do the work of ``read_ws_frame`` (may raise ``IncompleteReadError``)."""
    first, second = await reader.read_exactly(2)
    if first & _RSV_BITS:
        message = "reserved bits must be zero"
        raise ProtocolError(message)
    try:
        opcode = Opcode(first & _OPCODE_MASK)
    except ValueError:
        message = f"unknown opcode {first & _OPCODE_MASK:#x}"
        raise ProtocolError(message) from None
    fin = bool(first & _FIN_BIT)
    masked = bool(second & _MASK_BIT)
    if masked != expect_masked:
        message = "frame is masked but must not be" if masked else "frame must be masked"
        raise ProtocolError(message)
    length = await _read_length(reader, second & _LENGTH_MASK)
    if _is_control(opcode) and (length > MAX_CONTROL_PAYLOAD or not fin):
        message = "control frames must be final and carry at most 125 bytes"
        raise ProtocolError(message)
    if length > max_payload:
        raise FrameTooLargeError(length, max_payload)
    key = await reader.read_exactly(MASK_KEY_SIZE) if masked else b""
    payload = await reader.read_exactly(length)
    return Frame(fin, opcode, apply_mask(payload, key) if masked else payload)
