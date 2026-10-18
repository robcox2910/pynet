"""Property-based tests -- rules that must hold for *every* input.

An ordinary test checks one example someone thought of: "frame the word
``hi``, and the bytes are ``00 00 00 02 h i``". A property test states a
rule instead, such as "however the network chops up the bytes, the
receiver gets back exactly the messages that were sent", and lets
Hypothesis invent hundreds of messages, and hundreds of ways to chop them,
to try to break it. If one breaks the rule, Hypothesis *shrinks* it down to
the smallest input that still fails, and shows you that.

Each test below says its rule in plain English in its docstring. The
strategies at the top are recipes for making random inputs; each one is
named for what it makes.

No test here opens a socket, starts a thread or sleeps: bytes travel
through a ``PretendPipe`` (``tests/pretend_pipe.py``) that hands them over
in exactly the chunks the test chose.

The step-by-step check of a whole conversation against a plain list lives
in ``tests/test_model.py``. See ``docs/property-testing.md`` for the
walk-through.
"""

import base64
import binascii
import contextlib
import ipaddress

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pynet.base64 import b64_decode_bytes, b64_encode
from pynet.dns import is_valid_ipv4
from pynet.errors import (
    ConnectionClosedError,
    FrameTooLargeError,
    HandshakeError,
    HTTPError,
    ProtocolError,
)
from pynet.framing import MAX_FRAME_SIZE, PEER_CLOSED_MESSAGE, encode_frame
from pynet.http import _parse_response
from pynet.messaging import Message, MessageDecoder, encode_message
from pynet.url import parse_url
from pynet.ws import (
    MAX_CONTROL_PAYLOAD,
    Frame,
    FrameReader,
    Opcode,
    encode_ws_frame,
    parse_http_head,
)
from tests.pretend_pipe import FrameInbox, PretendPipe, pretend_reader, read_until_hang_up, run_now
from tests.strategies import PROPERTY_TIMEOUT, chunks_of, messages

# A WebSocket payload over 125 bytes needs a longer length field, so test
# payloads go a little past that; 64 KiB ones (the longest field) would
# make every example slow and add nothing a short one doesn't show.
MOST_WS_PAYLOAD_BYTES = 300
# Junk frame readers accept at most this many payload bytes, so a random
# header claiming more is refused (FrameTooLargeError) instead of waited for.
JUNK_PAYLOAD_LIMIT = 64
WS_LIMIT = 1 << 20
# An over-long octet has this many digits: past int()'s 4,300-digit limit.
OVER_LONG_DIGITS = 5000

pytestmark = PROPERTY_TIMEOUT  # see tests/strategies.py

# --- Strategies: recipes for random inputs -----------------------------------
# messages() and chunks_of() live in tests/strategies.py, shared with the
# model check.


def payloads() -> st.SearchStrategy[list[bytes]]:
    """Make a list of message payloads: any bytes, empty ones included."""
    return st.lists(st.binary())


BASE64_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


@st.composite
def base64_groups(draw: st.DrawFn) -> str:
    """Make a group of four: Base64 letters, then 0 to 3 "=" signs (3 is never right)."""
    padding = draw(st.sampled_from(["", "=", "==", "==="]))
    letters = draw(
        st.text(alphabet=BASE64_LETTERS, min_size=4 - len(padding), max_size=4 - len(padding))
    )
    return letters + padding


def base64ish_text() -> st.SearchStrategy[str]:
    """Make text that is often Base64 and often nearly Base64: odd padding, stray marks."""
    groups = st.lists(base64_groups()).map("".join)
    with_stray_marks = st.text(alphabet=BASE64_LETTERS + "=-_ \né")
    return groups | with_stray_marks


@st.composite
def websocket_frames(draw: st.DrawFn) -> Frame:
    """Make any legal WebSocket frame: control frames are final and at most 125 bytes."""
    # The control-frame rule is RFC 6455 section 5.5; encode_ws_frame refuses the rest.
    opcode = draw(st.sampled_from(Opcode))
    if opcode >= Opcode.CLOSE:
        return Frame(True, opcode, draw(st.binary(max_size=MAX_CONTROL_PAYLOAD)))
    return Frame(draw(st.booleans()), opcode, draw(st.binary(max_size=MOST_WS_PAYLOAD_BYTES)))


def mask_keys() -> st.SearchStrategy[bytes | None]:
    """Make a 4-byte mask key (a client talking) or None (a server talking)."""
    return st.none() | st.binary(min_size=4, max_size=4)


def bricks_joined(bricks: list[bytes]) -> st.SearchStrategy[bytes]:
    """Glue random picks from ``bricks`` (and a few random bytes) into one byte string."""
    return st.lists(st.sampled_from(bricks) | st.binary(max_size=3)).map(b"".join)


def number_ish_bytes() -> st.SearchStrategy[bytes]:
    """Make bytes that are often a number (decimal or hex), and often nearly one."""
    bricks = [b"0", b"5", b"f", b"-", b"+", b"_", b"x", b";", b" ", b"\r\n", b"hello"]
    return bricks_joined(bricks)


def status_lines() -> st.SearchStrategy[bytes]:
    """Make a status line whose code is a number, nearly a number, or missing."""
    with_code = number_ish_bytes().map(b"HTTP/1.1 %b OK\r\n".__mod__)
    return with_code | st.just(b"HTTP/1.1\r\n")


def http_headers() -> st.SearchStrategy[bytes]:
    """Make one header line that changes how the body is read, with an odd value or not."""
    content_length = number_ish_bytes().map(b"Content-Length: %b\r\n".__mod__)
    return st.just(b"Transfer-Encoding: chunked\r\n") | content_length


def http_responses() -> st.SearchStrategy[bytes]:
    """Make junk shaped like an HTTP response: a status line, a header or two, a blank line, a body."""
    return st.tuples(
        status_lines(),
        st.lists(http_headers(), max_size=2).map(b"".join),
        st.sampled_from([b"\r\n", b""]),
        number_ish_bytes(),
    ).map(b"".join)


def http_heads() -> st.SearchStrategy[bytes]:
    """Make junk that often looks like an HTTP head: start lines, header lines, colons."""
    return bricks_joined([b"GET / HTTP/1.1", b"HTTP/1.1 101 OK", b"\r\n", b" ", b":", b"Host"])


def message_frames() -> st.SearchStrategy[list[bytes]]:
    """Make frame payloads that are often nearly messages: J and B frames holding junk."""
    envelope_ish = bricks_joined(
        [
            b"J",
            b"B",
            b"{",
            b"}",
            b'"v":1',
            b'"type":"push"',
            b'"method":"m"',
            b'"attachments":1',
            b",",
        ]
    )
    return st.lists(st.binary() | envelope_ish)


def url_text() -> st.SearchStrategy[str]:
    """Make text that often looks like a URL: schemes, hosts, ports, paths, queries."""
    bricks = [
        "http",
        "://",
        "example.com",
        ":",
        "80",
        "x",
        "/",
        "?",
        "a=1",
        "&",
        "#",
        "=",
        "\u0663",
    ]
    return st.lists(st.sampled_from(bricks) | st.text(max_size=2)).map("".join)


def octet_ish_text() -> st.SearchStrategy[str]:
    """Make one piece of an IPv4 address: often a good number, sometimes an odd one."""
    good = ["0", "1", "25", "255"]
    # "\u0661" is an Arabic-Indic one, "\u00b2" a superscript two.
    # The last one is far longer than any octet: past 4,300 digits, int() itself refuses.
    odd = ["256", "01", "1000", "", "a", " 1", "+1", "\u0661", "\u00b2", "1" * OVER_LONG_DIGITS]
    return st.sampled_from(good) | st.sampled_from(odd)


def ipv4ish_text() -> st.SearchStrategy[str]:
    """Make text that often looks like an IPv4 address: pieces joined with dots, often four."""
    four_pieces = st.lists(octet_ish_text(), min_size=4, max_size=4)
    any_pieces = st.lists(octet_ish_text(), min_size=1, max_size=6)
    return (four_pieces | any_pieces).map(".".join)


# --- Helpers -----------------------------------------------------------------


def stdlib_decode(text: str) -> bytes | None:
    """Decode ``text`` with Python's own ``base64``; None means it refused."""
    try:
        return base64.b64decode(text, validate=True)
    except binascii.Error, ValueError:  # ValueError: text that isn't ASCII
        return None


def our_decode(text: str) -> bytes | None:
    """Decode ``text`` with ``b64_decode_bytes``; None means it raised ValueError."""
    try:
        return b64_decode_bytes(text)
    except ValueError:
        return None


def ipaddress_says_valid(text: str) -> bool:
    """Return whether Python's own ``ipaddress`` accepts ``text`` as an IPv4 address."""
    try:
        ipaddress.IPv4Address(text)
    except ValueError:
        return False
    return True


# --- Base64: same answers as the standard library ----------------------------


@given(data=st.binary())
def test_base64_encode_matches_the_standard_library(data: bytes) -> None:
    """Encoding any bytes gives exactly the text Python's own base64 gives."""
    assert b64_encode(data) == base64.b64encode(data).decode("ascii")


@given(text=base64ish_text())
def test_base64_decode_matches_the_standard_library(text: str) -> None:
    """Any text decodes to the bytes Python's base64 gives, and is refused exactly when it refuses."""
    assert our_decode(text) == stdlib_decode(text)


@given(data=st.binary())
def test_base64_round_trip(data: bytes) -> None:
    """Decoding what we encoded gives back the very same bytes."""
    assert b64_decode_bytes(b64_encode(data)) == data


# --- Round trips across random chunk splits ----------------------------------


@given(sent=payloads(), data=st.data())
def test_frames_survive_any_chunking(sent: list[bytes], data: st.DataObject) -> None:
    """However the bytes are cut up, the receiver gets exactly the frames sent, then a clean close."""
    wire = b"".join(encode_frame(payload) for payload in sent)
    inbox = FrameInbox(max_frame_size=MAX_FRAME_SIZE)
    for chunk in data.draw(chunks_of(wire)):
        inbox.pipe.hand_over(chunk)

    received, goodbye = read_until_hang_up(inbox)

    assert received == sent
    assert goodbye == PEER_CLOSED_MESSAGE


@given(sent=st.lists(websocket_frames()), mask_key=mask_keys(), data=st.data())
def test_websocket_frames_survive_any_chunking(
    sent: list[Frame], mask_key: bytes | None, data: st.DataObject
) -> None:
    """However the bytes are cut up, a FrameReader gets exactly the frames sent, unmasked."""
    wire = b"".join(encode_ws_frame(frame, mask_key=mask_key) for frame in sent)
    pipe = PretendPipe()
    for chunk in data.draw(chunks_of(wire)):
        pipe.hand_over(chunk)
    pipe.hang_up()
    reader = FrameReader(pretend_reader(pipe), expect_masked=mask_key is not None)

    received = [run_now(reader.read(WS_LIMIT)) for _ in sent]

    assert repr(received) == repr(sent)
    with pytest.raises(ConnectionClosedError):  # and nothing extra after them
        run_now(reader.read(WS_LIMIT))


@given(sent=st.lists(messages()))
def test_messages_survive_the_codec(sent: list[Message]) -> None:
    """Feeding every frame encode_message makes to one decoder gives back the same messages."""
    decoder = MessageDecoder()
    received = [
        message
        for msg in sent
        for frame in encode_message(msg)
        if (message := decoder.feed(frame)) is not None
    ]

    assert repr(received) == repr(sent)


# --- Parsers on junk: only the errors they promise ---------------------------
# Each test feeds a parser junk and lets through only the error its
# docstring promises: ``contextlib.suppress(HTTPError)`` means "an
# HTTPError is fine, stop here quietly". There is no assert after it.
# Anything else -- an IndexError, a ValueError, a hang stopped by the
# 10-second timeout -- escapes and fails the test.


@given(raw=st.binary() | http_responses())
def test_http_parser_only_raises_http_error(raw: bytes) -> None:
    """Any bytes at all parse into a Response or raise HTTPError, never anything else."""
    with contextlib.suppress(HTTPError):
        _parse_response(raw)


@given(raw=st.binary() | http_heads())
def test_handshake_parser_only_raises_handshake_error(raw: bytes) -> None:
    """Any bytes at all parse into an HTTP head or raise HandshakeError, never anything else."""
    with contextlib.suppress(HandshakeError):
        parse_http_head(raw)


@given(raw=st.binary(), expect_masked=st.booleans())
def test_websocket_reader_only_raises_its_errors(raw: bytes, expect_masked: bool) -> None:
    """Reading frames from any bytes ends in ProtocolError, FrameTooLargeError or ConnectionClosedError."""
    pipe = PretendPipe()
    if raw:
        pipe.hand_over(raw)
    pipe.hang_up()
    reader = FrameReader(pretend_reader(pipe), expect_masked=expect_masked)
    with contextlib.suppress(ProtocolError, FrameTooLargeError, ConnectionClosedError):
        while True:
            run_now(reader.read(JUNK_PAYLOAD_LIMIT))


@given(frames=message_frames())
def test_message_decoder_only_raises_protocol_error(frames: list[bytes]) -> None:
    """Feeding any frames to a MessageDecoder gives messages, None or ProtocolError, never anything else."""
    decoder = MessageDecoder()
    for frame in frames:
        with contextlib.suppress(ProtocolError):
            decoder.feed(frame)


@given(raw=st.text() | url_text())
def test_url_parser_only_raises_value_error(raw: str) -> None:
    """Any text at all parses into a URL or raises ValueError, never anything else."""
    with contextlib.suppress(ValueError):
        parse_url(raw)


@given(text=st.text() | ipv4ish_text())
def test_ipv4_check_agrees_with_ipaddress(text: str) -> None:
    """is_valid_ipv4 says True exactly when Python's own ipaddress accepts the text."""
    assert is_valid_ipv4(text) == ipaddress_says_valid(text)
