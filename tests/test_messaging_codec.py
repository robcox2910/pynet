"""Test the message envelope codec: Message <-> frame payloads."""

import json

import pytest

from pynet.errors import ProtocolError
from pynet.messaging import (
    KIND_BINARY,
    KIND_JSON,
    MAX_NESTING,
    PROTOCOL_VERSION,
    JsonValue,
    Message,
    MessageDecoder,
    MessageType,
    encode_message,
)

REQUEST_ID = 1
OTHER_ID = 7
METHOD = "echo"
BODY: dict[str, JsonValue] = {"x": 1}
BLOB_ONE = b"\x00\xff"
BLOB_TWO = b"second attachment"
TWO_ATTACHMENTS = 2
BAD_VERSION = 99
NEGATIVE_COUNT = -1
NESTING_DEPTH = 100_000
UNICODE_TEXT = "café ☃"


def deeply_nested() -> JsonValue:
    """Return a list nested far deeper than the recursion limit."""
    value: JsonValue = []
    for _ in range(NESTING_DEPTH):
        value = [value]
    return value


def envelope_of(frames: list[bytes]) -> dict[str, object]:
    """Return the decoded JSON envelope from the first frame."""
    return json.loads(frames[0][1:])


def json_frame(envelope: object) -> bytes:
    """Return a JSON-kind frame holding ``envelope``."""
    return KIND_JSON + json.dumps(envelope).encode("utf-8")


def good(**overrides: object) -> dict[str, object]:
    """Return a valid request envelope with ``overrides`` applied."""
    envelope: dict[str, object] = {
        "v": PROTOCOL_VERSION,
        "type": "request",
        "id": REQUEST_ID,
        "method": METHOD,
        "body": None,
    }
    envelope.update(overrides)
    return envelope


def without(envelope: dict[str, object], key: str) -> dict[str, object]:
    """Return ``envelope`` minus ``key``."""
    return {k: v for k, v in envelope.items() if k != key}


class TestEnvelopeShape:
    """Test what encode_message writes."""

    def test_json_envelope_shape(self) -> None:
        """Encode a request as v, type, id, method, body."""
        frames = encode_message(
            Message(MessageType.REQUEST, id=REQUEST_ID, method=METHOD, body=BODY)
        )
        assert frames[0][:1] == KIND_JSON
        assert envelope_of(frames) == {
            "v": PROTOCOL_VERSION,
            "type": "request",
            "id": REQUEST_ID,
            "method": METHOD,
            "body": BODY,
        }
        assert len(frames) == 1

    def test_compact_utf8_json(self) -> None:
        """Write compact JSON with real UTF-8 rather than escapes."""
        frames = encode_message(Message(MessageType.PUSH, method=METHOD, body=UNICODE_TEXT))
        assert b" " not in frames[0][1:].replace(UNICODE_TEXT.encode(), b"")
        assert UNICODE_TEXT.encode("utf-8") in frames[0]

    def test_response_shape(self) -> None:
        """Include id and body but no method on a response."""
        frames = encode_message(Message(MessageType.RESPONSE, id=OTHER_ID, body=BODY))
        assert envelope_of(frames) == {
            "v": PROTOCOL_VERSION,
            "type": "response",
            "id": OTHER_ID,
            "body": BODY,
        }

    def test_error_shape(self) -> None:
        """Include id and body but no method on an error."""
        frames = encode_message(Message(MessageType.ERROR, id=OTHER_ID, body="boom"))
        assert envelope_of(frames) == {
            "v": PROTOCOL_VERSION,
            "type": "error",
            "id": OTHER_ID,
            "body": "boom",
        }

    def test_push_shape(self) -> None:
        """Include method and body but no id on a push."""
        frames = encode_message(Message(MessageType.PUSH, method=METHOD, body=BODY))
        assert envelope_of(frames) == {
            "v": PROTOCOL_VERSION,
            "type": "push",
            "method": METHOD,
            "body": BODY,
        }

    @pytest.mark.parametrize("kind", [MessageType.PING, MessageType.PONG])
    def test_ping_pong_shape(self, kind: MessageType) -> None:
        """Keep only v, type and a null body on ping and pong."""
        frames = encode_message(Message(kind))
        assert envelope_of(frames) == {"v": PROTOCOL_VERSION, "type": kind.value, "body": None}

    def test_attachment_count_and_frames(self) -> None:
        """Declare the count and follow with one binary frame each."""
        frames = encode_message(
            Message(MessageType.PUSH, method=METHOD, attachments=(BLOB_ONE, BLOB_TWO))
        )
        assert envelope_of(frames)["attachments"] == TWO_ATTACHMENTS
        assert frames[1:] == [KIND_BINARY + BLOB_ONE, KIND_BINARY + BLOB_TWO]

    def test_no_attachments_key_when_zero(self) -> None:
        """Omit the attachments key when there are none."""
        frames = encode_message(Message(MessageType.PING))
        assert "attachments" not in envelope_of(frames)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_float_refused(self, bad: float) -> None:
        """Refuse NaN and infinity when encoding."""
        with pytest.raises(ProtocolError):
            encode_message(Message(MessageType.PUSH, method=METHOD, body=bad))

    def test_unserialisable_body_refused(self) -> None:
        """Refuse a body JSON cannot represent."""
        with pytest.raises(ProtocolError):
            encode_message(Message(MessageType.PUSH, method=METHOD, body={1, 2}))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    @pytest.mark.parametrize(
        "message",
        [
            Message(MessageType.PUSH, method=METHOD, body="\ud800"),
            Message(MessageType.PUSH, method="\ud800"),
            Message(MessageType.PUSH, method=METHOD, body=deeply_nested()),
        ],
        ids=["surrogate-body", "surrogate-method", "deeply-nested"],
    )
    def test_unencodable_message_refused(self, message: Message) -> None:
        """Raise ProtocolError, never a raw encoding or recursion error."""
        with pytest.raises(ProtocolError):
            encode_message(message)


class TestRoundTrip:
    """Test encode then decode."""

    def test_body_at_nesting_limit_roundtrips(self) -> None:
        """Accept a body nested exactly MAX_NESTING levels deep."""
        body: JsonValue = None
        for _ in range(MAX_NESTING):
            body = [body]
        msg = Message(MessageType.PUSH, method=METHOD, body=body)
        assert MessageDecoder().feed(encode_message(msg)[0]) == msg

    def test_roundtrip_with_attachments(self) -> None:
        """Rebuild a message and its attachments exactly."""
        message = Message(
            MessageType.REQUEST,
            id=REQUEST_ID,
            method=METHOD,
            body={"list": [1, 2.5, None, True], "s": UNICODE_TEXT},
            attachments=(BLOB_ONE, BLOB_TWO),
        )
        decoder = MessageDecoder()
        results = [decoder.feed(frame) for frame in encode_message(message)]
        assert results == [None, None, message]

    @pytest.mark.parametrize(
        "message",
        [
            Message(MessageType.REQUEST, id=REQUEST_ID, method=METHOD, body=BODY),
            Message(MessageType.RESPONSE, id=OTHER_ID, body=[1, 2]),
            Message(MessageType.ERROR, id=OTHER_ID, body="boom"),
            Message(MessageType.PUSH, method=METHOD),
            Message(MessageType.PING),
            Message(MessageType.PONG),
            Message(MessageType.PONG, id=OTHER_ID, attachments=(b"",)),
        ],
    )
    def test_every_type_roundtrips(self, message: Message) -> None:
        """Round-trip each message type."""
        decoder = MessageDecoder()
        results = [decoder.feed(frame) for frame in encode_message(message)]
        assert results[-1] == message

    def test_decoder_waits_for_attachments(self) -> None:
        """Return None until the last declared attachment arrives."""
        message = Message(MessageType.PUSH, method=METHOD, attachments=(BLOB_ONE, BLOB_TWO))
        first, second, third = encode_message(message)
        decoder = MessageDecoder()
        assert decoder.feed(first) is None
        assert decoder.feed(second) is None
        assert decoder.feed(third) == message

    def test_decoder_handles_back_to_back_messages(self) -> None:
        """Reuse the decoder for consecutive messages."""
        decoder = MessageDecoder()
        ping = Message(MessageType.PING)
        for _ in range(TWO_ATTACHMENTS):
            assert decoder.feed(encode_message(ping)[0]) == ping


class TestProtocolErrors:
    """Test every way a frame can be rejected."""

    @pytest.mark.parametrize(
        "frame",
        [
            b"",
            b"X{}",
            KIND_BINARY + b"orphan",
            KIND_JSON + b"\xff\xfe",
            KIND_JSON + b"{not json",
            KIND_JSON + b"[1, 2]",
            KIND_JSON + b'"text"',
            KIND_JSON + b"null",
            json_frame(good(v=BAD_VERSION)),
            json_frame(without(good(), "v")),
            json_frame(good(type="bogus")),
            json_frame(without(good(), "type")),
            json_frame(without(good(), "id")),
            json_frame(without(good(type="response"), "id")),
            json_frame(without(good(type="error"), "id")),
            json_frame(without(good(), "method")),
            json_frame(without(good(type="push"), "method")),
            json_frame(good(attachments=NEGATIVE_COUNT)),
            json_frame(good(attachments="2")),
            json_frame(good(attachments=1.5)),
            json_frame(good(attachments=True)),
            json_frame(good(id="one")),
            json_frame(good(method=5)),
            KIND_JSON + b'{"v":1,"type":"ping","body":NaN}',
            KIND_JSON + b'{"v":1,"type":"ping","body":Infinity}',
        ],
    )
    def test_bad_frame_raises(self, frame: bytes) -> None:
        """Raise ProtocolError for each malformed frame."""
        with pytest.raises(ProtocolError):
            MessageDecoder().feed(frame)

    def test_closed_deep_nesting_refused(self) -> None:
        """Refuse well-formed JSON that nests deeper than MAX_NESTING."""
        deep = b"[" * (MAX_NESTING + 1) + b"]" * (MAX_NESTING + 1)
        frame = KIND_JSON + b'{"v":1,"type":"push","method":"m","body":' + deep + b"}"
        with pytest.raises(ProtocolError, match="nested deeper"):
            MessageDecoder().feed(frame)

    def test_deeply_nested_json_refused_and_decoder_reusable(self) -> None:
        """Turn a recursion bomb into ProtocolError and keep decoding."""
        decoder = MessageDecoder()
        with pytest.raises(ProtocolError):
            decoder.feed(KIND_JSON + b"[" * NESTING_DEPTH)
        ping = Message(MessageType.PING)
        assert decoder.feed(encode_message(ping)[0]) == ping

    def test_unsupported_version_message(self) -> None:
        """Name the problem unsupported_version."""
        with pytest.raises(ProtocolError, match="unsupported_version"):
            MessageDecoder().feed(json_frame(good(v=BAD_VERSION)))

    def test_json_frame_while_attachments_pending(self) -> None:
        """Refuse a JSON frame when attachments are still owed."""
        decoder = MessageDecoder()
        assert decoder.feed(json_frame(good(attachments=1))) is None
        with pytest.raises(ProtocolError):
            decoder.feed(json_frame(good()))

    def test_decoder_reusable_after_error(self) -> None:
        """Forget pending state after an error and decode again."""
        decoder = MessageDecoder()
        decoder.feed(json_frame(good(attachments=TWO_ATTACHMENTS)))
        decoder.feed(KIND_BINARY + BLOB_ONE)
        with pytest.raises(ProtocolError):
            decoder.feed(b"X")
        with pytest.raises(ProtocolError):
            decoder.feed(KIND_BINARY + BLOB_TWO)
        ping = Message(MessageType.PING)
        assert decoder.feed(encode_message(ping)[0]) == ping

    def test_decoder_reusable_after_pending_json_error(self) -> None:
        """Reset pending state when a JSON frame interrupts attachments."""
        decoder = MessageDecoder()
        decoder.feed(json_frame(good(attachments=1)))
        with pytest.raises(ProtocolError):
            decoder.feed(json_frame(good()))
        ping = Message(MessageType.PING)
        assert decoder.feed(encode_message(ping)[0]) == ping
