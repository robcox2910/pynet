"""Walkie-talkie call signs -- the envelope that says who, what and which call.

Raw frames are just anonymous chunks of bytes. Messaging adds an envelope
so both ends know what each chunk means: a request carries a call sign
(``id``) so the matching response can say "this is the answer to call
42", and a ``method`` naming what is being asked for. Large or binary
payloads travel as separate attachments so they never have to be turned
into text.

The codec is pure (no I/O): it turns a ``Message`` into a list of frame
payloads and back. Each payload starts with a one-byte kind marker:

- ``J`` -- a JSON envelope, always the first frame of a message.
- ``B`` -- raw attachment bytes; as many follow as the envelope declares.

Envelope keys: ``v`` (protocol version) and ``type`` are always written.
``body`` is always written (it may be ``null``). ``id`` and ``method``
are written only when they are not ``None``. ``attachments`` (a count) is
written only when there is at least one attachment.

Rules worth knowing:

- NaN and infinity are not valid JSON, so they are refused both when
  encoding and when decoding.
- A body may nest lists and objects at most ``MAX_NESTING`` deep, like
  boxes inside boxes. Deeper is refused both ways, so a "box bomb" can
  never crash either end (how deep Python itself can go differs between
  computers, so we pick the limit ourselves).
- Any breach of the protocol raises ``ProtocolError``. The decoder then
  forgets any half-finished message, so it can be used again.
"""

import json
from dataclasses import dataclass
from enum import StrEnum

from pynet.errors import ProtocolError

PROTOCOL_VERSION = 1
KIND_JSON = b"J"
KIND_BINARY = b"B"
KIND_SIZE = 1
MAX_NESTING = 100

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class MessageType(StrEnum):
    """The kinds of message that can cross the wire."""

    REQUEST = "request"
    RESPONSE = "response"
    ERROR = "error"
    PUSH = "push"
    PING = "ping"
    PONG = "pong"


NEEDS_ID = frozenset({MessageType.REQUEST, MessageType.RESPONSE, MessageType.ERROR})
NEEDS_METHOD = frozenset({MessageType.REQUEST, MessageType.PUSH})


@dataclass(frozen=True)
class Message:
    """One message: an envelope plus optional binary attachments.

    Args:
        type: What kind of message this is.
        id: The call sign pairing a response with its request.
        method: The name of the operation requested or pushed.
        body: Any JSON-compatible value.
        attachments: Raw byte payloads sent beside the JSON.

    """

    type: MessageType
    id: int | None = None
    method: str | None = None
    body: JsonValue = None
    attachments: tuple[bytes, ...] = ()


def encode_message(msg: Message) -> list[bytes]:
    """Turn ``msg`` into frame payloads, ready for ``send_frame``.

    Args:
        msg: The message to encode.

    Returns:
        The JSON envelope frame followed by one binary frame per attachment.

    Raises:
        ProtocolError: If the envelope cannot be written as strict JSON
            (for example it holds NaN or a non-JSON type).

    """
    envelope: dict[str, JsonValue] = {"v": PROTOCOL_VERSION, "type": msg.type.value}
    if msg.id is not None:
        envelope["id"] = msg.id
    if msg.method is not None:
        envelope["method"] = msg.method
    envelope["body"] = msg.body
    if msg.attachments:
        envelope["attachments"] = len(msg.attachments)
    _check_nesting(msg.body)
    try:
        text = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        payload = text.encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        message = f"cannot encode envelope: {error}"
        raise ProtocolError(message) from error
    return [KIND_JSON + payload, *(KIND_BINARY + data for data in msg.attachments)]


def _check_nesting(value: JsonValue) -> None:
    """Refuse ``value`` if lists/objects nest deeper than ``MAX_NESTING``.

    Walks with its own stack instead of recursing, so the check itself
    can never hit Python's recursion limit.
    """
    stack: list[tuple[JsonValue, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, list | dict):
            if depth >= MAX_NESTING:
                message = f"nested deeper than {MAX_NESTING} levels"
                raise ProtocolError(message)
            children = item.values() if isinstance(item, dict) else item
            stack.extend((child, depth + 1) for child in children)


def _reject_constant(name: str) -> JsonValue:
    """Refuse the non-standard JSON constants ``NaN`` and ``Infinity``."""
    message = f"non-finite number {name} is not allowed"
    raise ValueError(message)


class MessageDecoder:
    """Rebuild messages from frame payloads, one frame at a time."""

    def __init__(self) -> None:
        """Start with no half-finished message."""
        self._pending: Message | None = None
        self._expected = 0
        self._attachments: list[bytes] = []

    def feed(self, frame: bytes) -> Message | None:
        """Accept one frame payload.

        Args:
            frame: A payload from ``recv_frame``, kind byte included.

        Returns:
            The finished message once its envelope and every declared
            attachment have arrived, otherwise ``None``.

        Raises:
            ProtocolError: If the frame breaks the protocol. Any pending
                partial message is discarded first.

        """
        try:
            return self._feed(frame)
        except ProtocolError:
            self._reset()
            raise

    def _reset(self) -> None:
        """Forget any half-finished message."""
        self._pending = None
        self._expected = 0
        self._attachments = []

    def _feed(self, frame: bytes) -> Message | None:
        """Dispatch ``frame`` on its kind byte."""
        kind = frame[:KIND_SIZE]
        if kind == KIND_JSON:
            if self._pending is not None:
                message = "JSON frame arrived while attachments were pending"
                raise ProtocolError(message)
            self._pending, self._expected = self._parse_envelope(frame[KIND_SIZE:])
            return self._finish_if_ready()
        if kind == KIND_BINARY:
            if self._pending is None:
                message = "binary frame with no pending envelope"
                raise ProtocolError(message)
            self._attachments.append(frame[KIND_SIZE:])
            return self._finish_if_ready()
        message = f"unknown frame kind {kind!r}"
        raise ProtocolError(message)

    def _finish_if_ready(self) -> Message | None:
        """Return the finished message if no attachments are still owed."""
        if self._pending is None or len(self._attachments) < self._expected:
            return None
        message = Message(
            self._pending.type,
            self._pending.id,
            self._pending.method,
            self._pending.body,
            tuple(self._attachments),
        )
        self._reset()
        return message

    @staticmethod
    def _parse_envelope(raw: bytes) -> tuple[Message, int]:
        """Validate a JSON envelope; return an attachment-less message and the count owed."""
        try:
            data: JsonValue = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
        except (ValueError, RecursionError) as error:
            message = f"invalid JSON envelope: {error}"
            raise ProtocolError(message) from error
        if not isinstance(data, dict):
            message = "envelope must be a JSON object"
            raise ProtocolError(message)
        _check_nesting(data.get("body"))
        if data.get("v") != PROTOCOL_VERSION:
            message = f"unsupported_version: {data.get('v')!r}"
            raise ProtocolError(message)
        try:
            kind = MessageType(data.get("type"))
        except ValueError as error:
            message = f"unknown message type {data.get('type')!r}"
            raise ProtocolError(message) from error
        msg_id = data.get("id")
        method = data.get("method")
        if msg_id is not None and (not isinstance(msg_id, int) or isinstance(msg_id, bool)):
            message = "id must be an integer"
            raise ProtocolError(message)
        if method is not None and not isinstance(method, str):
            message = "method must be a string"
            raise ProtocolError(message)
        if kind in NEEDS_ID and msg_id is None:
            message = f"{kind.value} requires an id"
            raise ProtocolError(message)
        if kind in NEEDS_METHOD and method is None:
            message = f"{kind.value} requires a method"
            raise ProtocolError(message)
        count = data.get("attachments", 0)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            message = "attachments must be a non-negative integer"
            raise ProtocolError(message)
        return Message(kind, msg_id, method, data.get("body")), count
