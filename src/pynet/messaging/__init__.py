"""Walkie-talkies with call signs -- say who you are, what you want, and who answers.

Messaging is two layers, each in its own module:

- ``codec`` -- the envelope: turn a ``Message`` into frame payloads and
  back, with no I/O at all.
- ``peer`` -- the ``Peer``: one end of a live connection that sends
  requests, answers them, delivers pushes and notices when the line dies.
- ``reconnect`` -- the ``ReconnectingClient``: a client that redials with
  exponential backoff and jitter whenever its connection drops.

Everything public is re-exported here, so ``from pynet.messaging import
Peer`` keeps working.
"""

from pynet.messaging.codec import (
    KIND_BINARY,
    KIND_JSON,
    KIND_SIZE,
    MAX_NESTING,
    NEEDS_ID,
    NEEDS_METHOD,
    PROTOCOL_VERSION,
    JsonValue,
    Message,
    MessageDecoder,
    MessageType,
    encode_message,
)
from pynet.messaging.peer import (
    BAD_MESSAGE,
    CLOSE_GRACE,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MAX_MISSED_PONGS,
    DEFAULT_REQUEST_TIMEOUT,
    FIRST_REQUEST_ID,
    HANDLER_ERROR,
    UNKNOWN_METHOD,
    UNSUPPORTED_VERSION,
    CloseCallback,
    Handler,
    Peer,
    Reply,
    connect,
    serve,
)
from pynet.messaging.reconnect import (
    DEFAULT_INITIAL_BACKOFF,
    DEFAULT_JITTER,
    DEFAULT_MAX_BACKOFF,
    ConnectionEvent,
    EventCallback,
    ReconnectingClient,
    backoff_delay,
)

__all__ = [
    "BAD_MESSAGE",
    "CLOSE_GRACE",
    "DEFAULT_HEARTBEAT_INTERVAL",
    "DEFAULT_INITIAL_BACKOFF",
    "DEFAULT_JITTER",
    "DEFAULT_MAX_BACKOFF",
    "DEFAULT_MAX_MISSED_PONGS",
    "DEFAULT_REQUEST_TIMEOUT",
    "FIRST_REQUEST_ID",
    "HANDLER_ERROR",
    "KIND_BINARY",
    "KIND_JSON",
    "KIND_SIZE",
    "MAX_NESTING",
    "NEEDS_ID",
    "NEEDS_METHOD",
    "PROTOCOL_VERSION",
    "UNKNOWN_METHOD",
    "UNSUPPORTED_VERSION",
    "CloseCallback",
    "ConnectionEvent",
    "EventCallback",
    "Handler",
    "JsonValue",
    "Message",
    "MessageDecoder",
    "MessageType",
    "Peer",
    "ReconnectingClient",
    "Reply",
    "backoff_delay",
    "connect",
    "encode_message",
    "serve",
]
