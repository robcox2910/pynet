"""A real phone call -- dialling, answering and talking over WebSocket.

This package is the from-scratch RFC 6455 implementation (the older
``pynet.websocket`` module is only a simulation). It grows in layers:

- ``handshake`` -- the opening exchange that upgrades an HTTP
  connection to a WebSocket.

Everything public is re-exported here.
"""

from pynet.errors import HandshakeError
from pynet.ws.handshake import (
    KEY_BYTES,
    WS_GUID,
    client_handshake_request,
    compute_accept,
    generate_key,
    parse_http_head,
    server_handshake_response,
    validate_client_handshake,
)

__all__ = [
    "KEY_BYTES",
    "WS_GUID",
    "HandshakeError",
    "client_handshake_request",
    "compute_accept",
    "generate_key",
    "parse_http_head",
    "server_handshake_response",
    "validate_client_handshake",
]
