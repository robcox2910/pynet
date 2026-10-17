"""A real phone call -- dialling, answering and talking over WebSocket.

This package is the from-scratch RFC 6455 implementation (the older
``pynet.websocket`` module is only a simulation). It grows in layers:

- ``handshake`` -- the opening exchange that upgrades an HTTP
  connection to a WebSocket.
- ``frames`` -- the envelopes: encoding, masking and decoding frames.

Everything public is re-exported here.
"""

from pynet.errors import HandshakeError
from pynet.ws.frames import (
    MASK_KEY_SIZE,
    MAX_CONTROL_PAYLOAD,
    Frame,
    Opcode,
    apply_mask,
    encode_ws_frame,
    read_ws_frame,
)
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
    "MASK_KEY_SIZE",
    "MAX_CONTROL_PAYLOAD",
    "WS_GUID",
    "Frame",
    "HandshakeError",
    "Opcode",
    "apply_mask",
    "client_handshake_request",
    "compute_accept",
    "encode_ws_frame",
    "generate_key",
    "parse_http_head",
    "read_ws_frame",
    "server_handshake_response",
    "validate_client_handshake",
]
