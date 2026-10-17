"""Custom exceptions for PyNet."""


class PyNetError(Exception):
    """Base exception for all PyNet errors."""


class DNSError(PyNetError):
    """Raise when a DNS lookup fails."""


class NetConnectionError(PyNetError):
    """Raise when a network connection fails."""


class HTTPError(PyNetError):
    """Raise when an HTTP request or response is invalid."""


class QueueEmpty(PyNetError):  # noqa: N818
    """Raise when ``get_nowait`` finds a queue with nothing in it."""


class QueueFull(PyNetError):  # noqa: N818
    """Raise when ``put_nowait`` finds a queue with no room."""


class ConnectionClosedError(NetConnectionError):
    """Raise when a connection has been closed or reset and cannot be used."""


class HeartbeatTimeoutError(ConnectionClosedError):
    """Raise when the other end stops answering heartbeat pings.

    It is the cause a ``Peer`` closes with when too many pings in a row
    go without a ``pong``: the line may look open, but nobody is there.
    """


class FrameTooLargeError(PyNetError):
    """Raise when a frame's length is over the allowed maximum.

    Args:
        size: The length the frame claimed or tried to use.
        limit: The largest length allowed.

    """

    def __init__(self, size: int, limit: int) -> None:
        """Remember the offending ``size`` and the ``limit``."""
        super().__init__(f"frame of {size} bytes exceeds the limit of {limit} bytes")
        self.size = size
        self.limit = limit


class ProtocolError(PyNetError):
    """Raise when a peer sends something that breaks the message protocol."""


class RemoteError(PyNetError):
    """Raise when the other end answers a request with an error reply.

    Handlers may also raise it on purpose to send a structured error:
    the peer then replies with this ``code`` and message.

    Args:
        code: A short machine-readable reason, such as ``"unknown_method"``.
        remote_message: The human-readable explanation from the other end.

    """

    def __init__(self, code: str, remote_message: str) -> None:
        """Remember the error ``code`` and ``remote_message``."""
        super().__init__(f"{code}: {remote_message}")
        self.code = code
        self.remote_message = remote_message
