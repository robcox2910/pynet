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
