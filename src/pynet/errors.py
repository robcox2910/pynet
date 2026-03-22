"""Custom exceptions for PyNet."""


class PyNetError(Exception):
    """Base exception for all PyNet errors."""


class DNSError(PyNetError):
    """Raise when a DNS lookup fails."""


class NetConnectionError(PyNetError):
    """Raise when a network connection fails."""


class HTTPError(PyNetError):
    """Raise when an HTTP request or response is invalid."""
