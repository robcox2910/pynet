"""A stopwatch you wind by hand -- time that moves only when you say so.

Tests should never wait for real seconds to pass. ``FakeClock`` is a
kitchen clock with no ticking: it stays put until you call ``advance``,
so a five-second timer can be tested in microseconds.
"""


class FakeClock:
    """Provide a manually advanced monotonic clock.

    Pass an instance as the ``clock`` of an ``EventLoop`` and call
    ``advance`` to move time forward.
    """

    def __init__(self, start: float = 0.0) -> None:
        """Create a clock showing ``start`` seconds."""
        self._now = start

    def __call__(self) -> float:
        """Return the current fake time in seconds."""
        return self._now

    def advance(self, seconds: float) -> None:
        """Move the clock forward by ``seconds``."""
        self._now += seconds
