"""The waiter at the restaurant -- runs ready jobs, watches timers, sleeps otherwise.

An ``EventLoop`` keeps two lists: a queue of callbacks that are ready
right now, and a heap of kitchen timers ordered by when they ring. Each
turn (``run_once``) the waiter waits in ``select`` until the next timer
is due (or forever if there is none), moves due timers onto the ready
queue, and runs the callbacks that were ready when the turn began.
"""

import heapq
import itertools
import logging
import selectors
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

type Callback = Callable[[], object]

logger = logging.getLogger("pynet.loop")


class Handle:
    """Hold a scheduled callback so it can be cancelled."""

    def __init__(self, callback: Callback) -> None:
        """Wrap ``callback`` in a cancellable handle."""
        self._callback: Callback | None = callback
        self._cancelled = False

    def cancel(self) -> None:
        """Cancel the callback; a cancelled handle never runs."""
        self._cancelled = True
        self._callback = None

    def cancelled(self) -> bool:
        """Return whether the handle has been cancelled."""
        return self._cancelled

    def run(self) -> None:
        """Invoke the callback unless cancelled."""
        callback = self._callback
        if callback is not None:
            callback()

    def describe(self) -> str:
        """Return a short description of the wrapped callback."""
        return repr(self._callback)


class TimerHandle(Handle):
    """Hold a callback scheduled for a point in time (a kitchen timer)."""

    def __init__(self, when: float, callback: Callback, sequence: int) -> None:
        """Schedule ``callback`` for loop time ``when``; ``sequence`` breaks ties."""
        super().__init__(callback)
        self.when = when
        self._sequence = sequence

    def __lt__(self, other: TimerHandle) -> bool:
        """Order by ``(when, sequence)`` for the heap."""
        return (self.when, self._sequence) < (other.when, other._sequence)


@dataclass(frozen=True)
class ErrorContext:
    """Describe an error reported to the loop's exception handler."""

    message: str
    exception: BaseException | None = None


class EventLoop:
    """Run callbacks and timers in a single thread without spinning."""

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        selector: selectors.BaseSelector | None = None,
    ) -> None:
        """Create a loop using ``clock`` for time and ``selector`` for waiting."""
        self._clock = clock
        self._selector = selector if selector is not None else selectors.DefaultSelector()
        self._ready: deque[Handle] = deque()
        self._timers: list[TimerHandle] = []
        self._sequence = itertools.count()
        self._running = False
        self._stopping = False
        self._closed = False
        self._exception_handler: Callable[[ErrorContext], None] | None = None

    def time(self) -> float:
        """Return the loop's current time in seconds."""
        return self._clock()

    def call_soon(self, callback: Callback) -> Handle:
        """Schedule ``callback`` to run on the next turn."""
        self._check_open()
        handle = Handle(callback)
        self._ready.append(handle)
        return handle

    def call_later(self, delay: float, callback: Callback) -> TimerHandle:
        """Schedule ``callback`` to run ``delay`` seconds from now."""
        return self.call_at(self.time() + delay, callback)

    def call_at(self, when: float, callback: Callback) -> TimerHandle:
        """Schedule ``callback`` to run at loop time ``when``."""
        self._check_open()
        handle = TimerHandle(when, callback, next(self._sequence))
        heapq.heappush(self._timers, handle)
        return handle

    def next_timeout(self) -> float | None:
        """Return how long to wait: 0 if work is ready, None to wait forever."""
        if self._ready:
            return 0.0
        while self._timers and self._timers[0].cancelled():
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return max(0.0, self._timers[0].when - self.time())

    def run_once(self) -> None:
        """Run a single turn: wait, collect due timers, run ready callbacks.

        A "virtual" clock (one with ``virtual = True``, such as
        ``FakeClock``) never makes the loop really sleep.

        Until Task 2 registers a wakeup socket, a selector with no file
        descriptors cannot be woken. Waiting forever on it would hang, so
        in that case (nothing registered, nothing scheduled) the turn
        returns immediately instead of blocking.
        """
        timeout = self.next_timeout()
        virtual = bool(getattr(self._clock, "virtual", False))
        events: list[object] = []
        if virtual:
            # A pretend clock never really sleeps: poll the selector only.
            if self._selector.get_map():
                events = list(self._selector.select(0))
            if not events and timeout is not None and timeout > 0:
                # Nothing ready, no I/O, a timer pending: an autojump clock
                # winds itself forward so the timer fires on this same turn.
                jump_to = getattr(self._clock, "jump_to", None)
                if callable(jump_to) and getattr(self._clock, "autojump", False):
                    jump_to(self._timers[0].when)
        elif timeout is not None or self._selector.get_map():
            self._selector.select(timeout)
        now = self.time()
        while self._timers and self._timers[0].when <= now:
            self._ready.append(heapq.heappop(self._timers))
        for _ in range(len(self._ready)):
            handle = self._ready.popleft()
            if not handle.cancelled():
                self._run_handle(handle)

    def run_forever(self) -> None:
        """Run turns until ``stop`` is called.

        Temporary (until the wakeup socket arrives): if there is nothing
        ready, no timers and no registered file descriptors, nothing can
        ever happen, so the loop returns rather than hanging.
        """
        self._check_open()
        self._running = True
        try:
            while not self._stopping:
                if self.next_timeout() is None and not self._selector.get_map():
                    break
                self.run_once()
        finally:
            self._running = False
            self._stopping = False

    def stop(self) -> None:
        """Make ``run_forever`` return after the current turn."""
        self._stopping = True

    def is_running(self) -> bool:
        """Return whether ``run_forever`` is active."""
        return self._running

    def close(self) -> None:
        """Close the loop and its selector; scheduling afterwards raises."""
        if self._closed:
            return
        self._closed = True
        self._ready.clear()
        self._timers.clear()
        self._selector.close()

    def is_closed(self) -> bool:
        """Return whether the loop has been closed."""
        return self._closed

    def set_exception_handler(self, handler: Callable[[ErrorContext], None] | None) -> None:
        """Install ``handler`` for reported errors; None restores the logger."""
        self._exception_handler = handler

    def report_error(self, ctx: ErrorContext) -> None:
        """Send ``ctx`` to the exception handler, or log it at ERROR."""
        if self._exception_handler is None:
            logger.error(ctx.message, exc_info=ctx.exception)
        else:
            self._exception_handler(ctx)

    def _check_open(self) -> None:
        """Raise RuntimeError if the loop is closed."""
        if self._closed:
            message = "Event loop is closed"
            raise RuntimeError(message)

    def _run_handle(self, handle: Handle) -> None:
        """Run one handle, reporting (never swallowing) its exceptions."""
        try:
            handle.run()
        except Exception as exc:  # noqa: BLE001
            self.report_error(ErrorContext(f"Exception in callback {handle.describe()}", exc))
