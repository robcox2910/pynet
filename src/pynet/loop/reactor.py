"""The waiter at the restaurant -- runs ready jobs, watches timers, sleeps otherwise.

An ``EventLoop`` keeps two lists: a queue of callbacks that are ready
right now, and a heap of kitchen timers ordered by when they ring. Each
turn (``run_once``) the waiter waits in ``select`` until the next timer
is due (or forever if there is none), moves due timers onto the ready
queue, and runs the callbacks that were ready when the turn began.

The waiter also watches doors (file descriptors). A socketpair "doorbell"
lets other threads wake a sleeping loop (the self-pipe trick).
"""

import contextlib
import heapq
import itertools
import logging
import selectors
import socket
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

type Callback = Callable[[], object]

logger = logging.getLogger("pynet.loop")

_thread_state = threading.local()

WAKEUP_BYTE = b"x"
DRAIN_SIZE = 4096


class FdWatch:
    """Hold the reader and writer handles registered for one file descriptor."""

    def __init__(self) -> None:
        """Start with neither a reader nor a writer."""
        self.reader: Handle | None = None
        self.writer: Handle | None = None

    def mask(self) -> int:
        """Return the selector event mask for the handles that are set."""
        mask = 0
        if self.reader is not None:
            mask |= selectors.EVENT_READ
        if self.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask


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


def running_loop() -> EventLoop | None:
    """Return the loop running a turn in this thread, or None."""
    loop: EventLoop | None = getattr(_thread_state, "loop", None)
    return loop


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
        self._ready_lock = threading.Lock()
        self._wakeup_read, self._wakeup_write = socket.socketpair()
        self._wakeup_read.setblocking(False)
        self._wakeup_write.setblocking(False)
        self._selector.register(self._wakeup_read, selectors.EVENT_READ, None)

    def time(self) -> float:
        """Return the loop's current time in seconds."""
        return self._clock()

    def call_soon(self, callback: Callback) -> Handle:
        """Schedule ``callback`` to run on the next turn.

        Not thread-safe: call it only from the loop's own thread. Other
        threads must use ``call_soon_threadsafe``.
        """
        self._check_open()
        handle = Handle(callback)
        self._ready.append(handle)
        return handle

    def call_soon_threadsafe(self, callback: Callback) -> Handle:
        """Schedule ``callback`` from any thread and wake the loop.

        Appends under a lock, then rings the doorbell (one byte on the
        wakeup socket). A full pipe means the loop is already awake.
        """
        self._check_open()
        handle = Handle(callback)
        with self._ready_lock:
            self._ready.append(handle)
        with contextlib.suppress(BlockingIOError):
            self._wakeup_write.send(WAKEUP_BYTE)
        return handle

    def add_reader(self, fd: int, callback: Callback) -> None:
        """Call ``callback`` whenever ``fd`` is readable; replaces any reader."""
        self._add_watch(fd, callback, reader=True)

    def remove_reader(self, fd: int) -> bool:
        """Stop watching ``fd`` for reading; return whether a reader existed."""
        return self._remove_watch(fd, reader=True)

    def add_writer(self, fd: int, callback: Callback) -> None:
        """Call ``callback`` whenever ``fd`` is writable; replaces any writer."""
        self._add_watch(fd, callback, reader=False)

    def remove_writer(self, fd: int) -> bool:
        """Stop watching ``fd`` for writing; return whether a writer existed."""
        return self._remove_watch(fd, reader=False)

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
        """Run a single turn: wait, collect I/O and due timers, run callbacks.

        The selector always runs, even with work ready (with timeout 0),
        so a callback that keeps rescheduling itself cannot starve I/O.
        A "virtual" clock (one with ``virtual = True``, such as
        ``FakeClock``) never makes the loop really sleep: it only polls.
        With an autojump clock, a turn that saw no I/O and has a pending
        timer winds the clock forward to it.

        While the turn runs, this loop is the thread's running loop (see
        ``running_loop``), so ``get_running_loop`` works inside callbacks.

        Raises:
            RuntimeError: If a different loop is mid-turn in this thread.

        """
        previous = running_loop()
        if previous is not None and previous is not self:
            message = "Cannot run a loop while another loop is running in this thread"
            raise RuntimeError(message)
        _thread_state.loop = self
        try:
            self._turn()
        finally:
            _thread_state.loop = previous

    def _turn(self) -> None:
        """Do the work of one turn (see ``run_once``)."""
        timeout = self.next_timeout()
        virtual = bool(getattr(self._clock, "virtual", False))
        if virtual:
            events = self._selector.select(0)
            if not events and timeout is not None and timeout > 0:
                jump_to = getattr(self._clock, "jump_to", None)
                if callable(jump_to) and getattr(self._clock, "autojump", False):
                    jump_to(self._timers[0].when)
        else:
            events = self._selector.select(timeout)
        self._dispatch_events(events)
        now = self.time()
        while self._timers and self._timers[0].when <= now:
            self._ready.append(heapq.heappop(self._timers))
        with self._ready_lock:
            batch = list(self._ready)
            self._ready.clear()
        for handle in batch:
            if not handle.cancelled():
                self._run_handle(handle)

    def run_forever(self) -> None:
        """Run turns until ``stop`` is called.

        With nothing scheduled the loop sleeps in ``select`` until a file
        descriptor fires or another thread calls ``call_soon_threadsafe``
        (for example ``call_soon_threadsafe(loop.stop)``). The internal
        wakeup socket is not "work", so it never makes the loop spin.
        """
        self._check_open()
        self._running = True
        try:
            while not self._stopping:
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
        self._selector.unregister(self._wakeup_read)
        self._wakeup_read.close()
        self._wakeup_write.close()
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

    def _add_watch(self, fd: int, callback: Callback, *, reader: bool) -> None:
        """Register ``callback`` as the reader or writer of ``fd``."""
        self._check_open()
        handle = Handle(callback)
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            watch = FdWatch()
            self._set_handle(watch, handle, reader=reader)
            self._selector.register(fd, watch.mask(), watch)
            return
        watch = key.data
        old = watch.reader if reader else watch.writer
        if old is not None:
            old.cancel()
        self._set_handle(watch, handle, reader=reader)
        self._selector.modify(fd, watch.mask(), watch)

    def _remove_watch(self, fd: int, *, reader: bool) -> bool:
        """Drop the reader or writer of ``fd``; return whether one existed."""
        if self._closed:
            return False
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            return False
        watch = key.data
        if watch is None:
            return False
        old = watch.reader if reader else watch.writer
        if old is None:
            return False
        old.cancel()
        self._set_handle(watch, None, reader=reader)
        if watch.mask() == 0:
            self._selector.unregister(fd)
        else:
            self._selector.modify(fd, watch.mask(), watch)
        return True

    @staticmethod
    def _set_handle(watch: FdWatch, handle: Handle | None, *, reader: bool) -> None:
        """Store ``handle`` as the reader or writer of ``watch``."""
        if reader:
            watch.reader = handle
        else:
            watch.writer = handle

    def _dispatch_events(self, events: list[tuple[selectors.SelectorKey, int]]) -> None:
        """Queue the callbacks for ready descriptors; drain the doorbell."""
        for key, mask in events:
            watch = key.data
            if watch is None:
                self._drain_wakeup()
                continue
            if mask & selectors.EVENT_READ and watch.reader is not None:
                self._ready.append(watch.reader)
            if mask & selectors.EVENT_WRITE and watch.writer is not None:
                self._ready.append(watch.writer)

    def _drain_wakeup(self) -> None:
        """Read every pending doorbell byte."""
        try:
            while self._wakeup_read.recv(DRAIN_SIZE):
                pass
        except BlockingIOError:
            pass

    def _run_handle(self, handle: Handle) -> None:
        """Run one handle, reporting (never swallowing) its exceptions."""
        try:
            handle.run()
        except Exception as exc:  # noqa: BLE001
            self.report_error(ErrorContext(f"Exception in callback {handle.describe()}", exc))
