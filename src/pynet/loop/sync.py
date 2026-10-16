"""Traffic rules for tasks -- a starting pistol, a conveyor belt, a key, a car park.

Tasks share one thread, but they still need to coordinate. ``Event`` is
a starting pistol: everyone waits, one shot releases them all. ``Queue``
is a conveyor belt that hands items from producers to consumers. ``Lock``
is a bathroom key: one holder at a time. ``Semaphore`` is a car park
with a fixed number of spaces.

Every waiter is a ``Future`` in a ``deque``, served first come, first
served. Cancellation needs care: a waiter cancelled while still queued
is simply dropped, but one cancelled *after* it was handed a space, key
or item must pass that gift on, or it would be lost for ever.
"""

from collections import deque
from contextlib import suppress

from pynet.errors import QueueEmpty, QueueFull
from pynet.loop.futures import Cancelled, Future
from pynet.loop.tasks import get_running_loop

__all__ = ["Event", "Lock", "Queue", "QueueEmpty", "QueueFull", "Semaphore"]


def _wake_first[T](waiters: deque[Future[T]], value: T) -> bool:
    """Hand ``value`` to the first live waiter; return whether one took it."""
    while waiters:
        waiter = waiters.popleft()
        if not waiter.done():
            waiter.set_result(value)
            return True
    return False


def _forget[T](waiters: deque[Future[T]], waiter: Future[T]) -> None:
    """Remove a cancelled waiter from the line if it is still there."""
    with suppress(ValueError):
        waiters.remove(waiter)


class Event:
    """Release every waiting task at once when set."""

    def __init__(self) -> None:
        """Create an event that starts cleared."""
        self._set = False
        self._waiters: deque[Future[None]] = deque()

    def set(self) -> None:
        """Fire the pistol: wake every waiter and stay set."""
        self._set = True
        while _wake_first(self._waiters, None):
            pass

    def clear(self) -> None:
        """Reset the event so later waiters block again."""
        self._set = False

    def is_set(self) -> bool:
        """Return whether the event is set."""
        return self._set

    async def wait(self) -> None:
        """Wait until the event is set; return at once if it already is."""
        if self._set:
            return
        waiter: Future[None] = Future(get_running_loop())
        self._waiters.append(waiter)
        try:
            await waiter
        except Cancelled:
            _forget(self._waiters, waiter)
            raise


class Queue[T]:
    """Pass items between tasks, oldest first.

    Args:
        maxsize: The most items allowed on the belt; ``<= 0`` means no limit.

    """

    def __init__(self, maxsize: int = 0) -> None:
        """Create an empty queue."""
        self._maxsize = maxsize
        self._items: deque[T] = deque()
        self._getters: deque[Future[T]] = deque()
        self._putters: deque[tuple[Future[None], T]] = deque()

    def qsize(self) -> int:
        """Return how many items are on the belt."""
        return len(self._items)

    def empty(self) -> bool:
        """Return whether the belt has no items."""
        return not self._items

    def full(self) -> bool:
        """Return whether the belt is at its limit."""
        return self._maxsize > 0 and len(self._items) >= self._maxsize

    def put_nowait(self, item: T) -> None:
        """Put ``item`` on the belt without waiting.

        Raises:
            QueueFull: If the belt is full.

        """
        if self.full():
            message = "queue is full"
            raise QueueFull(message)
        self._items.append(item)
        self._dispatch()

    def get_nowait(self) -> T:
        """Take the oldest item without waiting.

        Raises:
            QueueEmpty: If the belt is empty.

        """
        if not self._items:
            message = "queue is empty"
            raise QueueEmpty(message)
        item = self._items.popleft()
        self._admit_putters()
        return item

    async def put(self, item: T) -> None:
        """Put ``item`` on the belt, waiting for room if it is full.

        If this call is cancelled after room was made for it, the item
        is already on the belt.
        """
        if not self.full():
            self.put_nowait(item)
            return
        waiter: Future[None] = Future(get_running_loop())
        entry = (waiter, item)
        self._putters.append(entry)
        try:
            await waiter
        except Cancelled:
            with suppress(ValueError):
                self._putters.remove(entry)
            raise

    async def get(self) -> T:
        """Take the oldest item, waiting for one if the belt is empty."""
        if self._items:
            return self.get_nowait()
        waiter: Future[T] = Future(get_running_loop())
        self._getters.append(waiter)
        try:
            return await waiter
        except Cancelled:
            if waiter.done() and not waiter.cancelled():
                self._items.appendleft(waiter.result())  # do not lose the gift
                self._dispatch()
            else:
                _forget(self._getters, waiter)
            raise

    def _dispatch(self) -> None:
        """Give waiting getters the items on the belt, oldest first."""
        while self._items and self._getters:
            if _wake_first(self._getters, self._items[0]):
                self._items.popleft()
        self._admit_putters()

    def _admit_putters(self) -> None:
        """Move waiting putters' items onto the belt while there is room."""
        while self._putters and not self.full():
            waiter, item = self._putters.popleft()
            if waiter.done():
                continue
            self._items.append(item)
            waiter.set_result(None)
            self._dispatch()


class Lock:
    """Let one task at a time hold the key; waiters queue first come, first served."""

    def __init__(self) -> None:
        """Create an unlocked lock."""
        self._locked = False
        self._waiters: deque[Future[None]] = deque()

    def locked(self) -> bool:
        """Return whether someone holds the key."""
        return self._locked

    async def acquire(self) -> None:
        """Take the key, waiting in line if someone else has it."""
        if not self._locked and not self._waiters:
            self._locked = True
            return
        waiter: Future[None] = Future(get_running_loop())
        self._waiters.append(waiter)
        try:
            await waiter  # the releaser keeps the lock locked and hands it over
        except Cancelled:
            if waiter.done() and not waiter.cancelled():
                self.release()  # we were handed the key: pass it on
            else:
                _forget(self._waiters, waiter)
            raise

    def release(self) -> None:
        """Hand the key to the next in line, or leave it free.

        Raises:
            RuntimeError: If the lock is not held.

        """
        if not self._locked:
            message = "release() of an unlocked Lock"
            raise RuntimeError(message)
        if not _wake_first(self._waiters, None):
            self._locked = False

    async def __aenter__(self) -> None:
        """Acquire the lock."""
        await self.acquire()

    async def __aexit__(self, *_exc: object) -> None:
        """Release the lock."""
        self.release()


class Semaphore:
    """Allow a fixed number of tasks in at once; waiters queue first come, first served.

    Args:
        value: The number of spaces, which is also the most it can hold.

    """

    def __init__(self, value: int) -> None:
        """Create a semaphore with ``value`` free spaces.

        Raises:
            ValueError: If ``value`` is negative.

        """
        if value < 0:
            message = "Semaphore value must not be negative"
            raise ValueError(message)
        self._initial = value
        self._value = value
        self._waiters: deque[Future[None]] = deque()

    def available(self) -> int:
        """Return how many spaces are free right now."""
        return self._value

    async def acquire(self) -> None:
        """Take a space, waiting in line if the car park is full."""
        if self._value > 0 and not self._waiters:
            self._value -= 1
            return
        waiter: Future[None] = Future(get_running_loop())
        self._waiters.append(waiter)
        try:
            await waiter  # the releaser hands its space straight over
        except Cancelled:
            if waiter.done() and not waiter.cancelled():
                self.release()  # we were handed a space: pass it on
            else:
                _forget(self._waiters, waiter)
            raise

    def release(self) -> None:
        """Give a space back, to the next waiter if there is one.

        Raises:
            ValueError: If that would exceed the initial value.

        """
        if _wake_first(self._waiters, None):
            return
        if self._value >= self._initial:
            message = "Semaphore released too many times"
            raise ValueError(message)
        self._value += 1

    async def __aenter__(self) -> None:
        """Acquire a space."""
        await self.acquire()

    async def __aexit__(self, *_exc: object) -> None:
        """Release the space."""
        self.release()
