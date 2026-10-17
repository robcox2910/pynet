"""Test Event, Queue, Lock and Semaphore."""

from collections.abc import Coroutine
from typing import Any

import pytest

from pynet.loop import (
    Cancelled,
    Event,
    Lock,
    Queue,
    QueueEmpty,
    QueueFull,
    Semaphore,
    gather,
    run,
    sleep,
    spawn,
)
from pynet.loop.testing import FakeClock

SHORT = 1.0
WORKERS = 5
LIMIT = 2
FIRST = 1
SECOND = 2
THIRD = 3
ITEM = 42
BOUND = 2
EMPTY = 0


def _run[T](main: Coroutine[Any, Any, T]) -> T:
    """Run ``main`` on an instant, deterministic autojump clock."""
    return run(main, clock=FakeClock(autojump=True))


class TestEvent:
    """Test the starting pistol."""

    def test_event_wakes_all_waiters(self) -> None:
        """Wake every waiter when the event is set."""

        async def main() -> int:
            """Start three waiters then fire the pistol."""
            event = Event()
            woken: list[int] = []

            async def waiter(n: int) -> None:
                """Wait and record."""
                await event.wait()
                woken.append(n)

            tasks = [spawn(waiter(n)) for n in (FIRST, SECOND, THIRD)]
            await sleep(SHORT)
            assert woken == []
            event.set()
            await gather(*tasks)
            return len(woken)

        assert _run(main()) == THIRD

    def test_set_clear_is_set(self) -> None:
        """Track the flag and return at once when already set."""

        async def main() -> None:
            """Exercise the flag."""
            event = Event()
            assert not event.is_set()
            event.set()
            assert event.is_set()
            await event.wait()
            event.clear()
            assert not event.is_set()

        _run(main())

    def test_cancelled_event_waiter_is_removed(self) -> None:
        """Drop a cancelled waiter without harming the others."""

        async def main() -> bool:
            """Cancel one waiter, then set."""
            event = Event()
            first = spawn(event.wait())
            second = spawn(event.wait())
            await sleep(SHORT)
            first.cancel()
            await sleep(SHORT)
            event.set()
            await second
            return first.cancelled()

        assert _run(main())


class TestQueue:
    """Test the conveyor belt."""

    def test_queue_get_waits_for_put(self) -> None:
        """Block a getter until an item arrives."""

        async def main() -> int:
            """Get before anything is put."""
            queue: Queue[int] = Queue()

            async def producer() -> None:
                """Put after a delay."""
                await sleep(SHORT)
                await queue.put(ITEM)

            spawn(producer())
            return await queue.get()

        assert _run(main()) == ITEM

    def test_bounded_queue_put_waits_when_full(self) -> None:
        """Block a putter on a full queue until a getter makes room."""

        async def main() -> list[int]:
            """Fill the belt, then put one more."""
            queue: Queue[int] = Queue(BOUND)
            order: list[int] = []
            await queue.put(FIRST)
            await queue.put(SECOND)
            assert queue.full()

            async def late_put() -> None:
                """Put a third item once there is room."""
                await queue.put(THIRD)
                order.append(THIRD)

            task = spawn(late_put())
            await sleep(SHORT)
            assert order == []
            assert await queue.get() == FIRST
            await task
            return [await queue.get(), await queue.get()]

        assert _run(main()) == [SECOND, THIRD]

    def test_get_nowait_empty_raises(self) -> None:
        """Raise QueueEmpty when nothing is on the belt."""
        queue: Queue[int] = Queue()
        assert queue.empty()
        with pytest.raises(QueueEmpty):
            queue.get_nowait()

    def test_put_nowait_full_raises(self) -> None:
        """Raise QueueFull when the belt is full."""
        queue: Queue[int] = Queue(1)
        queue.put_nowait(FIRST)
        with pytest.raises(QueueFull):
            queue.put_nowait(SECOND)
        assert queue.qsize() == 1

    def test_unbounded_never_full(self) -> None:
        """Treat maxsize <= 0 as unbounded."""
        queue: Queue[int] = Queue(EMPTY)
        for n in range(WORKERS * WORKERS):
            queue.put_nowait(n)
        assert not queue.full()

    def test_fifo_order(self) -> None:
        """Hand items out in the order they arrived."""

        async def main() -> list[int]:
            """Put three, get three."""
            queue: Queue[int] = Queue()
            for n in (FIRST, SECOND, THIRD):
                await queue.put(n)
            return [await queue.get() for _ in range(THIRD)]

        assert _run(main()) == [FIRST, SECOND, THIRD]

    def test_cancelled_get_after_assignment_keeps_item(self) -> None:
        """Put an assigned item back when its getter is cancelled."""

        async def main() -> int:
            """Cancel a getter right after it was handed an item."""
            queue: Queue[int] = Queue()
            getter = spawn(queue.get())
            await sleep(SHORT)
            queue.put_nowait(ITEM)
            getter.cancel()
            with pytest.raises(Cancelled):
                await getter
            return queue.get_nowait()

        assert _run(main()) == ITEM

    def test_cancelled_getter_before_put_is_skipped(self) -> None:
        """Skip a cancelled getter and serve the next one."""

        async def main() -> int:
            """Cancel the first getter, put once."""
            queue: Queue[int] = Queue()
            first = spawn(queue.get())
            second = spawn(queue.get())
            await sleep(SHORT)
            first.cancel()
            await sleep(SHORT)
            queue.put_nowait(ITEM)
            return await second

        assert _run(main()) == ITEM

    def test_cancelled_putter_is_skipped(self) -> None:
        """Skip a cancelled putter when making room."""

        async def main() -> int:
            """Cancel the first waiting putter."""
            queue: Queue[int] = Queue(1)
            queue.put_nowait(FIRST)
            first = spawn(queue.put(SECOND))
            second = spawn(queue.put(THIRD))
            await sleep(SHORT)
            first.cancel()
            await sleep(SHORT)
            queue.get_nowait()
            await second
            return queue.get_nowait()

        assert _run(main()) == THIRD


class TestLock:
    """Test the bathroom key."""

    def test_lock_is_fifo(self) -> None:
        """Serve waiters in the order they asked."""

        async def main() -> list[int]:
            """Three tasks queue for the lock."""
            lock = Lock()
            order: list[int] = []

            async def worker(n: int) -> None:
                """Take the lock, record, hold briefly."""
                async with lock:
                    order.append(n)
                    await sleep(SHORT)

            await gather(*(worker(n) for n in (FIRST, SECOND, THIRD)))
            return order

        assert _run(main()) == [FIRST, SECOND, THIRD]

    def test_release_unlocked_raises(self) -> None:
        """Refuse to release a lock nobody holds."""
        with pytest.raises(RuntimeError):
            Lock().release()

    def test_locked_flag(self) -> None:
        """Report whether the key is taken."""

        async def main() -> None:
            """Acquire and release."""
            lock = Lock()
            assert not lock.locked()
            await lock.acquire()
            assert lock.locked()
            lock.release()
            assert not lock.locked()

        _run(main())

    def test_cancel_after_wakeup_passes_lock_on(self) -> None:
        """Hand the key to the next waiter when the chosen one is cancelled."""

        async def main() -> list[str]:
            """Cancel a waiter after it was given the lock."""
            lock = Lock()
            log: list[str] = []
            await lock.acquire()

            async def waiter(name: str) -> None:
                """Take and release the lock."""
                async with lock:
                    log.append(name)

            first = spawn(waiter("first"))
            second = spawn(waiter("second"))
            await sleep(SHORT)
            lock.release()
            first.cancel()
            with pytest.raises(Cancelled):
                await first
            await second
            return log

        assert _run(main()) == ["second"]

    def test_cancel_before_wakeup_removes_waiter(self) -> None:
        """Forget a waiter cancelled while still waiting."""

        async def main() -> bool:
            """Cancel a queued waiter, then release."""
            lock = Lock()
            await lock.acquire()
            waiter = spawn(lock.acquire())
            await sleep(SHORT)
            waiter.cancel()
            await sleep(SHORT)
            lock.release()
            return lock.locked()

        assert not _run(main())


class TestSemaphore:
    """Test the car park."""

    def test_semaphore_limits_concurrency(self) -> None:
        """Never let more than the limit run at once."""

        async def main() -> int:
            """Run five workers through two spaces."""
            sem = Semaphore(LIMIT)
            running = 0
            peak = 0

            async def worker() -> None:
                """Hold a space for a while."""
                nonlocal running, peak
                async with sem:
                    running += 1
                    peak = max(peak, running)
                    await sleep(SHORT)
                    running -= 1

            await gather(*(worker() for _ in range(WORKERS)))
            return peak

        assert _run(main()) == LIMIT

    def test_semaphore_over_release_raises(self) -> None:
        """Refuse to release beyond the initial value."""
        with pytest.raises(ValueError, match="released"):
            Semaphore(LIMIT).release()

    def test_negative_value_raises(self) -> None:
        """Refuse a negative starting value."""
        with pytest.raises(ValueError, match="negative"):
            Semaphore(-1)

    def test_available_counts_free_spaces(self) -> None:
        """Report free spaces."""

        async def main() -> None:
            """Take and return a space."""
            sem = Semaphore(LIMIT)
            await sem.acquire()
            assert sem.available() == LIMIT - 1
            sem.release()
            assert sem.available() == LIMIT

        _run(main())

    def test_cancelled_waiter_does_not_steal_slot(self) -> None:
        """Keep the slot for later waiters when an earlier one is cancelled."""

        async def main() -> int:
            """Cancel a waiter, release, and check the slot is usable."""
            sem = Semaphore(1)
            await sem.acquire()
            cancelled = spawn(sem.acquire())
            await sleep(SHORT)
            cancelled.cancel()
            await sleep(SHORT)
            sem.release()
            assert sem.available() == 1
            await sem.acquire()
            return sem.available()

        assert _run(main()) == EMPTY

    def test_cancel_after_handoff_passes_slot_on(self) -> None:
        """Pass a handed-over slot to the next waiter."""

        async def main() -> list[str]:
            """Cancel a waiter that was already given the slot."""
            sem = Semaphore(1)
            await sem.acquire()
            log: list[str] = []

            async def waiter(name: str) -> None:
                """Take a space once."""
                async with sem:
                    log.append(name)

            first = spawn(waiter("first"))
            second = spawn(waiter("second"))
            await sleep(SHORT)
            sem.release()
            first.cancel()
            with pytest.raises(Cancelled):
                await first
            await second
            return log

        assert _run(main()) == ["second"]
