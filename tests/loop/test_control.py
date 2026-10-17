"""Test timeouts, ``wait_for`` and ``gather``."""

from collections.abc import Coroutine
from typing import Any

import pytest

from pynet.loop import (
    Cancelled,
    ErrorContext,
    EventLoop,
    Future,
    Task,
    Timeout,
    current_task,
    gather,
    get_running_loop,
    run,
    sleep,
    spawn,
    timeout,
    wait_for,
)
from pynet.loop.testing import FakeClock

SHORT = 1.0
MEDIUM = 5.0
LONG = 100.0
ANSWER = 42
FIRST = 1
SECOND = 2
THIRD = 3
NO_CANCELS = 0


class BoomError(Exception):
    """Mark an error raised on purpose by a test."""


def _run[T](main: Coroutine[Any, Any, T]) -> T:
    """Run ``main`` on an instant, deterministic autojump clock."""
    return run(main, clock=FakeClock(autojump=True))


async def _value_after(value: int, seconds: float) -> int:
    """Return ``value`` after sleeping ``seconds``."""
    await sleep(seconds)
    return value


async def _boom_after(seconds: float) -> int:
    """Raise ``BoomError`` after sleeping ``seconds``."""
    await sleep(seconds)
    message = "boom"
    raise BoomError(message)


class TestTimeout:
    """Test the ``timeout`` async context manager."""

    def test_timeout_raises_timeout_error(self) -> None:
        """Raise ``TimeoutError`` when the block outlives its limit."""

        async def main() -> float:
            """Sleep too long inside a short timeout."""
            with pytest.raises(TimeoutError):
                async with timeout(SHORT):
                    await sleep(LONG)
            return get_running_loop().time()

        assert _run(main()) == SHORT

    def test_timeout_not_triggered_returns_normally(self) -> None:
        """Leave the block normally when it finishes in time."""

        async def main() -> tuple[int, bool, int]:
            """Finish quickly inside a long timeout."""
            async with timeout(LONG) as limit:
                value = await _value_after(ANSWER, SHORT)
            task = current_task()
            assert task is not None
            return value, limit.expired(), task.cancelling()

        assert _run(main()) == (ANSWER, False, NO_CANCELS)

    def test_timeout_none_means_no_limit(self) -> None:
        """Never expire when the limit is None."""

        async def main() -> bool:
            """Sleep a long time with no limit."""
            async with timeout(None) as limit:
                await sleep(LONG)
            return limit.expired()

        assert _run(main()) is False

    def test_finally_runs_on_timeout(self) -> None:
        """Run ``finally`` blocks inside the block when it times out."""
        events: list[str] = []

        async def main() -> None:
            """Record cleanup inside a timed-out block."""
            try:
                async with timeout(SHORT):
                    try:
                        await sleep(LONG)
                    finally:
                        events.append("cleanup")
            except TimeoutError:
                events.append("timed out")

        _run(main())

        assert events == ["cleanup", "timed out"]

    def test_expired_reports_firing(self) -> None:
        """Report ``expired()`` and clear the cancel request after firing."""

        async def main() -> tuple[bool, int]:
            """Time out, then inspect the timeout and the task."""
            limit = timeout(SHORT)
            with pytest.raises(TimeoutError):
                async with limit:
                    await sleep(LONG)
            task = current_task()
            assert task is not None
            return limit.expired(), task.cancelling()

        assert _run(main()) == (True, NO_CANCELS)

    def test_outer_cancel_inside_timeout_stays_cancelled(self) -> None:
        """Let an outside cancel through as ``Cancelled``, not ``TimeoutError``."""
        seen: list[str] = []

        async def worker() -> None:
            """Sleep inside a long timeout."""
            try:
                async with timeout(LONG):
                    await sleep(LONG)
            except TimeoutError:
                seen.append("timeout")
            except Cancelled:
                seen.append("cancelled")
                raise

        async def main() -> bool:
            """Cancel the worker from outside while it waits."""
            task = spawn(worker())
            await sleep(SHORT)
            task.cancel()
            with pytest.raises(Cancelled):
                await task
            return task.cancelled()

        assert _run(main()) is True
        assert seen == ["cancelled"]

    def test_nested_timeouts_inner_fires(self) -> None:
        """Raise ``TimeoutError`` at the inner block only when the inner fires."""
        events: list[str] = []

        async def main() -> None:
            """Nest a short timeout inside a long one."""
            async with timeout(LONG) as outer:
                try:
                    async with timeout(SHORT):
                        await sleep(MEDIUM)
                except TimeoutError:
                    events.append("inner timed out")
                await sleep(SHORT)
                events.append("outer still running")
            events.append(f"outer expired={outer.expired()}")

        _run(main())

        assert events == ["inner timed out", "outer still running", "outer expired=False"]

    def test_nested_timeouts_outer_fires(self) -> None:
        """Raise ``TimeoutError`` at the outer block when the outer fires."""
        events: list[str] = []

        async def main() -> None:
            """Nest a long timeout inside a short one."""
            try:
                async with timeout(SHORT):
                    try:
                        async with timeout(LONG):
                            await sleep(MEDIUM)
                    except TimeoutError:
                        events.append("inner timed out")
            except TimeoutError:
                events.append("outer timed out")

        _run(main())

        assert events == ["outer timed out"]

    def test_late_timer_does_nothing(self) -> None:
        """Ignore the timer if it rings after the block already finished."""

        async def main() -> tuple[bool, int]:
            """Finish the block, then ring its timer by hand."""
            limit = timeout(LONG)
            async with limit:
                await sleep(0)
            limit._on_timeout()
            await sleep(0)
            task = current_task()
            assert task is not None
            return limit.expired(), task.cancelling()

        assert _run(main()) == (False, NO_CANCELS)

    def test_timeout_outside_task_raises(self) -> None:
        """Refuse to enter a timeout when no task is running."""
        limit = timeout(SHORT)
        coro = limit.__aenter__()
        with pytest.raises(RuntimeError, match="task"):
            coro.send(None)

    def test_timeout_cannot_be_reused(self) -> None:
        """Refuse to enter the same timeout twice."""

        async def main() -> None:
            """Enter a finished timeout again."""
            limit = timeout(SHORT)
            async with limit:
                pass
            with pytest.raises(RuntimeError, match="once"):
                async with limit:
                    pass

        _run(main())

    def test_timeout_returns_timeout_instance(self) -> None:
        """Build a ``Timeout`` object from ``timeout()``."""
        assert isinstance(timeout(SHORT), Timeout)


class TestWaitFor:
    """Test ``wait_for``."""

    def test_wait_for_returns_value_and_times_out(self) -> None:
        """Return the value in time, or raise ``TimeoutError`` when late."""

        async def main() -> int:
            """Await one quick and one slow coroutine."""
            value = await wait_for(_value_after(ANSWER, SHORT), LONG)
            with pytest.raises(TimeoutError):
                await wait_for(_value_after(ANSWER, LONG), SHORT)
            return value

        assert _run(main()) == ANSWER

    def test_wait_for_cancels_a_late_task(self) -> None:
        """Cancel a task that does not finish within the limit."""

        async def main() -> bool:
            """Wait for a slow task with a short limit."""
            task = spawn(_value_after(ANSWER, LONG))
            with pytest.raises(TimeoutError):
                await wait_for(task, SHORT)
            return task.cancelled()

        assert _run(main()) is True

    def test_wait_for_none_waits_forever(self) -> None:
        """Wait as long as it takes when the limit is None."""
        assert _run(wait_for(_value_after(ANSWER, LONG), None)) == ANSWER


class TestGather:
    """Test ``gather``."""

    def test_gather_preserves_order(self) -> None:
        """Return results in argument order, not finishing order."""

        async def main() -> list[int | BaseException]:
            """Gather three coroutines that finish in reverse order."""
            return await gather(
                _value_after(FIRST, MEDIUM),
                _value_after(SECOND, SHORT),
                _value_after(THIRD, 0),
            )

        assert _run(main()) == [FIRST, SECOND, THIRD]

    def test_gather_runs_children_concurrently(self) -> None:
        """Finish in the time of the slowest child, not the sum."""

        async def main() -> float:
            """Gather two sleepers and report the loop time."""
            await gather(_value_after(FIRST, MEDIUM), _value_after(SECOND, MEDIUM))
            return get_running_loop().time()

        assert _run(main()) == MEDIUM

    def test_gather_empty_returns_empty_list(self) -> None:
        """Return ``[]`` when given nothing to gather."""

        async def main() -> list[BaseException]:
            """Gather nothing."""
            return await gather()

        assert _run(main()) == []

    def test_gather_accepts_tasks_and_futures(self) -> None:
        """Use tasks and futures as they are."""

        async def main() -> list[int | BaseException]:
            """Gather a task and a future filled by a timer."""
            loop = get_running_loop()
            future: Future[int] = Future(loop)
            loop.call_later(SHORT, lambda: future.set_result(SECOND))
            task = spawn(_value_after(FIRST, SHORT))
            return await gather(task, future)

        assert _run(main()) == [FIRST, SECOND]

    def test_gather_first_error_cancels_siblings(self) -> None:
        """Raise the first error and cancel the children still running."""
        events: list[str] = []

        async def sibling() -> int:
            """Sleep for ages, recording when cancelled."""
            try:
                await sleep(LONG)
            except Cancelled:
                events.append("cancelled")
                raise
            return FIRST

        async def main() -> None:
            """Gather a failing child with a slow sibling."""
            with pytest.raises(BoomError):
                await gather(sibling(), _boom_after(SHORT))
            events.append("gather raised")

        _run(main())

        assert events == ["cancelled", "gather raised"]

    def test_gather_return_exceptions(self) -> None:
        """Put errors in the result list instead of raising them."""

        async def main() -> list[int | BaseException]:
            """Gather a success and a failure."""
            return await gather(
                _value_after(FIRST, SHORT), _boom_after(SHORT), return_exceptions=True
            )

        results = _run(main())

        assert results[0] == FIRST
        assert isinstance(results[1], BoomError)

    def test_gather_return_exceptions_includes_cancelled_child(self) -> None:
        """Report a child cancelled elsewhere as a ``Cancelled`` result."""

        async def main() -> list[int | BaseException]:
            """Cancel one gathered task from outside the gather."""
            task = spawn(_value_after(FIRST, LONG))
            task.cancel()
            return await gather(task, _value_after(SECOND, SHORT), return_exceptions=True)

        results = _run(main())

        assert isinstance(results[0], Cancelled)
        assert results[1] == SECOND

    def test_cancelling_gather_cancels_children(self) -> None:
        """Cancel every child when the gathering task is cancelled."""
        children: list[Task[int]] = []

        async def gatherer() -> None:
            """Gather two slow tasks."""
            children.extend([spawn(_value_after(FIRST, LONG)), spawn(_value_after(SECOND, LONG))])
            await gather(*children)

        async def main() -> list[bool]:
            """Cancel the gatherer while it waits."""
            task = spawn(gatherer())
            await sleep(SHORT)
            task.cancel()
            with pytest.raises(Cancelled):
                await task
            return [child.cancelled() for child in children]

        assert _run(main()) == [True, True]

    def test_gather_reports_secondary_errors(self) -> None:
        """Report, not swallow, errors from children after the first one."""
        seen: list[ErrorContext] = []

        async def bad_cleanup() -> int:
            """Raise a different error while being cancelled."""
            try:
                await sleep(LONG)
            finally:
                message = "cleanup failed"
                raise ValueError(message)

        async def main() -> None:
            """Gather a failing child with a child whose cleanup fails."""
            get_running_loop().set_exception_handler(seen.append)
            with pytest.raises(BoomError):
                await gather(bad_cleanup(), _boom_after(SHORT))

        _run(main())

        assert len(seen) == FIRST
        assert isinstance(seen[0].exception, ValueError)

    def test_gather_rejects_future_from_other_loop(self) -> None:
        """Refuse a future that belongs to a different loop."""
        other_loop = EventLoop()
        foreign: Future[int] = Future(other_loop)

        async def main() -> None:
            """Gather a future from a loop that is not running."""
            with pytest.raises(ValueError, match="different loop"):
                await gather(foreign)

        _run(main())
        other_loop.close()
