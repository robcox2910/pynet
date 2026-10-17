"""Test futures, tasks, ``run``, ``spawn`` and ``sleep``."""

import contextlib
import gc
import threading
import time
from collections.abc import Generator
from typing import Any

import pytest

from pynet.errors import PyNetError
from pynet.loop import (
    Cancelled,
    ErrorContext,
    EventLoop,
    Future,
    InvalidStateError,
    Task,
    current_task,
    get_running_loop,
    run,
    sleep,
    spawn,
)
from pynet.loop.testing import FakeClock

ANSWER = 42
OTHER_ANSWER = 7
DELAY = 0.1
OVERLAP_LIMIT = 2 * DELAY
LONG_SLEEP = 1000.0
INSTANT_LIMIT = 1.0
ONE = 1
TWO = 2
NO_CANCELS = 0
TASK_NAME = "worker"
CANCEL_MESSAGE = "stop now"
STEPS = 200


class BoomError(Exception):
    """Mark an error raised on purpose by a test."""


async def _answer() -> int:
    """Return the answer without awaiting anything."""
    return ANSWER


async def _boom() -> None:
    """Raise ``BoomError`` straight away."""
    message = "boom"
    raise BoomError(message)


class Foreign:
    """Pretend to be an awaitable from another library."""

    def __await__(self) -> Generator[str]:
        """Yield something that is not a pynet Future."""
        yield "not a future"


class TestRun:
    """Test the ``run`` entry point."""

    def test_run_returns_coroutine_result(self) -> None:
        """Return whatever the main coroutine returns."""
        assert run(_answer()) == ANSWER

    def test_run_reraises_exception(self) -> None:
        """Re-raise the exception that escaped the main coroutine."""
        with pytest.raises(BoomError, match="boom"):
            run(_boom())

    def test_run_closes_its_loop(self) -> None:
        """Close the loop that ``run`` created once main finishes."""
        loops: list[EventLoop] = []

        async def main() -> None:
            """Remember the running loop."""
            loops.append(get_running_loop())

        run(main())

        assert loops[0].is_closed()

    def test_run_cancels_leftover_tasks(self) -> None:
        """Cancel tasks still pending when main returns and let them clean up."""
        cleaned: list[str] = []

        async def straggler() -> None:
            """Sleep for ages, recording cleanup when cancelled."""
            try:
                await sleep(LONG_SLEEP)
            finally:
                cleaned.append("done")

        async def main() -> int:
            """Spawn a straggler, let it start, then return."""
            spawn(straggler())
            spawn(straggler())
            await sleep(0)
            return ANSWER

        assert run(main()) == ANSWER
        assert cleaned == ["done", "done"]

    def test_run_reports_leftover_task_failure(self) -> None:
        """Report a leftover task that fails while being cancelled."""
        seen: list[ErrorContext] = []

        async def bad_cleanup() -> None:
            """Raise from cleanup when cancelled."""
            try:
                await sleep(LONG_SLEEP)
            finally:
                message = "cleanup failed"
                raise BoomError(message)

        async def main() -> None:
            """Spawn the bad task and return."""
            get_running_loop().set_exception_handler(seen.append)
            spawn(bad_cleanup())
            await sleep(0)

        run(main())

        assert len(seen) == ONE
        assert isinstance(seen[0].exception, BoomError)

    def test_run_inside_running_loop_raises(self) -> None:
        """Refuse to start a nested ``run`` from inside a running loop."""

        async def main() -> None:
            """Try to call ``run`` again."""
            coro = _answer()
            try:
                with pytest.raises(RuntimeError, match="running"):
                    run(coro)
            finally:
                coro.close()

        run(main())

    def test_sleep_with_autojump_clock_is_instant(self) -> None:
        """Finish a 1000-second sleep at once with an autojump fake clock."""
        clock = FakeClock(autojump=True)

        async def main() -> float:
            """Sleep a long virtual time and report the loop time."""
            await sleep(LONG_SLEEP)
            return get_running_loop().time()

        started = time.monotonic()
        finished_at = run(main(), clock=clock)

        assert time.monotonic() - started < INSTANT_LIMIT
        assert finished_at >= LONG_SLEEP


class TestSleep:
    """Test ``sleep``."""

    def test_two_sleeps_overlap(self) -> None:
        """Run two sleeping tasks at the same time, not one after the other."""

        async def main() -> None:
            """Spawn two sleepers and wait for both."""
            first = spawn(sleep(DELAY))
            second = spawn(sleep(DELAY))
            await first
            await second

        started = time.monotonic()
        run(main())

        assert time.monotonic() - started < OVERLAP_LIMIT

    def test_sleep_zero_lets_other_tasks_run(self) -> None:
        """Yield to other ready tasks with ``sleep(0)``."""
        order: list[str] = []

        async def other() -> None:
            """Record that the other task ran."""
            order.append("other")

        async def main() -> None:
            """Spawn a task, yield once, then record."""
            spawn(other())
            await sleep(0)
            await sleep(0)
            order.append("main")

        run(main())

        assert order == ["other", "main"]


class TestFuture:
    """Test the ``Future`` box."""

    def test_done_callbacks_run_later_not_inline(self) -> None:
        """Schedule done callbacks on the loop instead of calling them inline."""
        loop = EventLoop()
        future: Future[int] = Future(loop)
        seen: list[int] = []
        future.add_done_callback(lambda f: seen.append(f.result()))

        future.set_result(ANSWER)
        assert seen == []
        loop.run_once()

        assert seen == [ANSWER]
        loop.close()

    def test_callback_added_after_done_also_runs_later(self) -> None:
        """Schedule a callback added to an already-finished future."""
        loop = EventLoop()
        future: Future[int] = Future(loop)
        future.set_result(ANSWER)
        seen: list[int] = []

        future.add_done_callback(lambda f: seen.append(f.result()))
        assert seen == []
        loop.run_once()

        assert seen == [ANSWER]
        loop.close()

    def test_remove_done_callback_counts_removals(self) -> None:
        """Remove every copy of a callback and return how many there were."""
        loop = EventLoop()
        future: Future[int] = Future(loop)
        seen: list[int] = []

        def callback(f: Future[int]) -> None:
            """Record the result."""
            seen.append(f.result())

        future.add_done_callback(callback)
        future.add_done_callback(callback)

        assert future.remove_done_callback(callback) == TWO
        future.set_result(ANSWER)
        loop.call_soon(lambda: None)
        loop.run_once()
        assert seen == []
        loop.close()

    def test_result_before_done_raises_invalid_state(self) -> None:
        """Raise ``InvalidStateError`` when asking a pending future for its result."""
        loop = EventLoop()
        future: Future[int] = Future(loop)

        with pytest.raises(InvalidStateError):
            future.result()
        with pytest.raises(InvalidStateError):
            future.exception()
        assert issubclass(InvalidStateError, PyNetError)
        loop.close()

    def test_set_result_twice_raises_invalid_state(self) -> None:
        """Refuse to fill a future that is already done."""
        loop = EventLoop()
        future: Future[int] = Future(loop)
        future.set_result(ANSWER)

        with pytest.raises(InvalidStateError):
            future.set_result(OTHER_ANSWER)
        with pytest.raises(InvalidStateError):
            future.set_exception(BoomError())
        loop.close()

    def test_exception_is_returned_and_raised(self) -> None:
        """Return the stored exception and raise it from ``result``."""
        loop = EventLoop()
        future: Future[int] = Future(loop)
        error = BoomError("bad")
        future.set_exception(error)

        assert future.done()
        assert future.exception() is error
        with pytest.raises(BoomError):
            future.result()
        loop.close()

    def test_set_exception_rejects_stop_iteration(self) -> None:
        """Refuse ``StopIteration``, which cannot travel through a coroutine."""
        loop = EventLoop()
        future: Future[int] = Future(loop)

        with pytest.raises(TypeError):
            future.set_exception(StopIteration())
        loop.close()

    def test_cancel_marks_future_cancelled(self) -> None:
        """Cancel a pending future; a done one cannot be cancelled."""
        loop = EventLoop()
        future: Future[int] = Future(loop)

        assert future.cancel(CANCEL_MESSAGE)
        assert future.cancelled()
        assert future.done()
        assert not future.cancel()
        with pytest.raises(Cancelled, match=CANCEL_MESSAGE):
            future.result()
        with pytest.raises(Cancelled):
            future.exception()
        loop.close()

    def test_cancelled_is_not_an_exception(self) -> None:
        """Make ``Cancelled`` a BaseException so ``except Exception`` misses it."""
        assert issubclass(Cancelled, BaseException)
        assert not issubclass(Cancelled, Exception)

    def test_get_loop_returns_owner(self) -> None:
        """Return the loop the future belongs to."""
        loop = EventLoop()
        future: Future[int] = Future(loop)

        assert future.get_loop() is loop
        loop.close()

    def test_await_future_resolved_by_timer(self) -> None:
        """Resume a coroutine when the future it awaits is resolved."""

        async def main() -> int:
            """Await a future that a timer fills in."""
            loop = get_running_loop()
            future: Future[int] = Future(loop)
            loop.call_later(0, lambda: future.set_result(ANSWER))
            return await future

        assert run(main()) == ANSWER

    def test_unretrieved_exception_reported(self) -> None:
        """Report a task exception nobody ever looked at once it is collected."""
        seen: list[ErrorContext] = []

        async def main() -> None:
            """Spawn a failing task, never await it, and let it finish."""
            get_running_loop().set_exception_handler(seen.append)
            spawn(_boom())
            await sleep(0)
            await sleep(0)

        run(main())
        gc.collect()

        assert len(seen) == ONE
        assert seen[0].message == "Future exception was never retrieved"
        assert isinstance(seen[0].exception, BoomError)

    def test_retrieved_exception_not_reported(self) -> None:
        """Stay quiet about an exception that was looked at."""
        seen: list[ErrorContext] = []
        loop = EventLoop()
        loop.set_exception_handler(seen.append)
        future: Future[int] = Future(loop)
        future.set_exception(BoomError())
        assert isinstance(future.exception(), BoomError)

        del future
        gc.collect()

        assert seen == []
        loop.close()


class TestTask:
    """Test tasks driving coroutines."""

    def test_awaiting_foreign_awaitable_raises_type_error(self) -> None:
        """Throw a helpful ``TypeError`` when awaiting a non-pynet awaitable."""
        messages: list[str] = []

        async def main() -> None:
            """Await something from another library."""
            with pytest.raises(TypeError, match="only pynet Futures") as info:
                await Foreign()
            messages.append(str(info.value))

        run(main())

        assert "asyncio" in messages[0]

    def test_awaiting_future_from_other_loop_raises(self) -> None:
        """Refuse a future that belongs to a different loop."""
        other = EventLoop()

        async def main() -> None:
            """Await a future owned by another loop."""
            future: Future[int] = Future(other)
            with pytest.raises(RuntimeError, match="different loop"):
                await future

        run(main())
        other.close()

    def test_get_running_loop_outside_loop_raises(self) -> None:
        """Raise ``RuntimeError`` when no loop is running in this thread."""
        with pytest.raises(RuntimeError, match="no running"):
            get_running_loop()
        assert current_task() is None

    def test_get_running_loop_set_by_run_once(self) -> None:
        """Track the running loop even when ``run_once`` is driven by hand."""
        loop = EventLoop()
        seen: list[EventLoop] = []
        loop.call_soon(lambda: seen.append(get_running_loop()))

        loop.run_once()

        assert seen == [loop]
        with pytest.raises(RuntimeError):
            get_running_loop()
        loop.close()

    def test_current_task_inside_task(self) -> None:
        """Return the task whose coroutine is running right now."""
        seen: list[Task[Any] | None] = []

        async def child() -> None:
            """Record the current task."""
            seen.append(current_task())

        async def main() -> None:
            """Record the current task, then spawn and await a child."""
            seen.append(current_task())
            task = spawn(child())
            await task
            seen.append(task)

        run(main())

        assert isinstance(seen[0], Task)
        assert seen[1] is seen[2]
        assert seen[0] is not seen[1]

    def test_task_names(self) -> None:
        """Use the given name, or make up a unique default one."""
        names: list[str] = []

        async def main() -> None:
            """Spawn named and unnamed tasks."""
            named = spawn(sleep(0), name=TASK_NAME)
            first = spawn(sleep(0))
            second = spawn(sleep(0))
            names.extend([named.get_name(), first.get_name(), second.get_name()])
            await named
            await first
            await second

        run(main())

        assert names[0] == TASK_NAME
        assert names[1].startswith("Task-")
        assert names[1] != names[2]

    def test_task_rejects_set_result(self) -> None:
        """Forbid filling a task by hand; only its coroutine may finish it."""

        async def main() -> None:
            """Try to set a task's result and exception directly."""
            task = spawn(sleep(0))
            with pytest.raises(RuntimeError):
                task.set_result(None)
            with pytest.raises(RuntimeError):
                task.set_exception(BoomError())
            await task

        run(main())

    def test_task_requires_coroutine(self) -> None:
        """Reject something that is not a coroutine."""
        loop = EventLoop()

        with pytest.raises(TypeError):
            Task(_answer, loop)  # type: ignore[arg-type]
        loop.close()

    def test_cancel_throws_cancelled_and_runs_finally(self) -> None:
        """Throw ``Cancelled`` at the await, run ``finally``, and end cancelled."""
        events: list[str] = []

        async def worker() -> None:
            """Sleep for ages and record cleanup."""
            try:
                events.append("started")
                await sleep(LONG_SLEEP)
                events.append("woke")
            finally:
                events.append("finally")

        async def main() -> None:
            """Start the worker, cancel it, and check it ended cancelled."""
            task = spawn(worker())
            await sleep(0)
            assert task.cancel(CANCEL_MESSAGE)
            assert task.cancelling() == ONE
            with pytest.raises(Cancelled):
                await task
            assert task.cancelled()
            assert not task.cancel()

        run(main())

        assert events == ["started", "finally"]

    def test_cancel_before_first_step(self) -> None:
        """Throw ``Cancelled`` at the very first step of a cancelled task."""
        events: list[str] = []

        async def worker() -> None:
            """Record that the body ran."""
            events.append("ran")

        async def main() -> None:
            """Cancel a task before it gets to run."""
            task = spawn(worker())
            task.cancel()
            with pytest.raises(Cancelled):
                await task

        run(main())

        assert events == []

    def test_caught_cancel_completes_with_result(self) -> None:
        """Finish normally when the coroutine catches ``Cancelled`` and returns."""

        async def stubborn() -> int:
            """Swallow cancellation and return a value."""
            try:
                await sleep(LONG_SLEEP)
            except Cancelled:
                return ANSWER
            return OTHER_ANSWER

        async def main() -> int:
            """Cancel the stubborn task and return its result."""
            task = spawn(stubborn())
            await sleep(0)
            task.cancel()
            result = await task
            assert not task.cancelled()
            return result

        assert run(main()) == ANSWER

    def test_cancelling_and_uncancel_count_requests(self) -> None:
        """Count cancel requests; ``uncancel`` takes one back."""

        async def stubborn() -> None:
            """Swallow cancellation."""
            with contextlib.suppress(Cancelled):
                await sleep(LONG_SLEEP)

        async def main() -> None:
            """Cancel twice, uncancel twice, and check the counter."""
            task = spawn(stubborn())
            await sleep(0)
            task.cancel()
            task.cancel()
            assert task.cancelling() == TWO
            assert task.uncancel() == ONE
            assert task.uncancel() == NO_CANCELS
            assert task.uncancel() == NO_CANCELS
            await task

        run(main())

    def test_self_cancel_then_return_ends_cancelled(self) -> None:
        """End cancelled when a task cancels itself and returns before an await."""

        async def selfish() -> int:
            """Cancel the current task, then return."""
            task = current_task()
            assert task is not None
            task.cancel()
            return ANSWER

        async def main() -> None:
            """Await the self-cancelling task."""
            task = spawn(selfish())
            with pytest.raises(Cancelled):
                await task
            assert task.cancelled()

        run(main())

    def test_cancel_awaiting_child_task_cancels_child(self) -> None:
        """Pass cancellation down to the task being awaited."""
        events: list[str] = []

        async def child() -> None:
            """Sleep for ages and record cancellation."""
            try:
                await sleep(LONG_SLEEP)
            except Cancelled:
                events.append("child cancelled")
                raise

        async def parent() -> None:
            """Await the child."""
            await spawn(child())

        async def main() -> None:
            """Cancel the parent while it awaits the child."""
            task = spawn(parent())
            await sleep(0)
            await sleep(0)
            task.cancel()
            with pytest.raises(Cancelled):
                await task

        run(main())

        assert events == ["child cancelled"]

    def test_task_exception_propagates_to_awaiter(self) -> None:
        """Re-raise a task's exception in whoever awaits it."""

        async def main() -> None:
            """Await a failing task."""
            with pytest.raises(BoomError):
                await spawn(_boom())

        run(main())

    def test_spawn_outside_loop_raises(self) -> None:
        """Refuse to spawn when no loop is running."""
        coro = _answer()
        with pytest.raises(RuntimeError):
            spawn(coro)
        coro.close()

    def test_self_cancel_then_await_throws_at_await(self) -> None:
        """Throw ``Cancelled`` at the next await after a task cancels itself."""
        events: list[str] = []

        async def selfish() -> None:
            """Cancel the current task, then try to sleep."""
            task = current_task()
            assert task is not None
            task.cancel()
            try:
                await sleep(0)
            except Cancelled:
                events.append("cancelled at await")
                raise

        async def main() -> None:
            """Await the self-cancelling task."""
            with pytest.raises(Cancelled):
                await spawn(selfish())

        run(main())

        assert events == ["cancelled at await"]

    def test_task_awaiting_itself_raises(self) -> None:
        """Throw ``RuntimeError`` into a task that awaits itself."""

        async def narcissist() -> None:
            """Await the current task."""
            task = current_task()
            assert task is not None
            with pytest.raises(RuntimeError, match="itself"):
                await task

        run(narcissist())

    def test_keyboard_interrupt_escapes_run(self) -> None:
        """Let ``KeyboardInterrupt`` stop the loop instead of hiding in a task."""

        async def interrupted() -> None:
            """Raise ``KeyboardInterrupt``."""
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            run(interrupted())

    def test_keyboard_interrupt_still_cancels_leftovers(self) -> None:
        """Cancel and clean up leftover tasks even when main is interrupted."""
        cleaned: list[str] = []

        async def straggler() -> None:
            """Sleep for ages, recording cleanup when cancelled."""
            try:
                await sleep(LONG_SLEEP)
            finally:
                cleaned.append("done")

        async def main() -> None:
            """Spawn a straggler, let it start, then get interrupted."""
            spawn(straggler())
            await sleep(0)
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            run(main())

        assert cleaned == ["done"]

    def test_interrupt_does_not_drop_the_rest_of_a_turn(self) -> None:
        """Keep callbacks queued behind an interrupt so leftovers can be cancelled."""
        bystanders: list[Task[None]] = []

        async def interrupter() -> None:
            """Raise ``KeyboardInterrupt`` on the first step."""
            raise KeyboardInterrupt

        async def bystander() -> None:
            """Sleep for ages."""
            await sleep(LONG_SLEEP)

        async def main() -> None:
            """Start both tasks so their first steps share one loop turn."""
            spawn(interrupter())
            bystanders.append(spawn(bystander()))
            await sleep(LONG_SLEEP)

        with pytest.raises(KeyboardInterrupt):
            run(main(), clock=FakeClock(autojump=True))

        assert bystanders[0].cancelled()

    def test_registry_is_per_loop(self) -> None:
        """Track the current task separately for loops running in two threads."""
        barrier = threading.Barrier(TWO)
        mismatches: list[str] = []
        finished: list[int] = []

        async def worker() -> None:
            """Check, across many pauses, that the current task is this task."""
            me = current_task()
            for _ in range(STEPS):
                await sleep(0)
                if current_task() is not me:
                    mismatches.append("wrong task")

        async def main() -> None:
            """Wait for the other thread's loop, then run workers side by side."""
            barrier.wait()
            tasks = [spawn(worker()) for _ in range(TWO)]
            for task in tasks:
                await task
            finished.append(ONE)

        threads = [threading.Thread(target=run, args=(main(),)) for _ in range(TWO)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert mismatches == []
        assert len(finished) == TWO

    def test_other_loop_cannot_run_inside_a_turn(self) -> None:
        """Refuse to run a second loop's turn from inside the first loop's turn."""
        outer = EventLoop()
        inner = EventLoop()
        inner.call_soon(lambda: None)
        errors: list[str] = []

        def nested() -> None:
            """Try to drive the inner loop."""
            try:
                inner.run_once()
            except RuntimeError as error:
                errors.append(str(error))

        outer.call_soon(nested)
        outer.run_once()

        assert len(errors) == ONE
        assert "another loop" in errors[0]
        outer.close()
        inner.close()
