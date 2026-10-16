"""Test the worker pool and run_in_thread."""

import contextlib
import threading
import time

import pytest

from pynet.loop import EventLoop, WorkerPool, gather, run, run_in_thread, sleep, spawn

VALUE = 42
SLEEP_SECONDS = 0.1
BLOCKING_CALLS = 4
OVERLAP_LIMIT = 0.3
TICK_SECONDS = 0.01
MIN_TICKS = 3
BLOCK_SECONDS = 0.2
POOL_SIZE = 2
JOBS = 6
ADDEND = 3
RACE_ROUNDS = 200


class BoomError(Exception):
    """Signal a failure inside a worker thread."""


def _fail() -> int:
    """Raise ``BoomError``."""
    raise BoomError


def _add(left: int, right: int = 0) -> int:
    """Return ``left + right``."""
    return left + right


class TestRunInThread:
    """Test sending blocking work to helper threads."""

    def test_run_in_thread_returns_value(self) -> None:
        """Return the function's value, passing args and kwargs."""

        async def main() -> int:
            """Run a sum in a thread."""
            return await run_in_thread(_add, VALUE, right=ADDEND)

        assert run(main()) == VALUE + ADDEND

    def test_run_in_thread_reraises(self) -> None:
        """Re-raise the worker's exception in the awaiting coroutine."""

        async def main() -> int:
            """Run a failing function in a thread."""
            return await run_in_thread(_fail)

        with pytest.raises(BoomError):
            run(main())

    def test_runs_off_loop_thread(self) -> None:
        """Run the function in a different thread than the loop's."""

        async def main() -> int:
            """Return the worker's thread id."""
            return await run_in_thread(threading.get_ident)

        assert run(main()) != threading.get_ident()

    def test_blocking_calls_overlap(self) -> None:
        """Run blocking calls side by side rather than one after another."""

        async def main() -> None:
            """Sleep in four threads at once."""
            await gather(*(run_in_thread(time.sleep, SLEEP_SECONDS) for _ in range(BLOCKING_CALLS)))

        start = time.monotonic()
        run(main())
        assert time.monotonic() - start < OVERLAP_LIMIT

    def test_loop_stays_responsive(self) -> None:
        """Keep other tasks ticking while a thread blocks."""
        ticks: list[int] = []

        async def ticker() -> None:
            """Count ticks until cancelled."""
            while True:
                await sleep(TICK_SECONDS)
                ticks.append(1)

        async def main() -> None:
            """Block a thread while the ticker runs."""
            task = spawn(ticker())
            await run_in_thread(time.sleep, BLOCK_SECONDS)
            task.cancel()

        run(main())
        assert len(ticks) >= MIN_TICKS

    def test_cancel_discards_result(self, caplog: pytest.LogCaptureFixture) -> None:
        """Cancel the awaiting task and drop the late result silently."""

        async def main() -> None:
            """Cancel a task awaiting a slow thread."""
            task = spawn(run_in_thread(time.sleep, SLEEP_SECONDS))
            await sleep(0)
            task.cancel()
            await sleep(SLEEP_SECONDS * 2)

        run(main())
        assert not caplog.records

    def test_result_after_loop_closed_is_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        """Swallow delivery to a closed loop quietly."""

        async def main() -> None:
            """Leave a slow job running when the loop shuts down."""
            spawn(run_in_thread(time.sleep, SLEEP_SECONDS))
            await sleep(0)

        run(main())
        assert not caplog.records


class TestWorkerPool:
    """Test the pool itself."""

    def test_shutdown_joins_workers(self) -> None:
        """Finish queued jobs and stop every thread on shutdown(wait=True)."""
        pool = WorkerPool(POOL_SIZE)
        done: list[int] = []
        lock = threading.Lock()

        def job() -> int:
            """Record completion."""
            time.sleep(TICK_SECONDS)
            return 1

        def on_done(value: int | None, error: BaseException | None) -> None:
            """Collect results."""
            assert error is None
            with lock:
                done.append(value or 0)

        for _ in range(JOBS):
            pool.submit(job, on_done)
        pool.shutdown(wait=True)
        assert len(done) == JOBS
        assert pool.thread_count() == 0

    def test_submit_after_shutdown_raises(self) -> None:
        """Refuse new work once shut down."""
        pool = WorkerPool()
        pool.shutdown()
        with pytest.raises(RuntimeError):
            pool.submit(int, lambda _v, _e: None)

    def test_threads_start_lazily_up_to_max(self) -> None:
        """Start no threads until work arrives, and never exceed the cap."""
        pool = WorkerPool(POOL_SIZE)
        assert pool.thread_count() == 0
        for _ in range(JOBS):
            pool.submit(int, lambda _v, _e: None)
        assert pool.thread_count() <= POOL_SIZE
        pool.shutdown()

    def test_loop_close_shuts_pool_down(self) -> None:
        """Shut the loop's pool down when the loop closes."""
        loop = EventLoop()
        pool = loop.worker_pool()
        assert loop.worker_pool() is pool
        loop.close()
        with pytest.raises(RuntimeError):
            pool.submit(int, lambda _v, _e: None)

    def test_zero_workers_rejected(self) -> None:
        """Reject a pool that could never run anything."""
        with pytest.raises(ValueError, match="max_workers"):
            WorkerPool(0)

    def test_shutdown_twice_is_harmless(self) -> None:
        """Allow shutdown to be called again."""
        pool = WorkerPool()
        pool.shutdown()
        pool.shutdown(wait=False)

    def test_failing_completion_callback_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log an exception raised by the completion callback."""
        pool = WorkerPool(1)

        def bad(_value: int | None, _error: BaseException | None) -> None:
            """Raise to prove errors are reported."""
            raise BoomError

        pool.submit(int, bad)
        pool.shutdown()
        assert "completion callback" in caplog.text


class TestStopIteration:
    """Test the one exception a Future cannot hold."""

    def test_stop_iteration_becomes_runtime_error(self) -> None:
        """Convert StopIteration from the worker into RuntimeError."""

        def stop() -> int:
            """Raise StopIteration."""
            raise StopIteration

        async def main() -> int:
            """Run it in a thread."""
            return await run_in_thread(stop)

        with pytest.raises(RuntimeError):
            run(main())


class TestCloseRace:
    """Test workers finishing while the loop closes."""

    def test_close_while_workers_deliver(self, caplog: pytest.LogCaptureFixture) -> None:
        """Close the loop amid finishing workers without logging or hanging."""
        for _ in range(RACE_ROUNDS):
            loop = EventLoop()
            pool = loop.worker_pool()
            start = threading.Event()

            def job(start: threading.Event = start) -> int:
                """Wait for the starting gun."""
                start.wait()
                return 1

            def on_done(
                _value: int | None, _error: BaseException | None, loop: EventLoop = loop
            ) -> None:
                """Deliver to the loop, dropping it if closed."""
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(int)

            for _ in range(POOL_SIZE * 2):
                pool.submit(job, on_done)
            start.set()
            loop.close()
        assert not caplog.records
