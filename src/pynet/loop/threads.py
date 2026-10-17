"""Helper kitchen staff -- send a slow errand out so the waiter keeps serving.

The event loop is one waiter. If one dish needs ten minutes of chopping
(a blocking call such as ``getaddrinfo`` or a database query), the whole
restaurant would stand still. So the waiter hands the job to a helper in
the back, keeps serving tables, and is told by the doorbell
(``call_soon_threadsafe``) when the helper is finished.

``WorkerPool`` is the team of helpers (our own threads fed from a queue);
``run_in_thread`` is the awaitable way to hand one job over.
"""

import logging
import queue
import threading
from collections.abc import Callable
from typing import cast

from pynet.loop.futures import Future
from pynet.loop.tasks import get_running_loop

logger = logging.getLogger("pynet.loop")

DEFAULT_WORKERS = 4

type DoneCallback[T] = Callable[[T | None, BaseException | None], None]
type _Job = tuple[Callable[[], object], DoneCallback[object]] | None


class WorkerPool:
    """Run blocking callables on a small team of daemon threads.

    Threads start lazily, one per submitted job, until ``max_workers``
    exist. Jobs wait in a FIFO queue; each worker takes the next one.

    Args:
        max_workers: The most threads the pool will ever start.

    """

    def __init__(self, max_workers: int = DEFAULT_WORKERS) -> None:
        """Create an empty pool that may grow to ``max_workers`` threads."""
        if max_workers < 1:
            message = "max_workers must be at least 1"
            raise ValueError(message)
        self._max_workers = max_workers
        self._queue: queue.SimpleQueue[_Job] = queue.SimpleQueue()
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._shutdown = False
        self._outstanding = 0

    def submit[T](self, fn: Callable[[], T], on_done: DoneCallback[T]) -> None:
        """Queue ``fn``; call ``on_done(result, error)`` from the worker thread.

        Exactly one of ``result`` and ``error`` is meaningful: ``error`` is
        the exception ``fn`` raised, or None on success.

        Raises:
            RuntimeError: If the pool has been shut down.

        """
        with self._lock:
            if self._shutdown:
                message = "Cannot submit to a pool that has been shut down"
                raise RuntimeError(message)
            self._outstanding += 1
            self._queue.put((fn, cast("DoneCallback[object]", on_done)))
            if len(self._threads) < self._max_workers:
                thread = threading.Thread(target=self._work, name="pynet-worker", daemon=True)
                self._threads.append(thread)
                thread.start()

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop accepting work; queued jobs still run, then workers exit.

        One sentinel per worker is queued behind the pending jobs, so
        ``wait=True`` joins every thread without hanging. Calling it again
        is harmless.
        """
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                for _ in self._threads:
                    self._queue.put(None)
            threads = list(self._threads)
        if wait:
            for thread in threads:
                thread.join()

    def busy(self) -> bool:
        """Return whether a submitted job has not finished (``on_done`` included)."""
        with self._lock:
            return self._outstanding > 0

    def thread_count(self) -> int:
        """Return how many worker threads are alive."""
        with self._lock:
            return sum(thread.is_alive() for thread in self._threads)

    def _work(self) -> None:
        """Take jobs off the queue until a sentinel arrives."""
        while True:
            job = self._queue.get()
            if job is None:
                return
            fn, on_done = job
            try:
                value = fn()
            except BaseException as exc:  # noqa: BLE001
                self._finish(on_done, None, exc)
            else:
                self._finish(on_done, value, None)
            with self._lock:
                self._outstanding -= 1

    @staticmethod
    def _finish(on_done: DoneCallback[object], value: object, error: BaseException | None) -> None:
        """Call ``on_done``; log (never lose) anything it raises."""
        try:
            on_done(value, error)
        except Exception:
            logger.exception("Exception in worker completion callback")


async def run_in_thread[**P, T](fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Run ``fn(*args, **kwargs)`` on a helper thread and await its result.

    The result (or exception) travels back with ``call_soon_threadsafe``.
    If the awaiting task is cancelled the thread still finishes, but its
    result is discarded. If the loop has been closed by then there is
    nobody to tell, so the ``RuntimeError`` from ``call_soon_threadsafe``
    is swallowed on purpose.

    Raises:
        RuntimeError: If no loop is running in this thread.

    """
    loop = get_running_loop()
    future: Future[T] = Future(loop)

    def deliver(value: T | None, error: BaseException | None) -> None:
        """Hand the outcome to the loop thread, if anyone still wants it."""

        def fill() -> None:
            """Fill the future unless it was cancelled meanwhile."""
            if future.done():
                return
            if error is not None:
                future.set_exception(
                    RuntimeError("StopIteration raised in worker")
                    if isinstance(error, StopIteration)
                    else error,
                )
            else:
                future.set_result(cast("T", value))

        try:
            loop.call_soon_threadsafe(fill)
        except RuntimeError:
            return

    loop.worker_pool().submit(lambda: fn(*args, **kwargs), deliver)
    return await future
