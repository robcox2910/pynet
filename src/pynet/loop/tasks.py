"""The stage manager -- pushes each actor (coroutine) on stage when it is their cue.

A coroutine is an actor who says a few lines and then pauses, holding
up a ``Future`` that says "wake me when this is ready". A ``Task`` is
the stage manager for one actor: it sends the actor on (``send``),
takes the future they hold up, and asks that future to call it back
when it is done. Then it pushes the actor on again, right where they
paused. Many tasks share one loop, so many actors take turns on a
single stage.

``run`` builds a fresh loop, runs one main coroutine to the end, tidies
away any tasks still running, and closes the loop.
"""

import inspect
import itertools
import weakref
from collections.abc import Callable, Coroutine
from typing import Any

from pynet.loop.futures import Cancelled, Future
from pynet.loop.reactor import ErrorContext, EventLoop, running_loop

SHUTDOWN_MESSAGE = "Unhandled exception in a task cancelled by run() shutdown"

_task_ids = itertools.count(1)
_current_tasks: dict[EventLoop, Task[Any]] = {}
_pending_tasks: weakref.WeakKeyDictionary[EventLoop, set[Task[Any]]] = weakref.WeakKeyDictionary()


class Task[T](Future[T]):
    """Drive a coroutine on a loop, finishing with its result.

    Args:
        coro: The coroutine to run.
        loop: The loop to run it on.
        name: A label for debugging; defaults to ``Task-<n>``.

    """

    def __init__(
        self, coro: Coroutine[Any, Any, T], loop: EventLoop, name: str | None = None
    ) -> None:
        """Wrap ``coro`` and schedule its first step on ``loop``.

        Raises:
            TypeError: If ``coro`` is not a coroutine.

        """
        if not inspect.iscoroutine(coro):
            message = f"a coroutine was expected, got {coro!r}"
            raise TypeError(message)
        super().__init__(loop)
        self._coro = coro
        self._name = name if name is not None else f"Task-{next(_task_ids)}"
        self._waiting_on: Future[Any] | None = None
        self._must_cancel = False
        self._num_cancels = 0
        loop.call_soon(self._step)
        _pending_tasks.setdefault(loop, set()).add(self)

    def get_name(self) -> str:
        """Return the task's name."""
        return self._name

    def set_result(self, value: T) -> None:  # noqa: ARG002
        """Refuse: only the coroutine can finish a task.

        Raises:
            RuntimeError: Always.

        """
        message = "Task does not support set_result()"
        raise RuntimeError(message)

    def set_exception(self, exception: BaseException) -> None:  # noqa: ARG002
        """Refuse: only the coroutine can finish a task.

        Raises:
            RuntimeError: Always.

        """
        message = "Task does not support set_exception()"
        raise RuntimeError(message)

    def cancel(self, msg: str = "") -> bool:
        """Ask the task to stop; return False if it is already done.

        Each call bumps the ``cancelling`` count. If the task is waiting on
        a future, that future is cancelled, so ``Cancelled`` pops out of the
        coroutine's ``await``. Otherwise ``Cancelled`` is thrown in at the
        next step. The coroutine may catch it and carry on.
        """
        if self.done():
            return False
        self._num_cancels += 1
        self._cancel_message = msg
        waiting_on = self._waiting_on
        if waiting_on is not None and waiting_on.cancel(msg):
            return True
        self._must_cancel = True
        return True

    def cancelling(self) -> int:
        """Return how many cancel requests are outstanding."""
        return self._num_cancels

    def uncancel(self) -> int:
        """Take back one cancel request; return how many remain."""
        if self._num_cancels > 0:
            self._num_cancels -= 1
        if self._num_cancels == 0:
            self._must_cancel = False
        return self._num_cancels

    def _step(self, exc: BaseException | None = None) -> None:
        """Push the coroutine on until it pauses on a future or finishes."""
        if self._must_cancel:
            exc = self._make_cancelled()
            self._must_cancel = False
        self._waiting_on = None
        loop = self.get_loop()
        _current_tasks[loop] = self  # steps never nest: callbacks are never inline
        try:
            yielded = self._coro.send(None) if exc is None else self._coro.throw(exc)
        except StopIteration as stop:
            if self._must_cancel:
                self._finish_cancelled(self._cancel_message)
            else:
                super().set_result(stop.value)
                self._forget()
        except Cancelled as cancelled:
            self._finish_cancelled(str(cancelled) if cancelled.args else "")
        except (KeyboardInterrupt, SystemExit) as error:
            super().set_exception(error)
            self._forget()
            self._unretrieved = False  # it is propagating, so it is not lost
            raise
        except BaseException as error:  # noqa: BLE001
            super().set_exception(error)
            self._forget()
        else:
            self._handle_yield(yielded)
        finally:
            del _current_tasks[loop]

    def _handle_yield(self, yielded: object) -> None:
        """Wait on a yielded future, or throw a helpful error back in."""
        loop = self.get_loop()
        error: BaseException | None = None
        if not isinstance(yielded, Future):
            error = TypeError(
                f"only pynet Futures can be awaited, got {yielded!r}; "
                "asyncio awaitables (asyncio.sleep, asyncio.Future, ...) "
                "do not work on a pynet loop"
            )
        elif yielded is self:
            error = RuntimeError("A task cannot await itself")
        else:
            future: Future[Any] = yielded  # pyright: ignore[reportUnknownVariableType]
            if future.get_loop() is not loop:
                error = RuntimeError("Cannot await a Future that belongs to a different loop")
            else:
                self._waiting_on = future
                future.add_done_callback(self._wakeup)
                if self._must_cancel and future.cancel(self._cancel_message):
                    self._must_cancel = False
                return
        loop.call_soon(lambda: self._step(error))

    def _wakeup(self, _future: Future[Any]) -> None:
        """Resume the coroutine now that the awaited future is done."""
        self._step()

    def _finish_cancelled(self, msg: str) -> None:
        """Mark the task cancelled and drop it from the pending set."""
        super().cancel(msg)
        self._forget()

    def _forget(self) -> None:
        """Remove the task from its loop's pending set."""
        _pending_tasks[self.get_loop()].discard(self)


def get_running_loop() -> EventLoop:
    """Return the loop running in this thread.

    Raises:
        RuntimeError: If no loop is running in this thread.

    """
    loop = running_loop()
    if loop is None:
        message = "There is no running pynet event loop in this thread"
        raise RuntimeError(message)
    return loop


def current_task() -> Task[Any] | None:
    """Return the task whose coroutine is running now, or None."""
    loop = running_loop()
    if loop is None:
        return None
    return _current_tasks.get(loop)


def spawn[T](coro: Coroutine[Any, Any, T], *, name: str | None = None) -> Task[T]:
    """Start ``coro`` as a new task on the running loop.

    Raises:
        RuntimeError: If no loop is running in this thread.

    """
    return Task(coro, get_running_loop(), name)


async def sleep(seconds: float) -> None:
    """Pause the current task for ``seconds``; ``sleep(0)`` just yields once."""
    loop = get_running_loop()
    future: Future[None] = Future(loop)

    def wake() -> None:
        """Fill the future unless it was cancelled meanwhile."""
        if not future.done():
            future.set_result(None)

    handle = loop.call_soon(wake) if seconds <= 0 else loop.call_later(seconds, wake)
    try:
        await future
    finally:
        handle.cancel()


def run[T](main: Coroutine[Any, Any, T], *, clock: Callable[[], float] | None = None) -> T:
    """Run ``main`` on a brand-new loop and return its result.

    When ``main`` finishes, any tasks still running are cancelled and given
    the chance to clean up, then the loop is closed.

    Args:
        main: The coroutine to run.
        clock: The loop's clock; defaults to ``time.monotonic``.

    Raises:
        RuntimeError: If a loop is already running in this thread.

    """
    if running_loop() is not None:
        main.close()
        message = "run() cannot be called while a loop is running in this thread"
        raise RuntimeError(message)
    loop = EventLoop() if clock is None else EventLoop(clock=clock)
    try:
        task = Task(main, loop)
        _run_until_done(loop, [task])
        _cancel_leftovers(loop)
        return task.result()
    finally:
        loop.close()


def _run_until_done(loop: EventLoop, futures: list[Future[Any]]) -> None:
    """Run ``loop`` until every future in ``futures`` is done."""
    waiting = [future for future in futures if not future.done()]

    def on_done(_future: Future[Any]) -> None:
        """Stop the loop once nothing is left to wait for."""
        if all(future.done() for future in waiting):
            loop.stop()

    for future in waiting:
        future.add_done_callback(on_done)
    while not all(future.done() for future in waiting):
        loop.run_forever()


def _cancel_leftovers(loop: EventLoop) -> None:
    """Cancel tasks still pending on ``loop`` and wait for them to finish."""
    leftovers: list[Future[Any]] = list(_pending_tasks.get(loop, set()))
    for task in leftovers:
        task.cancel()
    _run_until_done(loop, leftovers)
    for task in leftovers:
        if not task.cancelled() and task.exception() is not None:
            loop.report_error(ErrorContext(SHUTDOWN_MESSAGE, task.exception()))
