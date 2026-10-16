"""The referee's whistle -- stop the play when time runs out.

A ``timeout`` is a referee with a stopwatch standing next to one block
of code. If the block is still playing when the whistle blows, the
referee cancels the task, and when the ``Cancelled`` comes flying out of
the block the referee turns it into a plain ``TimeoutError``. A cancel
from anyone else is not the referee's business, so it stays a
``Cancelled``.

``wait_for`` is a timeout around a single ``await``. ``gather`` is a
team captain: it starts several awaitables together, waits for all of
them, and hands back their results in the order they were named. If one
player fouls out (raises), the captain sends the others off too.
"""

from collections.abc import Awaitable, Sequence
from types import TracebackType
from typing import Any, Literal

from pynet.loop.futures import Cancelled, Future
from pynet.loop.reactor import ErrorContext, EventLoop, TimerHandle
from pynet.loop.tasks import Task, current_task, get_running_loop

type _TimeoutState = Literal["created", "entered", "exited"]

CREATED: _TimeoutState = "created"
ENTERED: _TimeoutState = "entered"
EXITED: _TimeoutState = "exited"
LOST_ERROR_MESSAGE = "Exception in a gathered awaitable that gather() could not return"


class Timeout:
    """Cancel the current task if a block runs longer than ``seconds``.

    Use it through ``timeout()`` as ``async with timeout(seconds): ...``.

    Args:
        seconds: The time limit; None means no limit.

    """

    def __init__(self, seconds: float | None) -> None:
        """Prepare a timeout that starts counting when the block is entered."""
        self._seconds = seconds
        self._state: _TimeoutState = CREATED
        self._expired = False
        self._task: Task[Any] | None = None
        self._handle: TimerHandle | None = None
        self._cancels_at_entry = 0

    def expired(self) -> bool:
        """Return whether the whistle blew (the limit was reached)."""
        return self._expired

    async def __aenter__(self) -> Timeout:
        """Start the stopwatch for the current task.

        Raises:
            RuntimeError: If no task is running, or the timeout was
                already used.

        """
        if self._state != CREATED:
            message = "A Timeout can only be entered once"
            raise RuntimeError(message)
        task = current_task()
        if task is None:
            message = "timeout() must be used inside a task"
            raise RuntimeError(message)
        self._state = ENTERED
        self._task = task
        self._cancels_at_entry = task.cancelling()
        if self._seconds is not None:
            self._handle = task.get_loop().call_later(self._seconds, self._on_timeout)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the stopwatch; turn our own cancel into ``TimeoutError``.

        Raises:
            TimeoutError: If the whistle blew and the ``Cancelled`` leaving
                the block came only from this timeout.

        """
        self._state = EXITED
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
        task = self._task
        if not self._expired or task is None:
            return
        # Take back our own cancel request. If others remain (someone else
        # also cancelled the task), the Cancelled is theirs: let it through.
        remaining = task.uncancel()
        if isinstance(exc, Cancelled) and remaining <= self._cancels_at_entry:
            raise TimeoutError from None

    def _on_timeout(self) -> None:
        """Blow the whistle: cancel the task, unless the block already ended."""
        task = self._task
        if self._state != ENTERED or task is None:
            return
        self._expired = True
        task.cancel()


def timeout(seconds: float | None) -> Timeout:
    """Return an async context manager that limits a block to ``seconds``.

    ``None`` means no limit. When the limit is reached the block is
    cancelled and ``TimeoutError`` is raised where the block ends.
    """
    return Timeout(seconds)


async def wait_for[T](aw: Awaitable[T], seconds: float | None) -> T:
    """Await ``aw``, giving up with ``TimeoutError`` after ``seconds``.

    If ``aw`` is a task or future, it is cancelled when time runs out.
    ``None`` means wait as long as it takes.

    Raises:
        TimeoutError: If ``aw`` does not finish in time.

    """
    async with timeout(seconds):
        return await aw


async def gather[T](*aws: Awaitable[T], return_exceptions: bool = False) -> list[T | BaseException]:
    """Run ``aws`` together and return their results in argument order.

    Coroutines (and other awaitables) are wrapped in tasks; futures and
    tasks are used as they are. With ``return_exceptions=False`` the
    first error raised by a child propagates and the other children are
    cancelled. With ``return_exceptions=True`` errors (including
    ``Cancelled`` for a child cancelled elsewhere) are put in the list.
    Cancelling the gather cancels every child. Either way, ``gather``
    only returns or raises once every child has finished.

    Raises:
        ValueError: If a future belongs to a different loop.

    """
    if not aws:
        return []
    loop = get_running_loop()
    children = _as_children(aws, loop)
    remaining = len(children)
    failures: list[Future[T]] = []
    wake: Future[None] = Future(loop)
    settled: Future[None] = Future(loop)

    def on_child_done(child: Future[T]) -> None:
        """Count finished children and wake the gather when it should act."""
        nonlocal remaining
        remaining -= 1
        if child.cancelled() or child.exception() is not None:
            failures.append(child)
        if remaining == 0 and not settled.done():
            settled.set_result(None)
        should_wake = remaining == 0 or (failures and not return_exceptions)
        if should_wake and not wake.done():
            wake.set_result(None)

    for child in children:
        child.add_done_callback(on_child_done)
    try:
        await wake
    except Cancelled:
        await _send_off(children, settled, loop, raised=None)
        raise
    if failures and not return_exceptions:
        first = failures[0]
        await _send_off(children, settled, loop, raised=first)
        first.result()  # raises the child's error (or Cancelled)
    return [_outcome(child) for child in children]


def _as_children[T](aws: Sequence[Awaitable[T]], loop: EventLoop) -> list[Future[T]]:
    """Turn each awaitable into a future on ``loop``, wrapping non-futures in tasks.

    Raises:
        ValueError: If a future belongs to a different loop.

    """
    for aw in aws:
        if isinstance(aw, Future) and aw.get_loop() is not loop:
            message = "gather() was given a Future that belongs to a different loop"
            raise ValueError(message)
    return [aw if isinstance(aw, Future) else Task(_await(aw), loop) for aw in aws]


async def _await[T](aw: Awaitable[T]) -> T:
    """Await ``aw`` and return its result (lets any awaitable become a task)."""
    return await aw


async def _send_off[T](
    children: list[Future[T]],
    settled: Future[None],
    loop: EventLoop,
    *,
    raised: Future[T] | None,
) -> None:
    """Cancel unfinished children, wait for all to finish, report lost errors.

    ``raised`` is the child whose error the gather is about to raise; any
    other child's error would otherwise be lost, so it is reported to the
    loop's exception handler instead.
    """
    for child in children:
        child.cancel()
    if not settled.done():
        await settled
    for child in children:
        if child is raised or child.cancelled():
            continue
        error = child.exception()
        if error is not None:
            loop.report_error(ErrorContext(LOST_ERROR_MESSAGE, error))


def _outcome[T](child: Future[T]) -> T | BaseException:
    """Return the child's result, or the exception it finished with."""
    if child.cancelled():
        try:
            child.result()
        except Cancelled as cancelled:
            return cancelled
    error = child.exception()
    return child.result() if error is None else error
