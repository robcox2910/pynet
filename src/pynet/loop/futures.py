"""The cloakroom ticket -- a promise that a result will be ready later.

A ``Future`` is an empty box with a ticket on it. Someone promises to
put a result (or an exception) in the box one day. Anyone holding the
ticket can ask "is it ready yet?", or leave a note (a done callback)
saying "tell me when it is". The loop delivers those notes on its next
turn -- never in the middle of whoever filled the box.

Awaiting a future is the trick that makes ``async def`` work on our
loop: ``__await__`` *yields the future itself* up to the task driving
the coroutine, which then waits for the box to be filled.
"""

from collections.abc import Callable, Generator
from typing import Any, Literal

from pynet.errors import PyNetError
from pynet.loop.reactor import ErrorContext, EventLoop

type _State = Literal["pending", "cancelled", "finished"]

PENDING: _State = "pending"
CANCELLED: _State = "cancelled"
FINISHED: _State = "finished"
UNRETRIEVED_MESSAGE = "Future exception was never retrieved"


class Cancelled(BaseException):
    """Signal that a future or task was cancelled.

    It is a ``BaseException`` (like ``KeyboardInterrupt``) so that an
    everyday ``except Exception`` does not accidentally swallow it.
    """


class InvalidStateError(PyNetError):
    """Raise when a future is asked for something it is not ready for."""


class Future[T]:
    """Hold a result that will be filled in later.

    Args:
        loop: The loop that delivers this future's done callbacks.

    """

    _result: T

    def __init__(self, loop: EventLoop) -> None:
        """Create a pending future that belongs to ``loop``."""
        self._loop = loop
        self._state: _State = PENDING
        self._exception: BaseException | None = None
        self._cancel_message = ""
        self._callbacks: list[Callable[[Future[T]], object]] = []
        self._unretrieved = False

    def get_loop(self) -> EventLoop:
        """Return the loop this future belongs to."""
        return self._loop

    def done(self) -> bool:
        """Return whether the future has a result, an exception, or was cancelled."""
        return self._state != PENDING

    def cancelled(self) -> bool:
        """Return whether the future was cancelled."""
        return self._state == CANCELLED

    def result(self) -> T:
        """Return the result, or raise the stored exception.

        Raises:
            InvalidStateError: If the future is still pending.
            Cancelled: If the future was cancelled.

        """
        self._check_finished()
        if self._exception is not None:
            self._unretrieved = False
            raise self._exception
        return self._result

    def exception(self) -> BaseException | None:
        """Return the stored exception, or None if there is a result.

        Raises:
            InvalidStateError: If the future is still pending.
            Cancelled: If the future was cancelled.

        """
        self._check_finished()
        self._unretrieved = False
        return self._exception

    def set_result(self, value: T) -> None:
        """Put ``value`` in the box and schedule the done callbacks.

        Raises:
            InvalidStateError: If the future is already done.

        """
        self._check_pending()
        self._result = value
        self._state = FINISHED
        self._schedule_callbacks()

    def set_exception(self, exception: BaseException) -> None:
        """Put ``exception`` in the box and schedule the done callbacks.

        Raises:
            InvalidStateError: If the future is already done.
            TypeError: If ``exception`` is a ``StopIteration``, which cannot
                travel through a coroutine.

        """
        self._check_pending()
        if isinstance(exception, StopIteration):
            message = "StopIteration cannot be stored in a Future"
            raise TypeError(message)
        self._exception = exception
        self._state = FINISHED
        self._unretrieved = True
        self._schedule_callbacks()

    def cancel(self, msg: str = "") -> bool:
        """Cancel the future; return False if it was already done."""
        if self.done():
            return False
        self._state = CANCELLED
        self._cancel_message = msg
        self._schedule_callbacks()
        return True

    def add_done_callback(self, cb: Callable[[Future[T]], object]) -> None:
        """Call ``cb(self)`` on a later loop turn once the future is done."""
        if self.done():
            self._loop.call_soon(lambda: cb(self))
        else:
            self._callbacks.append(cb)

    def remove_done_callback(self, cb: Callable[[Future[T]], object]) -> int:
        """Remove every registration of ``cb``; return how many were removed."""
        kept = [callback for callback in self._callbacks if callback != cb]
        removed = len(self._callbacks) - len(kept)
        self._callbacks = kept
        return removed

    def __await__(self) -> Generator[Future[Any], None, T]:
        """Hand this future to the driving task, then return its result.

        The ``yield`` passes the future up to the ``Task``; the task resumes
        the coroutine here only once the future is done.
        """
        if not self.done():
            yield self
        return self.result()

    def __del__(self) -> None:
        """Report an exception that nobody ever retrieved."""
        if getattr(self, "_unretrieved", False):
            self._loop.report_error(ErrorContext(UNRETRIEVED_MESSAGE, self._exception))

    def _make_cancelled(self) -> Cancelled:
        """Build the ``Cancelled`` error for this future."""
        return Cancelled(self._cancel_message) if self._cancel_message else Cancelled()

    def _check_finished(self) -> None:
        """Raise unless the future has a result or exception."""
        if self._state == CANCELLED:
            raise self._make_cancelled()
        if self._state == PENDING:
            message = "Result is not ready"
            raise InvalidStateError(message)

    def _check_pending(self) -> None:
        """Raise if the future is already done."""
        if self.done():
            message = f"Future is already {self._state}"
            raise InvalidStateError(message)

    def _schedule_callbacks(self) -> None:
        """Queue every done callback on the loop and forget them."""
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks:
            self._loop.call_soon(lambda cb=cb: cb(self))
