"""Baby monitors -- hear the moment a child process finishes, without checking on it.

When a child process exits, the kernel keeps a tiny record of how it
ended (a "zombie") until its parent collects it with ``waitpid``. The
kernel also sends the parent a ``SIGCHLD`` signal: that is the baby
monitor crackling. It does not say *which* child -- only "one of them
needs you".

The ``ChildWatcher`` listens to that monitor for every loop in the
process:

1. The bell. ``signal.set_wakeup_fd`` makes Python's own C-level signal
   handler write a byte into our socketpair the instant *any* signal
   arrives, on whichever thread the kernel picked. (A Python-level
   handler alone is not enough: it only runs between bytecodes on the
   main thread, so a signal landing just as the main loop goes to sleep
   in ``select`` would not be heard until something else woke it.) Our
   Python ``SIGCHLD`` handler only rings the bell again as a backup and
   chains to the previous handler: it can interrupt *any* main-thread
   code, even code holding a lock, so it must not take locks or touch a
   loop.
2. The night nurse. A small daemon thread sleeps on the bell. Each time
   it rings, it asks every child we are tracking ``waitpid(pid,
   WNOHANG)`` -- "has *this* one finished?" (never ``waitpid(-1)``, which
   would steal other code's children) -- under a ``threading.Lock``.
3. The hand-over. Each exit code goes to the child's own loop with
   ``call_soon_threadsafe``, and that loop calls ``on_exit``.

Two safety nets catch the rare misses: right after ``register`` we check
the child once (it might have exited before we started tracking it),
and while a loop has live children it checks them itself every
``CHILD_POLL_INTERVAL`` seconds, stopping when none remain.
"""

import contextlib
import functools
import os
import signal
import socket
import threading
import weakref
from collections.abc import Callable
from types import FrameType

from pynet.loop.reactor import ErrorContext, EventLoop, TimerHandle

CHILD_POLL_INTERVAL = 1.0
UNKNOWN_EXIT_CODE = 255
BELL_BYTE = b"\x00"  # not a signal number, so a previous wakeup-fd owner skips it
BELL_DRAIN = 4096
NO_FD = -1
NURSE_NAME = "pynet-child-watcher"
NOT_INSTALLED_MESSAGE = (
    "the child watcher is not installed: call get_child_watcher().install() "
    "from the main thread (pynet.loop.run() does this when it runs there)"
)
REAPED_CALLBACK_ERROR_MESSAGE = "Exception in a child's on_reaped callback"
MAIN_THREAD_MESSAGE = "the child watcher can only be installed from the main thread"

type ExitCallback = Callable[[int], None]
type _Entry = tuple[EventLoop, ExitCallback, ExitCallback | None]
type _SignalHandler = Callable[[int, FrameType | None], object] | int | signal.Handlers | None


class ChildWatcher:
    """Notice child processes exiting and tell each child's loop.

    Use the process-wide instance from ``get_child_watcher()``.
    """

    def __init__(self) -> None:
        """Start uninstalled, tracking nobody."""
        self._lock = threading.Lock()
        self._children: dict[int, _Entry] = {}
        self._polls: dict[EventLoop, TimerHandle] = {}
        self._installed = False
        self._previous: _SignalHandler = None
        self._bell: socket.socket | None = None
        self._nurse: threading.Thread | None = None
        self._previous_wakeup_fd = NO_FD
        self._busy_hooked: weakref.WeakSet[EventLoop] = weakref.WeakSet()

    def install(self) -> None:
        """Start listening for ``SIGCHLD``; installing twice does nothing.

        Raises:
            RuntimeError: If called from a thread other than the main thread
                (only the main thread may set signal handlers).

        """
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError(MAIN_THREAD_MESSAGE)
        if self._installed:
            return
        bell_read, bell_write = socket.socketpair()
        bell_write.setblocking(False)
        self._bell = bell_write
        self._previous_wakeup_fd = signal.set_wakeup_fd(
            bell_write.fileno(), warn_on_full_buffer=False
        )
        nurse = threading.Thread(target=self._night_nurse, args=(bell_read,), name=NURSE_NAME)
        nurse.daemon = True
        # Start the nurse with SIGCHLD blocked: it never needs the signal itself.
        old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGCHLD})
        try:
            nurse.start()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
        self._nurse = nurse
        self._previous = signal.signal(signal.SIGCHLD, self._on_sigchld)
        self._installed = True
        self._ring()  # catch any child that exited before we were listening

    def is_installed(self) -> bool:
        """Return whether the watcher is listening for ``SIGCHLD``."""
        return self._installed

    def uninstall(self) -> None:
        """Stop listening; put back the previous ``SIGCHLD`` handler and wakeup fd.

        Children still tracked stay tracked: their loops' fallback polls
        keep checking them.
        """
        if not self._installed:
            return
        previous = self._previous if self._previous is not None else signal.SIG_DFL
        signal.signal(signal.SIGCHLD, previous)
        signal.set_wakeup_fd(self._previous_wakeup_fd)
        self._installed = False
        self._previous = None
        bell, self._bell = self._bell, None
        if bell is not None:
            bell.close()  # the nurse sees EOF and goes home
        nurse, self._nurse = self._nurse, None
        if nurse is not None:
            nurse.join()
        self._previous_wakeup_fd = NO_FD

    def register(
        self,
        pid: int,
        loop: EventLoop,
        on_exit: ExitCallback,
        *,
        on_reaped: ExitCallback | None = None,
    ) -> None:
        """Track ``pid``; call ``on_exit(code)`` on ``loop`` once it has exited.

        ``code`` is the exit status, or minus the signal number if a
        signal killed it. Call this from ``loop``'s own thread.

        ``on_reaped(code)``, if given, is called the instant the child is
        reaped -- on whichever thread reaped it, with the watcher's lock
        held, and even if ``loop`` closes before ``on_exit`` can run. Keep
        it tiny (record the code; never block or touch a loop).

        Raises:
            RuntimeError: If the watcher is not installed.

        """
        if not self._installed:
            raise RuntimeError(NOT_INSTALLED_MESSAGE)
        if loop not in self._busy_hooked:
            # A loop with a live child is busy: an autojump clock waits for it.
            self._busy_hooked.add(loop)
            loop.add_busy_check(functools.partial(self.has_children, loop))
        with self._lock:
            self._children[pid] = (loop, on_exit, on_reaped)
            code = self._check(pid)  # it may have exited before we were tracking it
            if code is not None:
                self._record_reap(pid, code)
        if code is None:
            self._ensure_poll(loop)
        else:
            loop.call_soon(functools.partial(self._deliver, loop, on_exit, code))

    def unregister(self, pid: int) -> None:
        """Stop tracking ``pid`` without reaping it; call from its loop's thread."""
        with self._lock:
            entry = self._children.pop(pid, None)
        if entry is not None:
            self._stop_poll_if_idle(entry[0])

    def send_signal(self, pid: int, sig: int, *, group: bool = False) -> bool:
        """Send ``sig`` to ``pid`` (or its process group) if we still track it.

        The check and the send happen under the watcher's lock, so the
        child cannot be reaped -- and its pid handed to a stranger --
        in between. Return whether the signal was sent.
        """
        with self._lock:
            if pid not in self._children:
                return False
            with contextlib.suppress(ProcessLookupError):
                if group:
                    os.killpg(pid, sig)
                else:
                    os.kill(pid, sig)
            return True

    def has_children(self, loop: EventLoop) -> bool:
        """Return whether any child of ``loop`` is still waiting to be reaped."""
        with self._lock:
            return self._owns_children(loop)

    def _tracks(self, pid: int) -> bool:
        """Return whether ``pid`` is still waiting to be reaped (for tests)."""
        with self._lock:
            return pid in self._children

    def _has_poll(self, loop: EventLoop) -> bool:
        """Return whether ``loop`` has a fallback poll scheduled (for tests)."""
        with self._lock:
            return loop in self._polls

    def _on_sigchld(self, signum: int, frame: FrameType | None) -> None:
        """Ring the bell as a backup, then chain to any previous Python handler.

        The wakeup fd has normally rung already; ringing again costs one
        cheap sweep and still works if other code has since taken the
        wakeup fd. This can interrupt any main-thread code -- even code
        holding a lock -- so it takes no locks and touches no loop.
        """
        self._ring()
        previous = self._previous
        if callable(previous):
            previous(signum, frame)

    def _ring(self) -> None:
        """Wake the night nurse by hand; a full socket means it is already awake."""
        bell = self._bell
        if bell is not None:
            with contextlib.suppress(OSError):
                bell.send(BELL_BYTE)

    def _night_nurse(self, bell: socket.socket) -> None:
        """Sweep the tracked children every time the bell rings, until it closes."""
        with bell:
            while True:
                try:
                    rung = bell.recv(BELL_DRAIN)
                except OSError:
                    return
                if not rung:
                    return
                self._pass_on(rung)
                self._sweep_and_hand_over()

    def _pass_on(self, rung: bytes) -> None:
        """Forward the signal bytes to whoever had the wakeup fd before us."""
        signals = rung.replace(BELL_BYTE, b"")
        if signals and self._previous_wakeup_fd != NO_FD:
            with contextlib.suppress(OSError):
                os.write(self._previous_wakeup_fd, signals)

    def _sweep_and_hand_over(self) -> None:
        """Reap every tracked child that has exited and tell its loop."""
        for loop, on_exit, code in self._sweep():
            try:
                loop.call_soon_threadsafe(functools.partial(self._deliver, loop, on_exit, code))
            except RuntimeError:
                # The loop has closed: nobody can be told on it. The child
                # is reaped (no zombie) and ``on_reaped`` already has the code.
                with self._lock:
                    if not self._owns_children(loop):
                        self._polls.pop(loop, None)

    def _sweep(self, only: EventLoop | None = None) -> list[tuple[EventLoop, ExitCallback, int]]:
        """Reap exited children (of ``only``, or of every loop); return them."""
        exited: list[tuple[EventLoop, ExitCallback, int]] = []
        with self._lock:
            for pid, (loop, on_exit, _) in list(self._children.items()):
                if only is not None and loop is not only:
                    continue
                code = self._check(pid)
                if code is not None:
                    self._record_reap(pid, code)
                    exited.append((loop, on_exit, code))
        return exited

    def _record_reap(self, pid: int, code: int) -> None:
        """Forget reaped ``pid`` and call its ``on_reaped`` (caller holds ``_lock``)."""
        loop, _, on_reaped = self._children.pop(pid)
        if on_reaped is None:
            return
        try:
            on_reaped(code)
        except Exception as error:  # noqa: BLE001 -- reported, not swallowed
            loop.report_error(ErrorContext(REAPED_CALLBACK_ERROR_MESSAGE, error))

    @staticmethod
    def _check(pid: int) -> int | None:
        """Return ``pid``'s exit code if it has exited (reaping it), else None.

        Callers hold ``_lock``, so two threads never reap the same pid. If
        something else already reaped it (``ECHILD``), the real code is
        lost and ``UNKNOWN_EXIT_CODE`` stands in.
        """
        try:
            reaped, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return UNKNOWN_EXIT_CODE
        if reaped == 0:
            return None
        return os.waitstatus_to_exitcode(status)

    def _deliver(self, loop: EventLoop, on_exit: ExitCallback, code: int) -> None:
        """Run on ``loop``: stop polling if it was the last child, then report."""
        self._stop_poll_if_idle(loop)
        on_exit(code)

    def _ensure_poll(self, loop: EventLoop) -> None:
        """Schedule ``loop``'s fallback poll unless one is already waiting."""
        with self._lock:
            if loop in self._polls:
                return
            self._polls[loop] = loop.call_later(
                CHILD_POLL_INTERVAL, functools.partial(self._poll, loop)
            )

    def _poll(self, loop: EventLoop) -> None:
        """Run on ``loop``: check its children, then poll again if any remain."""
        with self._lock:
            self._polls.pop(loop, None)
        for _, on_exit, code in self._sweep(loop):
            loop.call_soon(functools.partial(self._deliver, loop, on_exit, code))
        with self._lock:
            still_busy = self._owns_children(loop)
        if still_busy:
            self._ensure_poll(loop)

    def _stop_poll_if_idle(self, loop: EventLoop) -> None:
        """Cancel ``loop``'s fallback poll once it has no children left."""
        with self._lock:
            if self._owns_children(loop):
                return
            handle = self._polls.pop(loop, None)
        if handle is not None:
            handle.cancel()

    def _owns_children(self, loop: EventLoop) -> bool:
        """Return whether any tracked child belongs to ``loop`` (caller holds ``_lock``)."""
        return any(owner is loop for owner, _, _ in self._children.values())


_watcher = ChildWatcher()


def get_child_watcher() -> ChildWatcher:
    """Return the process-wide child watcher (there is only one ``SIGCHLD``)."""
    return _watcher
