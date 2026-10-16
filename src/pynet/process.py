"""Helpers in the kitchen -- start a job, carry on serving, hear when it is done.

A subprocess is a helper you send off to do a whole job by themselves
(``make``, ``pytest``, ``rsync``...). You hand them a job card (the
command), and they talk back through three pipes: ``stdin`` (you to
them), ``stdout`` and ``stderr`` (them to you). Those pipes are ordinary
streams, so reading a helper's output never freezes the loop.

You do not stand over them asking "finished yet?". The child watcher's
baby monitor (``SIGCHLD``) tells the loop the moment a helper finishes,
and ``await process.wait()`` hands you their exit code: ``0`` for "all
good", another number for "something went wrong", or minus a signal
number (``-9``) if they were stopped with a signal.

Each helper starts in a **new session** by default, so it leads its own
process group. ``kill_group()`` then stops the helper *and* anything it
started -- a shell's background ``sleep`` included -- so no grandchild is
left holding your pipe open.
"""

import contextlib
import os
import signal
import subprocess
from collections.abc import Mapping, Sequence

from pynet.loop.children import get_child_watcher
from pynet.loop.futures import Future
from pynet.loop.reactor import EventLoop
from pynet.loop.tasks import get_running_loop
from pynet.streams import StreamReader, StreamWriter, pipe_reader, pipe_writer

NOT_INSTALLED_MESSAGE = (
    "create_process needs the child watcher: run your code with pynet.loop.run() on the "
    "main thread, or call get_child_watcher().install() from the main thread first"
)
NO_GROUP_MESSAGE = "kill_group() needs a process started with new_session=True"


class Process:
    """A running (or finished) child process with stream pipes.

    Build one with ``create_process``, not directly.

    Attributes:
        pid: The child's process id (and, with ``new_session``, its group id).
        returncode: None while running; then the exit code, or minus the
            signal number that killed it.
        stdin: A writer to the child's stdin, or None if not piped.
        stdout: A reader of the child's stdout, or None if not piped.
        stderr: A reader of the child's stderr, or None if not piped.

    """

    def __init__(
        self,
        popen: subprocess.Popen[bytes],
        loop: EventLoop,
        *,
        stdin: StreamWriter | None,
        stdout: StreamReader | None,
        stderr: StreamReader | None,
        new_session: bool,
    ) -> None:
        """Wrap ``popen`` and ask the child watcher to report its exit on ``loop``."""
        self._popen = popen
        self._loop = loop
        self._new_session = new_session
        self._waiters: list[Future[int]] = []
        self.pid: int = popen.pid
        self.returncode: int | None = None
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        get_child_watcher().register(self.pid, loop, self._on_exit)

    async def wait(self) -> int:
        """Wait for the child to exit and return its exit code.

        Each call waits on its own future, so a ``wait()`` that is
        cancelled (for example by a timeout) does not upset any other.
        """
        if self.returncode is not None:
            return self.returncode
        waiter: Future[int] = Future(self._loop)
        self._waiters.append(waiter)
        try:
            return await waiter
        finally:
            if waiter in self._waiters:
                self._waiters.remove(waiter)

    def send_signal(self, sig: int) -> None:
        """Send ``sig`` to the child; do nothing if it has already been reaped."""
        get_child_watcher().send_signal(self.pid, sig)

    def terminate(self) -> None:
        """Ask the child to stop (``SIGTERM``)."""
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        """Stop the child at once (``SIGKILL``)."""
        self.send_signal(signal.SIGKILL)

    def kill_group(self, sig: int = signal.SIGKILL) -> None:
        """Send ``sig`` to the child's whole process group (``os.killpg``).

        This reaches grandchildren too. Does nothing if the child has
        already been reaped.

        Raises:
            RuntimeError: If the child was started with ``new_session=False``
                (it then shares *our* group, which we must not kill).

        """
        if not self._new_session:
            raise RuntimeError(NO_GROUP_MESSAGE)
        get_child_watcher().send_signal(self.pid, sig, group=True)

    def close(self) -> None:
        """Close every pipe that is still open, throwing away unread output.

        Call it once you are done with the process, so pipes nobody read
        to the end do not leak file descriptors. It does not stop the child.
        """
        if self.stdin is not None:
            self.stdin.abort()
        for reader in (self.stdout, self.stderr):
            if reader is not None:
                reader.close()

    def _on_exit(self, code: int) -> None:
        """Record the exit code and wake every ``wait()``.

        Called by the child watcher once the child has been reaped. We set
        ``Popen.returncode`` too (we never call ``Popen.wait``), so the
        ``Popen`` object knows the child is gone and stays quiet when it is
        garbage-collected.
        """
        self.returncode = code
        self._popen.returncode = code
        waiters, self._waiters = self._waiters, []
        if self._loop.is_closed():
            return
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(code)


def _close_all(fds: list[int]) -> None:
    """Close each of ``fds``, ignoring ones that are already closed."""
    for fd in fds:
        with contextlib.suppress(OSError):
            os.close(fd)


async def create_process(
    args: Sequence[str],
    *,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    stdin: bool = False,
    stdout: bool = True,
    stderr: bool = True,
    new_session: bool = True,
) -> Process:
    """Start ``args`` as a child process and return it straight away.

    Args:
        args: The program and its arguments, e.g. ``["echo", "hi"]``.
        cwd: The directory to run it in (default: ours).
        env: Its whole environment (default: a copy of ours).
        stdin: Give it a pipe we can write to; otherwise it reads ``/dev/null``.
        stdout: Pipe its stdout to us; otherwise it shares ours.
        stderr: Pipe its stderr to us; otherwise it shares ours.
        new_session: Start it in a new session (and process group) so
            ``kill_group`` can stop it and everything it starts.

    Raises:
        RuntimeError: If the child watcher is not installed.
        OSError: If the program cannot be started (e.g. ``FileNotFoundError``).

    """
    loop = get_running_loop()
    if not get_child_watcher().is_installed():
        raise RuntimeError(NOT_INSTALLED_MESSAGE)
    # Our own pipes. os.pipe() fds are not inheritable, so a helper we start
    # at the same moment never grabs another helper's pipe by accident.
    stdin_pair = os.pipe() if stdin else None
    stdout_pair = os.pipe() if stdout else None
    stderr_pair = os.pipe() if stderr else None
    child_ends = [pair[0] for pair in (stdin_pair,) if pair] + [
        pair[1] for pair in (stdout_pair, stderr_pair) if pair
    ]
    our_ends = [pair[1] for pair in (stdin_pair,) if pair] + [
        pair[0] for pair in (stdout_pair, stderr_pair) if pair
    ]
    try:
        popen = subprocess.Popen(  # noqa: S603 -- running the caller's command is the point
            list(args),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            stdin=stdin_pair[0] if stdin_pair else subprocess.DEVNULL,
            stdout=stdout_pair[1] if stdout_pair else None,
            stderr=stderr_pair[1] if stderr_pair else None,
            start_new_session=new_session,
        )
    except BaseException:
        _close_all(child_ends + our_ends)
        raise
    finally:
        _close_all(child_ends)  # the child has its own copies now
    return Process(
        popen,
        loop,
        stdin=pipe_writer(stdin_pair[1]) if stdin_pair else None,
        stdout=pipe_reader(stdout_pair[0]) if stdout_pair else None,
        stderr=pipe_reader(stderr_pair[0]) if stderr_pair else None,
        new_session=new_session,
    )
