"""Test the child watcher and subprocesses running on the event loop."""

import importlib.util
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from pynet.loop import ChildWatcher, gather, get_child_watcher, get_running_loop, run, wait_for
from pynet.loop import children as children_module
from pynet.process import Process, create_process

EXIT_CODE = 3
OK = b"OK\n"
HI = b"hi\n"
OOPS = b"oops\n"
STDIN_BYTE = b"x"
SLEEP_SECONDS = "0.3"
SHORT_SLEEP = "0.1"
BELL_READ = 64
CONCURRENT_JOBS = 3
CONCURRENT_LIMIT = 0.6
SHORT = 2.0
LONG = 5.0
FAST_POLL = 0.02
JOIN_TIMEOUT = 5.0
EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "job_runner.py"


@pytest.fixture(autouse=True)
def watcher() -> Iterator[ChildWatcher]:
    """Install the process-wide child watcher for each test and restore it after."""
    child_watcher = get_child_watcher()
    child_watcher.install()
    yield child_watcher
    child_watcher.install()


def _in_thread(fn: Callable[..., object], *args: object) -> object:
    """Run ``fn(*args)`` in a new thread; return its value or raise its exception."""
    outcome: dict[str, object] = {}

    def body() -> None:
        """Record what ``fn`` returned or raised."""
        try:
            outcome["value"] = fn(*args)
        except BaseException as error:  # noqa: BLE001 -- re-raised in the caller
            outcome["error"] = error

    thread = threading.Thread(target=body)
    thread.start()
    thread.join(JOIN_TIMEOUT)
    assert not thread.is_alive()
    if "error" in outcome:
        error = outcome["error"]
        assert isinstance(error, BaseException)
        raise error
    return outcome.get("value")


async def _output_of(args: list[str]) -> tuple[bytes, int]:
    """Run ``args`` and return its stdout and exit code."""
    process = await create_process(args)
    assert process.stdout is not None
    data = await wait_for(process.stdout.read(), LONG)
    code = await wait_for(process.wait(), LONG)
    process.close()
    return data, code


class TestCreateProcess:
    """Test launching commands and collecting their output and exit codes."""

    def test_captures_stdout(self) -> None:
        """Read what ``echo`` printed and see it exit with 0."""
        assert run(_output_of(["echo", "hi"])) == (HI, 0)

    def test_exit_code(self) -> None:
        """Report the code the command exited with."""

        async def main() -> int:
            """Run a shell that exits with ``EXIT_CODE``."""
            process = await create_process(["sh", "-c", f"exit {EXIT_CODE}"])
            code = await wait_for(process.wait(), LONG)
            assert process.returncode == EXIT_CODE
            process.close()
            return code

        assert run(main()) == EXIT_CODE

    def test_stderr_separate(self) -> None:
        """Keep stderr apart from stdout."""

        async def main() -> list[bytes | BaseException]:
            """Write one line to each stream."""
            process = await create_process(["sh", "-c", "echo hi; echo oops >&2"])
            assert process.stdout is not None
            assert process.stderr is not None
            output = await wait_for(gather(process.stdout.read(), process.stderr.read()), LONG)
            await wait_for(process.wait(), LONG)
            return output

        assert run(main()) == [HI, OOPS]

    def test_stdin_pipe(self) -> None:
        """Feed bytes to ``cat`` and read them back."""

        async def main() -> bytes:
            """Write one byte, close stdin, read the echo."""
            process = await create_process(["cat"], stdin=True)
            assert process.stdin is not None
            assert process.stdout is not None
            process.stdin.write(STDIN_BYTE)
            await process.stdin.drain()
            process.stdin.close()
            data = await wait_for(process.stdout.read(), LONG)
            await wait_for(process.wait(), LONG)
            process.close()
            return data

        assert run(main()) == STDIN_BYTE

    def test_no_pipes_when_not_asked(self) -> None:
        """Leave stdin, stdout and stderr as None when no pipe was requested."""

        async def main() -> Process:
            """Start ``true`` without any pipes."""
            process = await create_process(["true"], stdout=False, stderr=False)
            await wait_for(process.wait(), LONG)
            return process

        process = run(main())
        assert process.stdin is None
        assert process.stdout is None
        assert process.stderr is None
        assert process.returncode == 0

    def test_cwd_and_env(self) -> None:
        """Pass the working directory and environment to the child."""

        async def main() -> bytes:
            """Print the cwd and one environment variable."""
            process = await create_process(
                ["sh", "-c", 'pwd; echo "$PYNET_TEST"'], cwd="/", env={"PYNET_TEST": "yes"}
            )
            assert process.stdout is not None
            data = await wait_for(process.stdout.read(), LONG)
            await wait_for(process.wait(), LONG)
            process.close()
            return data

        assert run(main()) == b"/\nyes\n"

    def test_missing_program_raises(self) -> None:
        """Raise ``FileNotFoundError`` for a program that does not exist."""

        async def main() -> None:
            """Try to start a command that is not on the PATH."""
            await create_process(["pynet-no-such-program"])

        with pytest.raises(FileNotFoundError):
            run(main())

    def test_wait_twice_and_after_exit(self) -> None:
        """Return the same code from every ``wait()``, even after the exit."""

        async def main() -> list[int | BaseException]:
            """Wait from two tasks at once, then once more."""
            process = await create_process(["sh", "-c", f"exit {EXIT_CODE}"])
            codes = await wait_for(gather(process.wait(), process.wait()), LONG)
            codes.append(await process.wait())
            process.close()
            return codes

        assert run(main()) == [EXIT_CODE, EXIT_CODE, EXIT_CODE]

    def test_cancelled_wait_does_not_break_later_wait(self) -> None:
        """Let a timed-out ``wait()`` go without spoiling the next one."""

        async def main() -> int:
            """Time out one wait, then wait properly."""
            process = await create_process(["sleep", SLEEP_SECONDS])
            with pytest.raises(TimeoutError):
                await wait_for(process.wait(), FAST_POLL)
            code = await wait_for(process.wait(), LONG)
            process.close()
            return code

        assert run(main()) == 0

    def test_processes_run_concurrently(self) -> None:
        """Run three 0.3s sleeps at once in well under 0.6s."""

        async def main() -> list[int | BaseException]:
            """Start three sleeps and wait for all of them."""
            processes = [
                await create_process(["sleep", SLEEP_SECONDS]) for _ in range(CONCURRENT_JOBS)
            ]
            codes = await wait_for(gather(*(process.wait() for process in processes)), LONG)
            for process in processes:
                process.close()
            return codes

        start = time.monotonic()
        assert run(main()) == [0] * CONCURRENT_JOBS
        assert time.monotonic() - start < CONCURRENT_LIMIT


class TestSignals:
    """Test stopping processes and whole process groups."""

    def test_kill_gives_negative_code(self) -> None:
        """Report a process killed by SIGKILL as ``-SIGKILL``."""

        async def main() -> int:
            """Start a long sleep and kill it."""
            process = await create_process(["sleep", "30"])
            process.kill()
            code = await wait_for(process.wait(), LONG)
            process.close()
            return code

        assert run(main()) == -signal.SIGKILL

    def test_terminate_gives_sigterm(self) -> None:
        """Report a terminated process as ``-SIGTERM``."""

        async def main() -> int:
            """Start a long sleep and terminate it."""
            process = await create_process(["sleep", "30"])
            process.terminate()
            code = await wait_for(process.wait(), LONG)
            process.close()
            return code

        assert run(main()) == -signal.SIGTERM

    def test_signals_after_exit_are_no_ops(self) -> None:
        """Ignore signals sent to a process that has already been reaped."""

        async def main() -> int:
            """Let ``true`` finish, then try to signal it."""
            process = await create_process(["true"])
            code = await wait_for(process.wait(), LONG)
            process.send_signal(signal.SIGTERM)
            process.kill()
            process.kill_group()
            process.close()
            return code

        assert run(main()) == 0

    def test_kill_group_releases_grandchild_pipe(self) -> None:
        """Kill the shell *and* its background sleep, so stdout reaches EOF."""

        async def main() -> tuple[bytes, bytes, int]:
            """Read the greeting, kill the group, then expect EOF and an exit."""
            process = await create_process(["sh", "-c", "sleep 30 & echo hi; wait"])
            assert process.stdout is not None
            greeting = await wait_for(process.stdout.read_until(), LONG)
            process.kill_group()
            rest = await wait_for(process.stdout.read(), SHORT)
            code = await wait_for(process.wait(), SHORT)
            process.close()
            return greeting, rest, code

        assert run(main()) == (HI, b"", -signal.SIGKILL)

    def test_kill_group_needs_new_session(self) -> None:
        """Refuse to kill a group the child does not lead."""

        async def main() -> None:
            """Start a child in our own session and try to kill its group."""
            process = await create_process(["sleep", "30"], new_session=False)
            try:
                with pytest.raises(RuntimeError, match="new_session"):
                    process.kill_group()
            finally:
                process.kill()
                await wait_for(process.wait(), LONG)
                process.close()

        run(main())


class TestChildWatcher:
    """Test the SIGCHLD watcher that notices children exiting."""

    def test_run_installs_watcher_on_main_thread(self, watcher: ChildWatcher) -> None:
        """Install the watcher when ``run()`` starts on the main thread."""
        watcher.uninstall()
        assert not watcher.is_installed()

        async def main() -> bool:
            """Report whether the watcher is installed."""
            return get_child_watcher().is_installed()

        assert run(main())

    def test_install_is_idempotent(self, watcher: ChildWatcher) -> None:
        """Install twice without stacking handlers, then uninstall cleanly."""
        watcher.install()
        watcher.install()
        assert watcher.is_installed()
        watcher.uninstall()
        assert not watcher.is_installed()
        watcher.uninstall()

    def test_uninstall_restores_previous_handler(self, watcher: ChildWatcher) -> None:
        """Put back whatever SIGCHLD handler was there before."""
        watcher.uninstall()
        calls: list[int] = []

        def previous(signum: int, _frame: object) -> None:
            """Record the signal."""
            calls.append(signum)

        old = signal.signal(signal.SIGCHLD, previous)
        try:
            watcher.install()
            assert signal.getsignal(signal.SIGCHLD) is not previous
            os.kill(os.getpid(), signal.SIGCHLD)
            watcher.uninstall()
            assert signal.getsignal(signal.SIGCHLD) is previous
            assert calls == [signal.SIGCHLD]
        finally:
            signal.signal(signal.SIGCHLD, old)

    def test_install_off_main_thread_raises(self, watcher: ChildWatcher) -> None:
        """Refuse to install from a thread other than the main thread."""
        with pytest.raises(RuntimeError, match="main thread"):
            _in_thread(watcher.install)

    def test_spawn_from_background_thread_loop(self) -> None:
        """Spawn from a loop running in another thread (watcher installed in main)."""
        assert _in_thread(run, _output_of(["echo", "OK"])) == (OK, 0)

    def test_spawn_without_watcher_raises(self, watcher: ChildWatcher) -> None:
        """Raise a helpful error when nobody installed the watcher."""
        watcher.uninstall()

        async def main() -> Process:
            """Try to start a process."""
            return await create_process(["true"])

        with pytest.raises(RuntimeError, match="child watcher"):
            _in_thread(run, main())

    def test_register_without_watcher_raises(self, watcher: ChildWatcher) -> None:
        """Refuse to track a pid while the watcher is not installed."""
        watcher.uninstall()

        async def main() -> None:
            """Try to register a pid."""
            watcher.register(os.getpid(), get_running_loop(), lambda _code: None)

        with pytest.raises(RuntimeError, match="child watcher"):
            _in_thread(run, main())

    def test_does_not_reap_foreign_children(self) -> None:
        """Leave children we did not start for their owner to wait on."""

        async def main() -> int:
            """Start one of our own processes alongside a foreign one."""
            foreign = subprocess.Popen(["true"])  # noqa: S607 -- a fixed test command
            ours = await create_process(["true"])
            await wait_for(ours.wait(), LONG)
            ours.close()
            return foreign.wait(LONG)

        assert run(main()) == 0

    def test_fallback_poll_notices_exit_without_signal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Notice an exit by polling when no ``SIGCHLD`` ever arrives."""
        monkeypatch.setattr(children_module, "CHILD_POLL_INTERVAL", FAST_POLL)

        async def main() -> int:
            """Wait for a short sleep while SIGCHLD is ignored by default."""
            process = await create_process(["sleep", SHORT_SLEEP])
            code = await wait_for(process.wait(), LONG)
            process.close()
            return code

        ours = signal.signal(signal.SIGCHLD, signal.SIG_DFL)  # the monitor's batteries are out
        try:
            assert run(main()) == 0
        finally:
            signal.signal(signal.SIGCHLD, ours)

    def test_wakeup_fd_bytes_are_passed_on(self, watcher: ChildWatcher) -> None:
        """Forward signal bytes to whoever had the wakeup fd before us."""
        watcher.uninstall()
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        previous_fd = signal.set_wakeup_fd(write_fd)
        try:
            watcher.install()
            os.kill(os.getpid(), signal.SIGCHLD)
            deadline = time.monotonic() + LONG
            received = b""
            while not received and time.monotonic() < deadline:
                try:
                    received = os.read(read_fd, BELL_READ)
                except BlockingIOError:
                    time.sleep(FAST_POLL)
            assert received == bytes([signal.SIGCHLD])
            watcher.uninstall()
            assert signal.set_wakeup_fd(previous_fd) == write_fd
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_poll_stops_when_no_children_remain(self, watcher: ChildWatcher) -> None:
        """Leave no fallback timer behind once the last child has exited."""

        async def main() -> tuple[bool, bool]:
            """Check the poll while a child runs and after it exits."""
            loop = get_running_loop()
            process = await create_process(["sleep", SHORT_SLEEP])
            polling = watcher._has_poll(loop)
            await wait_for(process.wait(), LONG)
            process.close()
            return polling, watcher._has_poll(loop)

        assert run(main()) == (True, False)

    def test_unregister_stops_tracking(self, watcher: ChildWatcher) -> None:
        """Forget an unregistered pid so its owner can reap it."""

        async def main() -> int:
            """Register a foreign child, then unregister and wait for it ourselves."""
            child = subprocess.Popen(["sleep", SHORT_SLEEP])  # noqa: S603, S607 -- fixed test command
            watcher.register(child.pid, get_running_loop(), lambda _code: None)
            watcher.unregister(child.pid)
            assert not watcher._has_poll(get_running_loop())
            return child.wait(LONG)

        assert run(main()) == 0

    def test_exit_after_loop_closed_is_still_reaped(self) -> None:
        """Reap a child whose loop has closed, leaving no zombie and raising nothing."""

        async def main() -> int:
            """Start a short sleep and leave without waiting."""
            process = await create_process(["sleep", SHORT_SLEEP], stdout=False, stderr=False)
            return process.pid

        pid = run(main())
        deadline = time.monotonic() + LONG
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(FAST_POLL)
        else:
            pytest.fail("the child was never reaped")


class TestExample:
    """Test the job runner example."""

    def test_job_runner_runs(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Run the three example jobs and report their exit codes."""
        spec = importlib.util.spec_from_file_location("job_runner", EXAMPLE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules["job_runner"] = module
        try:
            spec.loader.exec_module(module)
            module.main()
        finally:
            del sys.modules["job_runner"]
        out = capsys.readouterr().out
        assert "[greet] hello" in out
        assert "exit 0" in out
        assert "timed out" in out
