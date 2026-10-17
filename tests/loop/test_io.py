"""Test file descriptors, the self-pipe doorbell and no-spinning behaviour."""

import selectors
import socket
import threading
import time

from pynet.loop import EventLoop
from pynet.loop.testing import FakeClock

THREAD_DELAY = 0.05
THREAD_WAKE_LIMIT = 2.0
DELAY = 0.2
MAX_IDLE_SELECTS = 3
MAX_ITERATIONS = 5
HOUR = 3600.0
BURST = 50
PAYLOAD = b"x"


class SpySelector(selectors.DefaultSelector):
    """Record every timeout passed to ``select``."""

    def __init__(self) -> None:
        """Start with no recorded calls."""
        super().__init__()
        self.timeouts: list[float | None] = []

    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        """Record ``timeout`` then delegate to the real selector."""
        self.timeouts.append(timeout)
        return super().select(timeout)


class TestFileDescriptors:
    """Test readers and writers."""

    def test_reader_fires_when_data_arrives(self) -> None:
        """Run the reader callback once data is readable."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[bytes] = []
        loop.add_reader(a.fileno(), lambda: seen.append(a.recv(1)))
        b.send(PAYLOAD)

        loop.run_once()

        assert seen == [PAYLOAD]
        loop.remove_reader(a.fileno())
        a.close()
        b.close()
        loop.close()

    def test_remove_reader_stops_callbacks(self) -> None:
        """Return True once, then False, and stop firing."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[int] = []
        loop.add_reader(a.fileno(), lambda: seen.append(1))
        b.send(PAYLOAD)

        assert loop.remove_reader(a.fileno()) is True
        assert loop.remove_reader(a.fileno()) is False
        loop.call_soon(lambda: None)
        loop.run_once()

        assert seen == []
        a.close()
        b.close()
        loop.close()

    def test_second_reader_replaces_first(self) -> None:
        """Keep only the most recently added reader."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[str] = []
        loop.add_reader(a.fileno(), lambda: seen.append("old"))
        loop.add_reader(a.fileno(), lambda: seen.append("new"))
        b.send(PAYLOAD)

        loop.run_once()

        assert seen == ["new"]
        loop.remove_reader(a.fileno())
        a.close()
        b.close()
        loop.close()

    def test_writer_fires_when_writable(self) -> None:
        """Run the writer callback on a writable socket and remove it."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[int] = []
        loop.add_writer(a.fileno(), lambda: seen.append(1))

        loop.run_once()

        assert seen == [1]
        assert loop.remove_writer(a.fileno()) is True
        assert loop.remove_writer(a.fileno()) is False
        a.close()
        b.close()
        loop.close()

    def test_reader_and_writer_on_same_fd(self) -> None:
        """Keep both watchers; removing one leaves the other."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[str] = []
        loop.add_reader(a.fileno(), lambda: seen.append("r"))
        loop.add_writer(a.fileno(), lambda: seen.append("w"))
        b.send(PAYLOAD)

        loop.run_once()
        assert sorted(seen) == ["r", "w"]

        assert loop.remove_writer(a.fileno()) is True
        seen.clear()
        loop.run_once()
        assert seen == ["r"]
        assert loop.remove_reader(a.fileno()) is True
        a.close()
        b.close()
        loop.close()

    def test_removed_during_turn_does_not_fire(self) -> None:
        """Skip a reader removed by an earlier callback in the same turn."""
        loop = EventLoop()
        a, b = socket.socketpair()
        seen: list[int] = []
        loop.add_reader(a.fileno(), lambda: seen.append(1))
        b.send(PAYLOAD)
        loop.call_soon(lambda: loop.remove_reader(a.fileno()))

        loop.run_once()
        assert seen == []
        a.close()
        b.close()
        loop.close()

    def test_virtual_clock_dispatches_io_and_suppresses_autojump(self) -> None:
        """Dispatch I/O on the virtual path and skip the clock jump that turn."""
        clock = FakeClock(autojump=True)
        loop = EventLoop(clock=clock)
        a, b = socket.socketpair()
        seen: list[int] = []
        loop.add_reader(a.fileno(), lambda: seen.append(1))
        loop.call_later(HOUR, lambda: None)
        b.send(PAYLOAD)

        loop.run_once()

        assert seen == [1]
        assert clock() == 0.0
        loop.remove_reader(a.fileno())
        a.close()
        b.close()
        loop.close()

    def test_remove_on_closed_loop_returns_false(self) -> None:
        """Report False rather than raise after close."""
        loop = EventLoop()
        loop.close()

        assert loop.remove_reader(0) is False


class TestSelfPipe:
    """Test cross-thread wakeups and idle behaviour."""

    def test_call_soon_threadsafe_wakes_blocked_loop(self) -> None:
        """Wake a loop blocked in select from another thread."""
        loop = EventLoop()

        def poke() -> None:
            """Sleep briefly then ask the loop to stop."""
            time.sleep(THREAD_DELAY)
            loop.call_soon_threadsafe(loop.stop)

        thread = threading.Thread(target=poke)
        began = time.monotonic()
        thread.start()
        loop.run_forever()
        thread.join()

        assert time.monotonic() - began < THREAD_WAKE_LIMIT
        loop.close()

    def test_many_threadsafe_calls_all_run(self) -> None:
        """Run every threadsafe callback even when the pipe fills up."""
        loop = EventLoop()
        count: list[int] = []
        for _ in range(BURST):
            loop.call_soon_threadsafe(lambda: count.append(1))

        loop.run_once()

        assert len(count) == BURST
        loop.close()

    def test_wakeup_fd_is_not_work(self) -> None:
        """Report no timeout when only the internal wakeup fd exists."""
        loop = EventLoop()

        assert loop.next_timeout() is None
        loop.close()

    def test_idle_loop_blocks_in_select(self) -> None:
        """Sleep in select instead of spinning while a timer is pending."""
        spy = SpySelector()
        loop = EventLoop(selector=spy)
        loop.call_later(DELAY, loop.stop)

        loop.run_forever()

        assert len(spy.timeouts) <= MAX_IDLE_SELECTS
        loop.close()

    def test_self_rescheduling_callback_does_not_starve_io(self) -> None:
        """Fire I/O and timers even while a callback keeps rescheduling."""
        loop = EventLoop()
        a, b = socket.socketpair()
        fired: list[str] = []

        def again() -> None:
            """Reschedule itself forever."""
            loop.call_soon(again)

        loop.call_soon(again)
        loop.add_reader(a.fileno(), lambda: fired.append("io"))
        loop.call_later(0, lambda: fired.append("timer"))
        b.send(PAYLOAD)

        for _ in range(MAX_ITERATIONS):
            loop.run_once()

        assert "io" in fired
        assert "timer" in fired
        loop.remove_reader(a.fileno())
        a.close()
        b.close()
        loop.close()
