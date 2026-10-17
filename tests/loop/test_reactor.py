"""Test the reactor: callbacks, timers, fake clock and error reporting."""

import logging
import selectors
import time

import pytest

from pynet.loop import ErrorContext, EventLoop
from pynet.loop.reactor import AUTOJUMP_GRACE
from pynet.loop.testing import FakeClock

DELAY = 5.0
SHORT = 1.0
TWO = 2.0
THREE = 3.0
EXPECTED_ZERO = 0.0
START_TIME = 100.0
LONG = 1000.0
FAST = 1.0
JUMP_START = 5.0
TICK_REAL = 0.01


class _RecordingSelector(selectors.DefaultSelector):
    """Remember every timeout ``select`` is called with."""

    def __init__(self) -> None:
        """Start with no recorded timeouts."""
        super().__init__()
        self.timeouts: list[float | None] = []

    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        """Record ``timeout``, then select as usual."""
        self.timeouts.append(timeout)
        return super().select(timeout)


class TestCallSoon:
    """Test immediate callbacks."""

    def test_call_soon_runs_in_fifo_order(self) -> None:
        """Run callbacks in the order they were scheduled."""
        loop = EventLoop()
        order: list[str] = []
        for name in ("a", "b", "c"):
            loop.call_soon(lambda n=name: order.append(n))

        loop.run_once()

        assert order == ["a", "b", "c"]

    def test_callbacks_added_during_run_wait_for_next_turn(self) -> None:
        """Run only the callbacks that were ready at the start of the turn."""
        loop = EventLoop()
        order: list[str] = []

        def first() -> None:
            """Schedule a follow-up callback."""
            order.append("first")
            loop.call_soon(lambda: order.append("second"))

        loop.call_soon(first)
        loop.run_once()
        assert order == ["first"]
        loop.run_once()
        assert order == ["first", "second"]

    def test_cancelled_callback_never_runs(self) -> None:
        """Skip a cancelled handle."""
        loop = EventLoop()
        fired: list[str] = []
        handle = loop.call_soon(lambda: fired.append("x"))
        handle.cancel()

        loop.run_once()

        assert handle.cancelled()
        assert fired == []


class TestTimers:
    """Test kitchen timers."""

    def test_call_later_does_not_fire_early(self) -> None:
        """Fire only once the delay has fully elapsed."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        loop.call_later(DELAY, lambda: fired.append("ding"))

        clock.advance(DELAY - SHORT)
        loop.run_once()
        assert fired == []
        clock.advance(SHORT)
        loop.run_once()

        assert fired == ["ding"]

    def test_timers_fire_in_deadline_order(self) -> None:
        """Fire timers by deadline regardless of scheduling order."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        fired: list[float] = []
        for delay in (THREE, SHORT, TWO):
            loop.call_later(delay, lambda d=delay: fired.append(d))

        clock.advance(THREE)
        loop.run_once()

        assert fired == [SHORT, TWO, THREE]

    def test_equal_deadlines_fire_in_schedule_order(self) -> None:
        """Break ties by scheduling sequence."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        loop.call_at(SHORT, lambda: fired.append("a"))
        loop.call_at(SHORT, lambda: fired.append("b"))

        clock.advance(SHORT)
        loop.run_once()

        assert fired == ["a", "b"]

    def test_cancelled_timer_never_fires(self) -> None:
        """Skip a cancelled timer."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        handle = loop.call_later(SHORT, lambda: fired.append("x"))
        handle.cancel()

        clock.advance(DELAY)
        loop.run_once()

        assert fired == []

    def test_call_at_uses_absolute_time(self) -> None:
        """Expose the deadline as ``when`` and use the loop clock."""
        clock = FakeClock(START_TIME)
        loop = EventLoop(clock=clock)

        handle = loop.call_later(DELAY, lambda: None)

        assert handle.when == START_TIME + DELAY
        assert loop.time() == START_TIME

    def test_run_once_does_not_sleep_when_timer_due(self) -> None:
        """Return immediately with a fake clock when a timer is already due."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        loop.call_later(DELAY, lambda: fired.append("x"))
        clock.advance(DELAY)

        loop.run_once()

        assert fired == ["x"]


class TestNextTimeout:
    """Test the no-spinning timeout rules."""

    def test_next_timeout_none_when_idle(self) -> None:
        """Wait forever when nothing is scheduled."""
        assert EventLoop().next_timeout() is None

    def test_next_timeout_zero_when_ready(self) -> None:
        """Do not wait when callbacks are ready."""
        loop = EventLoop()
        loop.call_soon(lambda: None)

        assert loop.next_timeout() == EXPECTED_ZERO

    def test_next_timeout_counts_down_to_timer(self) -> None:
        """Wait exactly until the earliest timer."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        loop.call_later(DELAY, lambda: None)
        assert loop.next_timeout() == DELAY

        clock.advance(SHORT)

        assert loop.next_timeout() == DELAY - SHORT

    def test_next_timeout_never_negative(self) -> None:
        """Clamp an overdue timer to zero."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        loop.call_later(SHORT, lambda: None)
        clock.advance(DELAY)

        assert loop.next_timeout() == EXPECTED_ZERO

    def test_next_timeout_ignores_cancelled_timers(self) -> None:
        """Skip cancelled timers at the head of the heap."""
        clock = FakeClock()
        loop = EventLoop(clock=clock)
        loop.call_later(SHORT, lambda: None).cancel()
        assert loop.next_timeout() is None

        loop.call_later(DELAY, lambda: None)

        assert loop.next_timeout() == DELAY


class TestErrors:
    """Test error reporting."""

    def test_callback_exception_reported_and_loop_continues(self) -> None:
        """Report a failing callback and still run the next one."""
        loop = EventLoop()
        seen: list[ErrorContext] = []
        fired: list[str] = []
        loop.set_exception_handler(seen.append)
        loop.call_soon(lambda: 1 / 0)
        loop.call_soon(lambda: fired.append("next"))

        loop.run_once()

        assert len(seen) == 1
        assert isinstance(seen[0].exception, ZeroDivisionError)
        assert fired == ["next"]

    def test_default_handler_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log reported errors on ``pynet.loop`` at ERROR."""
        loop = EventLoop()
        loop.call_soon(lambda: 1 / 0)

        with caplog.at_level(logging.ERROR, logger="pynet.loop"):
            loop.run_once()

        records = [r for r in caplog.records if r.name == "pynet.loop"]
        assert len(records) == 1
        assert records[0].levelno == logging.ERROR
        assert records[0].exc_info is not None

    def test_clearing_handler_restores_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """Fall back to logging after the handler is set to None."""
        loop = EventLoop()
        loop.set_exception_handler(lambda _ctx: None)
        loop.set_exception_handler(None)

        with caplog.at_level(logging.ERROR, logger="pynet.loop"):
            loop.report_error(ErrorContext("boom"))

        assert [r.getMessage() for r in caplog.records] == ["boom"]

    def test_failing_handler_falls_back_to_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """Log both errors, and keep the loop running, when the handler itself raises."""
        loop = EventLoop()
        fired: list[str] = []

        def broken_handler(_ctx: ErrorContext) -> None:
            """Fail while handling an error."""
            raise KeyError

        loop.set_exception_handler(broken_handler)
        loop.call_soon(lambda: 1 / 0)
        loop.call_soon(lambda: fired.append("next"))

        with caplog.at_level(logging.ERROR, logger="pynet.loop"):
            loop.run_once()

        logged = [r.exc_info[1] for r in caplog.records if r.exc_info is not None]
        assert [type(error) for error in logged] == [ZeroDivisionError, KeyError]
        assert fired == ["next"]


class TestLifecycle:
    """Test run_forever, stop and close."""

    def test_stop_ends_run_forever(self) -> None:
        """Return from run_forever after stop is called."""
        loop = EventLoop()
        states: list[bool] = []

        def finish() -> None:
            """Record running state and stop."""
            states.append(loop.is_running())
            loop.stop()

        loop.call_soon(finish)
        loop.run_forever()

        assert states == [True]
        assert not loop.is_running()

    def test_run_forever_fires_real_timer(self) -> None:
        """Block in select until a real short timer fires."""
        loop = EventLoop()
        loop.call_later(0.01, loop.stop)

        loop.run_forever()

        assert not loop.is_running()

    def test_call_on_closed_loop_raises(self) -> None:
        """Refuse scheduling on a closed loop."""
        loop = EventLoop()
        loop.close()
        loop.close()

        assert loop.is_closed()
        with pytest.raises(RuntimeError):
            loop.call_soon(lambda: None)
        with pytest.raises(RuntimeError):
            loop.call_later(SHORT, lambda: None)
        with pytest.raises(RuntimeError):
            loop.run_forever()


class TestFakeClock:
    """Test the hand-wound clock."""

    def test_advance_moves_time(self) -> None:
        """Start at the given time and move only when advanced."""
        clock = FakeClock(START_TIME)
        assert clock() == START_TIME

        clock.advance(SHORT)

        assert clock() == START_TIME + SHORT


class TestVirtualClock:
    """Test that fake clocks never make the loop really sleep."""

    def test_virtual_clock_never_really_sleeps(self) -> None:
        """Return at once from run_once with a far timer pending."""
        loop = EventLoop(clock=FakeClock())
        fired: list[str] = []
        loop.call_later(LONG, lambda: fired.append("x"))

        began = time.monotonic()
        loop.run_once()

        assert time.monotonic() - began < FAST
        assert fired == []

    def test_autojump_fires_far_timer_fast(self) -> None:
        """Wind the clock forward when idle so far timers fire quickly."""
        clock = FakeClock(autojump=True)
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        loop.call_later(LONG, lambda: fired.append("a"))
        loop.call_later(LONG * 2, loop.stop)

        began = time.monotonic()
        loop.run_forever()

        assert time.monotonic() - began < FAST
        assert fired == ["a"]
        assert clock() >= LONG * 2

    def test_autojump_waits_a_grace_period_before_jumping(self) -> None:
        """Give in-flight I/O a short real grace in ``select`` before jumping."""
        selector = _RecordingSelector()
        loop = EventLoop(clock=FakeClock(autojump=True), selector=selector)
        loop.call_later(LONG, lambda: None)

        loop.run_once()

        assert selector.timeouts == [AUTOJUMP_GRACE]

    def test_autojump_holds_still_while_the_loop_is_busy(self) -> None:
        """Never jump past a timer while a busy check says real work is in flight."""
        clock = FakeClock(autojump=True)
        loop = EventLoop(clock=clock)
        busy = [True]
        fired: list[str] = []
        loop.add_busy_check(lambda: bool(busy))
        loop.call_later(LONG, lambda: fired.append("late"))
        loop.call_later(TICK_REAL, lambda: busy.clear())  # busy ends after a real tick

        loop.run_once()

        assert fired == []
        assert clock() < LONG

    def test_autojump_fires_on_same_turn(self) -> None:
        """Fire the earliest timer in the very run_once that jumps."""
        clock = FakeClock(autojump=True)
        loop = EventLoop(clock=clock)
        fired: list[str] = []
        loop.call_later(LONG, lambda: fired.append("x"))

        loop.run_once()

        assert fired == ["x"]

    def test_jump_to_never_goes_backwards(self) -> None:
        """Ignore jumps into the past."""
        clock = FakeClock(JUMP_START)
        clock.jump_to(SHORT)
        assert clock() == JUMP_START
        clock.jump_to(DELAY * 2)
        assert clock() == DELAY * 2
