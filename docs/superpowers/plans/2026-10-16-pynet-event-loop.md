# PyNet Event Loop and Real Networking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give PyNet a from-scratch event loop (reactor → tasks → streams → framing → message protocol → real WebSocket), plus subprocesses and thread interop, each with a kid-friendly docs page, without touching the existing simulations.

**Architecture:** A `selectors`-based reactor with a ready queue, timer heap and self-pipe; our own `Future`/`Task` drive plain `async def` coroutines; non-blocking fd streams carry TCP and pipes; a length-prefixed, kind-tagged frame layer carries a versioned JSON envelope with binary attachments; RFC 6455 WebSocket sits on the same streams. A single process-wide child watcher reaps subprocesses for any loop.

**Tech Stack:** Python 3.14 stdlib only (no `asyncio`), uv, pytest (+ pytest-timeout, dev only), ruff, pyright strict.

**Spec:** `docs/superpowers/specs/2026-10-16-pynet-event-loop-design.md`

## Global Constraints

- Python `>=3.14`; **zero runtime dependencies**; never import `asyncio` anywhere in `src/`.
- Never use `from __future__ import annotations` or `TYPE_CHECKING`.
- macOS + Linux only (POSIX signals, process groups).
- Every public module/class/function (and every test class/method) has a Google-style, imperative-mood docstring; module docstrings open with an analogy line in the existing style (`"""Registered mail -- reliable, ordered delivery.`).
- No magic numbers in code or tests (PLR2004): use module-level named constants.
- `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright src tests`, `uv run pytest` (≥80% coverage) all pass at every commit.
- Conventional commits; work on branch `feat/event-loop` (already created, spec committed); PRs into protected `main`.
- Existing simulation modules and their public APIs (`resolve`, `b64_encode`, `b64_decode`, URL parsing, `tcp.py`, `websocket.py`, …) keep their behaviour.
- Errors in callbacks/tasks/connections are reported (loop exception handler → `logging.getLogger("pynet.loop")`), never swallowed.
- An idle loop blocks in `select` (no spinning, no short-sleep polling).
- Every task that adds a module also adds its `docs/concepts/<page>.md` (analogy heading, plain explanation, ASCII diagram, runnable snippet, "When to use", "What we test", "Next up") — analogies from spec §7.
- Each user-visible layer updates PyStack in the same step (Tasks 9, 15, 19), on a matching `feat/pynet-event-loop` branch in `/Users/robcox/development/python/pystack`; the PyStack PR merges after the PyNet PR (PyStack CI clones PyNet `main`).

## Review Focus

1. **Peer drops mid-request** — pending `request()` calls must fail promptly with `ConnectionClosedError`, not hang until timeout. (Test in Task 12.)
2. **Grandchild keeps a job's stdout pipe open** (`sh -c "sleep 30 & echo hi"`) — after `kill_group()`, reading stdout must reach EOF and `wait()` must return. (Test in Task 8.)
3. **A callback that reschedules itself forever with `call_soon`** must not starve I/O or timers. (Test in Task 2.)
4. **A request handler raises** — the caller gets a `RemoteError` with code `handler_error`, and the connection keeps working for the next request. (Test in Task 12.)
5. **A read cancelled by a timeout** must leave no stale selector registration; the next read on the same stream works. (Test in Task 7.)

---

## File Structure

```
src/pynet/
  errors.py              + ConnectionClosedError, FrameTooLargeError, ProtocolError,
                           RemoteError, NotConnectedError, HeartbeatTimeoutError
  base64.py              + b64_decode_bytes
  loop/
    __init__.py          re-exports the public loop API
    reactor.py           EventLoop, Handle, TimerHandle, ErrorContext
    testing.py           FakeClock
    futures.py           Future, Cancelled, InvalidStateError
    tasks.py             Task, get_running_loop, current_task, run, spawn, sleep
    control.py           timeout (Timeout), wait_for, gather
    sync.py              Event, Queue, QueueEmpty, QueueFull, Lock, Semaphore
    threads.py           WorkerPool, run_in_thread
    children.py          ChildWatcher, get_child_watcher
  streams.py             StreamReader, StreamWriter, Server, open_connection, start_server,
                         IncompleteReadError, LimitOverrunError, pipe_reader, pipe_writer
  process.py             Process, create_process
  framing.py             encode_frame, FramedConnection, MAX_FRAME_SIZE
  messaging.py           Message, MessageType, Reply, encode_message, MessageDecoder,
                         Peer, connect, serve, backoff_delay, ReconnectingClient, ConnectionEvent
  ws/
    __init__.py
    handshake.py         compute_accept, generate_key, validate_client_handshake,
                         server_handshake_response, client_handshake_request, HandshakeError
    frames.py            Opcode, Frame, apply_mask, encode_ws_frame, read_ws_frame
    connection.py        WebSocket, WebSocketClosed, close-code constants
    endpoints.py         accept_websocket, serve_websocket, connect_websocket
tests/loop/              test_reactor.py, test_io.py, test_tasks.py, test_control.py,
                         test_sync.py, test_threads.py
tests/                   test_streams.py, test_process.py, test_framing.py,
                         test_messaging_codec.py, test_peer.py, test_heartbeat.py,
                         test_reconnect.py, test_ws_handshake.py, test_ws_frames.py,
                         test_ws_connection.py
examples/                echo_server.py, job_runner.py, chat_room.py, heartbeat_demo.py, ws_echo.py
docs/concepts/           one page per layer (named in each task)
```

---

### Task 0: Housekeeping

**Files:**
- Modify: `src/pynet/{base64,tcp,tls,udp,smtp,ftp,ping,websocket}.py` — remove the `from __future__ import annotations` line (8 hand-written files; `_version.py` is generated and excluded).
- Modify: `pyproject.toml` (dev deps + pytest addopts)
- Create: `tests/loop/__init__.py`

- [ ] **Step 1:** Run `grep -rln "from __future__" src tests` and delete the import from each listed file (not `_version.py`, which is generated and excluded).
- [ ] **Step 2:** Add `"pytest-timeout>=2.3.1"` to `[project.optional-dependencies].dev`, add `"--timeout=10"` to pytest `addopts`, then `uv sync --all-extras`.
- [ ] **Step 3:** Confirm the DNS parametrize list in `tests/test_dns.py:31-40` has commas between `"0.0.0.0"` and `"255.255.255.255"` (the stale `.pytest_cache` failure is from an old run); delete `.pytest_cache`.
- [ ] **Step 4:** Run the full check suite. Expected: all pass, pyright 0 errors.
- [ ] **Step 5:** Commit `chore: drop __future__ imports and add per-test timeout`.

---

### Task 1: Reactor — callbacks, timers, fake clock, error reporting

**Files:**
- Create: `src/pynet/loop/__init__.py`, `src/pynet/loop/reactor.py`, `src/pynet/loop/testing.py`
- Test: `tests/loop/test_reactor.py`
- Docs: `docs/concepts/event-loop.md` (waiter analogy), `docs/concepts/timers.md` (kitchen timers)

**Interfaces:**
- Produces:
  - `type Callback = Callable[[], object]`
  - `class Handle`: `cancel() -> None`, `cancelled() -> bool`
  - `class TimerHandle(Handle)`: `when: float`; ordered by `(when, sequence)` for the heap
  - `@dataclass(frozen=True) class ErrorContext`: `message: str`, `exception: BaseException | None`
  - `class EventLoop(clock: Callable[[], float] = time.monotonic, selector: selectors.BaseSelector | None = None)`:
    `time() -> float`, `call_soon(cb) -> Handle`, `call_later(delay: float, cb) -> TimerHandle`,
    `call_at(when: float, cb) -> TimerHandle`, `next_timeout() -> float | None`, `run_once() -> None`,
    `run_forever() -> None`, `stop() -> None`, `is_running() -> bool`, `close() -> None`, `is_closed() -> bool`,
    `set_exception_handler(h: Callable[[ErrorContext], None] | None) -> None`, `report_error(ctx: ErrorContext) -> None`
  - `class FakeClock(start: float = 0.0)`: `__call__() -> float`, `advance(seconds: float) -> None`

`next_timeout()` rules (the core of "no spinning"): `0.0` if callbacks are ready; `None` (block forever) if no timers and no ready callbacks; otherwise `max(0.0, earliest_when - time())`. `run_once()` selects with that timeout, queues I/O callbacks, moves due timers into the ready queue, then runs **only the callbacks that were ready at the start of the run phase** (snapshot length). Cancelled handles are skipped (lazy deletion from the heap). `stop()` makes `run_forever` return after the current iteration. Calling `call_*` on a closed loop raises `RuntimeError`. Default exception handler logs `ctx.message` with `exc_info=ctx.exception` at ERROR on `pynet.loop`.

- [ ] **Step 1: Write failing tests**

```python
DELAY = 5.0
SHORT = 1.0

def test_call_soon_runs_in_fifo_order() -> None: ...           # ["a","b","c"] after run_once
def test_call_later_does_not_fire_early() -> None:
    clock = FakeClock(); loop = EventLoop(clock=clock); fired: list[str] = []
    loop.call_later(DELAY, lambda: fired.append("ding"))
    clock.advance(DELAY - SHORT); loop.run_once(); assert fired == []
    clock.advance(SHORT); loop.run_once(); assert fired == ["ding"]
def test_timers_fire_in_deadline_order() -> None: ...           # scheduled 3,1,2 → fire 1,2,3
def test_cancelled_timer_never_fires() -> None: ...
def test_next_timeout_none_when_idle() -> None: assert EventLoop().next_timeout() is None
def test_next_timeout_zero_when_ready() -> None: ...
def test_next_timeout_counts_down_to_timer() -> None: ...       # == DELAY, then DELAY-SHORT after advance
def test_callback_exception_reported_and_loop_continues() -> None: ...  # handler gets ErrorContext with ZeroDivisionError; next callback still runs
def test_default_handler_logs(caplog) -> None: ...              # record on "pynet.loop" at ERROR
def test_stop_ends_run_forever() -> None: ...
def test_call_on_closed_loop_raises() -> None: ...
```

- [ ] **Step 2:** `uv run pytest tests/loop/test_reactor.py -v` → FAIL (ImportError).
- [ ] **Step 3:** Implement `reactor.py` and `testing.py` with the interfaces above (heap via `heapq`, ready queue via `collections.deque`).
- [ ] **Step 4:** Re-run → PASS. Write `event-loop.md` and `timers.md`; link `event-loop.md` from `docs/index.md`'s table.
- [ ] **Step 5:** Commit `feat(loop): add reactor with callbacks and timers`.

---

### Task 2: Reactor — file descriptors and the self-pipe

**Files:**
- Modify: `src/pynet/loop/reactor.py`
- Test: `tests/loop/test_io.py`
- Docs: `docs/concepts/self-pipe-trick.md` (doorbell the loop hears while asleep)

**Interfaces:**
- Produces on `EventLoop`: `add_reader(fd: int, cb) -> None`, `remove_reader(fd: int) -> bool`, `add_writer(fd: int, cb) -> None`, `remove_writer(fd: int) -> bool`, `call_soon_threadsafe(cb) -> Handle`.

A fd may have a reader and a writer at once (selector key data holds both; modify mask on add/remove). Adding a second reader for the same fd replaces the first. The loop creates a `socket.socketpair()` at construction, registers the read end internally, and `call_soon_threadsafe` appends under a `threading.Lock` then writes one byte (ignore `BlockingIOError` when the pipe is full — it's already awake). The wakeup reader drains all bytes. `close()` unregisters and closes the pair. Internal fds do not count as "work" for `next_timeout()`.

- [ ] **Step 1: Write failing tests**

```python
def test_reader_fires_when_data_arrives() -> None: ...       # socketpair; write b"x"; run_once; callback ran
def test_remove_reader_stops_callbacks() -> None: ...        # returns True, then False on second remove
def test_writer_fires_when_writable() -> None: ...
def test_reader_and_writer_on_same_fd() -> None: ...
def test_call_soon_threadsafe_wakes_blocked_loop() -> None:
    # loop.run_forever() in main thread; another thread sleeps 0.05 then call_soon_threadsafe(loop.stop)
    # assert run_forever returns within THREAD_WAKE_LIMIT seconds
def test_idle_loop_blocks_in_select() -> None:
    # selector spy subclass of DefaultSelector records each timeout passed to select();
    # schedule call_later(DELAY, stop) on a real clock with DELAY=0.2; run_forever;
    # assert number of select calls <= MAX_IDLE_SELECTS (3)  — proves no spinning
def test_self_rescheduling_callback_does_not_starve_io() -> None:   # Review Focus 3
    # callback re-adds itself via call_soon forever; a reader on a socketpair with data
    # and a call_later(0) timer must both fire within MAX_ITERATIONS run_once() calls
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the fd methods and self-pipe.
- [ ] **Step 4:** Run → PASS. Write `self-pipe-trick.md`.
- [ ] **Step 5:** Commit `feat(loop): watch file descriptors and wake safely from threads`.

---

### Task 3: Futures, tasks, `run`, `sleep`

**Files:**
- Create: `src/pynet/loop/futures.py`, `src/pynet/loop/tasks.py`; update `loop/__init__.py` re-exports
- Test: `tests/loop/test_tasks.py`
- Docs: `docs/concepts/tasks-and-await.md` (bookmarks in recipes)

**Interfaces:**
- Produces:
  - `class Cancelled(BaseException)` — BaseException so `except Exception` doesn't swallow it.
  - `class InvalidStateError(PyNetError)`
  - `class Future[T](loop: EventLoop)`: `done()`, `cancelled()`, `result() -> T`, `exception() -> BaseException | None`, `set_result(v: T)`, `set_exception(e: BaseException)`, `cancel(msg: str = "") -> bool`, `add_done_callback(cb: Callable[[Future[T]], object])`, `remove_done_callback(cb) -> int`, `get_loop() -> EventLoop`, `__await__() -> Generator[Future[Any], None, T]`.
    Done callbacks are scheduled with `loop.call_soon`, never called inline. `result()` on a pending future raises `InvalidStateError`; on a cancelled one raises `Cancelled`. A future whose exception is never retrieved reports `ErrorContext("Future exception was never retrieved", exc)` from `__del__`.
  - `class Task[T](Future[T])(coro: Coroutine[Any, Any, T], loop: EventLoop, name: str | None = None)`: `get_name() -> str`, `cancel(msg="") -> bool`, `cancelling() -> int`, `uncancel() -> int`.
  - `get_running_loop() -> EventLoop` (RuntimeError if none; tracked per-thread while `run_forever` runs), `current_task() -> Task[Any] | None`
  - `run[T](main: Coroutine[Any, Any, T], *, clock: Callable[[], float] | None = None) -> T` — new loop, runs until `main` done, cancels leftover tasks, closes loop, returns result or re-raises.
  - `spawn[T](coro, *, name: str | None = None) -> Task[T]` (on running loop)
  - `async sleep(seconds: float) -> None` (`sleep(0)` yields once)

`Task._step` sends/throws into the coroutine; a yielded `Future` gets a done callback that schedules the next step; yielding anything else throws `TypeError` into the coroutine with message containing `"only pynet Futures can be awaited"` and `"asyncio"`. `cancel()` increments a counter, cancels the awaited future if any, else marks to throw `Cancelled` at the next step.

- [ ] **Step 1: Write failing tests**

```python
ANSWER = 42
def test_run_returns_coroutine_result() -> None: assert run(_answer()) == ANSWER
def test_run_reraises_exception() -> None: ...
def test_two_sleeps_overlap() -> None:
    # real clock, DELAY = 0.1: spawn two sleep(DELAY) tasks and await both; elapsed < 2 * DELAY
def test_done_callbacks_run_later_not_inline() -> None: ...
def test_result_before_done_raises_invalid_state() -> None: ...
def test_awaiting_foreign_awaitable_raises_type_error() -> None:
    class Foreign:
        def __await__(self): yield "not a future"
    # inside run(): with pytest.raises(TypeError, match="only pynet Futures"): await Foreign()
def test_get_running_loop_outside_loop_raises() -> None: ...
def test_current_task_inside_task() -> None: ...
def test_unretrieved_exception_reported() -> None:
    # spawn a task that raises; never await it; let it finish; del; gc.collect(); handler saw ErrorContext
def test_cancel_throws_cancelled_and_runs_finally() -> None: ...
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `futures.py` and `tasks.py`.
- [ ] **Step 4:** Run → PASS. Write `tasks-and-await.md` (show the yield-a-Future trick step by step).
- [ ] **Step 5:** Commit `feat(loop): add futures, tasks and run()`.

---

### Task 4: Timeouts, `wait_for`, `gather`

**Files:**
- Create: `src/pynet/loop/control.py`
- Test: `tests/loop/test_control.py`
- Docs: `docs/concepts/cancellation-and-timeouts.md` (referee's whistle)

**Interfaces:**
- Consumes: `Task.cancel/cancelling/uncancel`, `current_task`, `EventLoop.call_later`.
- Produces:
  - `class Timeout` + `timeout(seconds: float | None) -> Timeout` (async context manager; `None` = no limit). On expiry cancels the current task; in `__aexit__`, if it fired and the exception is `Cancelled` and `task.uncancel() == 0`, raise built-in `TimeoutError` from `None`. `expired() -> bool`.
  - `async wait_for[T](aw: Awaitable[T], seconds: float | None) -> T`
  - `async gather[T](*aws: Awaitable[T], return_exceptions: bool = False) -> list[T | BaseException]` — results in argument order; with `return_exceptions=False` the first exception propagates and the remaining children are cancelled; cancelling the gather cancels all children.

- [ ] **Step 1: Write failing tests**

```python
def test_timeout_raises_timeout_error() -> None: ...                # sleep(LONG) inside timeout(SHORT)
def test_timeout_not_triggered_returns_normally() -> None: ...
def test_finally_runs_on_timeout() -> None: ...
def test_outer_cancel_inside_timeout_stays_cancelled() -> None:
    # task with `async with timeout(LONG): await sleep(LONG)`; cancel it from outside → Cancelled, not TimeoutError
def test_wait_for_returns_value_and_times_out() -> None: ...
def test_gather_preserves_order() -> None: ...                       # different sleep lengths, results in arg order
def test_gather_first_error_cancels_siblings() -> None: ...          # sibling's finally recorded "cancelled"
def test_gather_return_exceptions() -> None: ...
```

- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement `control.py`. **Step 4:** Run → PASS; write the docs page.
- [ ] **Step 5:** Commit `feat(loop): add timeouts, wait_for and gather`.

---

### Task 5: Sync primitives

**Files:**
- Create: `src/pynet/loop/sync.py`
- Test: `tests/loop/test_sync.py`
- Docs: `docs/concepts/sync-primitives.md` (starting pistol, conveyor belt, bathroom key, car park)

**Interfaces:**
- Produces:
  - `Event`: `set()`, `clear()`, `is_set() -> bool`, `async wait() -> None`
  - `QueueEmpty(PyNetError)`, `QueueFull(PyNetError)`
  - `Queue[T](maxsize: int = 0)`: `async put(item: T)`, `async get() -> T`, `put_nowait`, `get_nowait`, `qsize() -> int`, `empty() -> bool`, `full() -> bool` (`maxsize <= 0` = unbounded)
  - `Lock`: `async acquire()`, `release()` (RuntimeError if unlocked), `locked() -> bool`, `async with`; waiters served FIFO
  - `Semaphore(value: int)`: `async acquire()`, `release()` (ValueError if it would exceed the initial value), `available() -> int`, `async with`; FIFO

Waiters are `Future`s in a `deque`; a cancelled waiter is skipped and must not consume a slot/item.

- [ ] **Step 1: Write failing tests** — `test_event_wakes_all_waiters`, `test_queue_get_waits_for_put`, `test_bounded_queue_put_waits_when_full`, `test_get_nowait_empty_raises`, `test_lock_is_fifo` (three tasks record order 1,2,3), `test_release_unlocked_raises`, `test_semaphore_limits_concurrency` (5 workers, `Semaphore(2)`, max concurrent observed == 2), `test_semaphore_over_release_raises`, `test_cancelled_waiter_does_not_steal_slot`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS; write docs (mention the scheduler will use `Semaphore` for resource pools).
- [ ] **Step 5:** Commit `feat(loop): add Event, Queue, Lock and Semaphore`.

---

### Task 6: Worker threads

**Files:**
- Create: `src/pynet/loop/threads.py`; modify `reactor.py` so `close()` shuts the loop's pool down
- Test: `tests/loop/test_threads.py`
- Docs: `docs/concepts/threads.md` (sending a slow errand out to a helper)

**Interfaces:**
- Consumes: `call_soon_threadsafe`, `Future`.
- Produces:
  - `DEFAULT_WORKERS = 4`
  - `class WorkerPool(max_workers: int = DEFAULT_WORKERS)`: `submit(fn: Callable[[], T], on_done: Callable[[T | None, BaseException | None], None]) -> None`, `shutdown(*, wait: bool = True) -> None`. Our own threads + `queue.SimpleQueue`; threads start lazily up to `max_workers`; daemon threads.
  - `EventLoop.worker_pool() -> WorkerPool` (lazy, one per loop)
  - `async run_in_thread[**P, T](fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T` — result/exception delivered back via `call_soon_threadsafe`; cancelling the awaiting task cancels the Future (the thread still finishes; result discarded).

- [ ] **Step 1: Write failing tests** — `test_run_in_thread_returns_value`, `test_run_in_thread_reraises`, `test_runs_off_loop_thread` (`threading.get_ident()` differs), `test_blocking_calls_overlap` (4 × `time.sleep(0.1)` via gather, elapsed < 0.3), `test_loop_stays_responsive` (a ticker task advances while a thread blocks), `test_shutdown_joins_workers`.
- [ ] **Step 2–4:** FAIL → implement → PASS; write docs.
- [ ] **Step 5:** Commit `feat(loop): run blocking work in helper threads`.

---

### Task 7: Streams (TCP + pipes)

**Files:**
- Create: `src/pynet/streams.py`
- Modify: `src/pynet/errors.py` (+ `ConnectionClosedError(NetConnectionError)`)
- Test: `tests/test_streams.py`
- Docs: `docs/concepts/streams.md` (water pipes with a tap); add `examples/echo_server.py`

**Interfaces:**
- Consumes: `add_reader/remove_reader/add_writer/remove_writer`, `Future`, `run_in_thread` (for `getaddrinfo` when host isn't an IP literal), `spawn`.
- Produces:
  - `DEFAULT_LIMIT = 64 * 1024`, `DEFAULT_HIGH_WATER = 64 * 1024`, `DEFAULT_LOW_WATER = 16 * 1024`, `READ_CHUNK = 64 * 1024`
  - `IncompleteReadError(PyNetError)` with `.partial: bytes`, `.expected: int`; `LimitOverrunError(PyNetError)`
  - `StreamReader`: `async read(n: int = -1) -> bytes` (`-1` = to EOF; `b""` at EOF), `async read_exactly(n: int) -> bytes`, `async read_until(sep: bytes = b"\n", *, limit: int = DEFAULT_LIMIT) -> bytes` (includes `sep`), `at_eof() -> bool`
  - `StreamWriter`: `write(data: bytes) -> None`, `async drain() -> None`, `close() -> None`, `is_closing() -> bool`, `async wait_closed() -> None`, `get_extra_info(name: str, default: object = None) -> object` (`"peername"`, `"sockname"`, `"fd"`). `write` after close and `drain` after peer reset raise `ConnectionClosedError`.
  - `async open_connection(host: str, port: int) -> tuple[StreamReader, StreamWriter]`
  - `Server`: `port: int` (actual bound port), `close()`, `async wait_closed()`, `async __aenter__/__aexit__`
  - `async start_server(handler: Callable[[StreamReader, StreamWriter], Awaitable[None]], host: str, port: int) -> Server` — each connection runs in its own `Task`; a handler exception is reported via `report_error` and that connection is closed; the server keeps accepting.
  - `pipe_reader(fd: int) -> StreamReader`, `pipe_writer(fd: int) -> StreamWriter` (set non-blocking, use `os.read`/`os.write`)

Reading registers a reader only while a read is waiting and removes it when satisfied **or cancelled**; the reader buffers into a `bytearray`. Writer: try `send` immediately, buffer the rest and register a writer callback until empty; `drain()` waits while buffered > high water until it falls ≤ low water.

- [ ] **Step 1: Write failing tests**

```python
def test_echo_roundtrip() -> None: ...                    # start_server on ("127.0.0.1", 0); client writes, reads back
def test_server_port_reports_bound_port() -> None: ...   # port != 0
def test_read_exactly_incomplete_raises() -> None: ...   # peer sends 3 bytes then closes; read_exactly(5) → .partial == b"abc"
def test_read_until_and_limit() -> None: ...
def test_read_returns_empty_at_eof() -> None: ...
def test_drain_applies_backpressure() -> None:
    # peer never reads; write 4*DEFAULT_HIGH_WATER; drain() must not finish within SHORT (wait_for → TimeoutError)
def test_write_after_close_raises() -> None: ...
def test_handler_exception_reported_server_survives() -> None: ...
def test_cancelled_read_leaves_stream_usable() -> None:   # Review Focus 5
    # wait_for(reader.read(1), SHORT) → TimeoutError; fd no longer registered in loop selector;
    # peer then sends b"z"; await reader.read(1) == b"z"
def test_pipe_reader_reads_os_pipe() -> None: ...
```

- [ ] **Step 2–4:** FAIL → implement → PASS; write `streams.md` (backpressure diagram) and `examples/echo_server.py`.
- [ ] **Step 5:** Commit `feat: add non-blocking TCP and pipe streams`.

---

### Task 8: Child watcher and subprocesses

**Files:**
- Create: `src/pynet/loop/children.py`, `src/pynet/process.py`
- Test: `tests/test_process.py`
- Docs: `docs/concepts/subprocesses.md` (helpers + baby monitors; a "how the pros do it" box on kqueue `EVFILT_PROC` / `pidfd`); add `examples/job_runner.py` (run 3 commands at once, tee output, report exit codes and timings)

**Interfaces:**
- Consumes: `call_soon_threadsafe`, `call_later`, `pipe_reader`, `pipe_writer`, `Future`.
- Produces:
  - `CHILD_POLL_INTERVAL = 1.0`
  - `class ChildWatcher`: `install() -> None` (RuntimeError unless on the main thread; idempotent), `is_installed() -> bool`, `uninstall() -> None`, `register(pid: int, loop: EventLoop, on_exit: Callable[[int], None]) -> None`, `unregister(pid: int) -> None`
  - `get_child_watcher() -> ChildWatcher` (process-wide singleton)
  - `run()` (Task 3) installs the watcher when called on the main thread.
  - `class Process`: `pid: int`, `returncode: int | None`, `stdin: StreamWriter | None`, `stdout: StreamReader | None`, `stderr: StreamReader | None`, `async wait() -> int`, `send_signal(sig: int)`, `terminate()`, `kill()`, `kill_group(sig: int = signal.SIGKILL)` (`os.killpg`; no-op if already reaped)
  - `async create_process(args: Sequence[str], *, cwd: str | os.PathLike[str] | None = None, env: Mapping[str, str] | None = None, stdin: bool = False, stdout: bool = True, stderr: bool = True, new_session: bool = True) -> Process` — raises `RuntimeError` mentioning `"child watcher"` if not installed.

Mechanics: `subprocess.Popen` with pipes and `start_new_session=new_session`; never call `Popen.wait/poll` (set `popen.returncode` ourselves after reaping). The SIGCHLD handler loops over tracked pids calling `os.waitpid(pid, os.WNOHANG)` (never `-1`), converts with `os.waitstatus_to_exitcode` (negative = killed by signal) and delivers via `loop.call_soon_threadsafe`. After `register`, do one immediate `waitpid(pid, WNOHANG)` to close the fork/register race. While a loop has live children it schedules a fallback check every `CHILD_POLL_INTERVAL` (covers a delayed signal), and stops when none remain.

- [ ] **Step 1: Write failing tests**

```python
EXIT_CODE = 3
def test_captures_stdout() -> None: ...                    # ["echo","hi"] → b"hi\n", wait() == 0
def test_exit_code() -> None: ...                          # ["sh","-c","exit 3"] → 3
def test_stderr_separate() -> None: ...
def test_kill_gives_negative_code() -> None: ...           # sleep 30; kill(); wait() == -signal.SIGKILL
def test_processes_run_concurrently() -> None: ...         # 3 × sleep 0.3 via gather: elapsed < 0.6
def test_kill_group_releases_grandchild_pipe() -> None:    # Review Focus 2
    # ["sh","-c","sleep 30 & echo hi; wait"]; read b"hi\n"; kill_group(); stdout.read() == b"" within SHORT; wait() returns
def test_spawn_from_background_thread_loop() -> None: ...  # watcher installed in main; run(...) in a thread spawns echo OK
def test_spawn_without_watcher_raises() -> None: ...       # uninstall(); in thread: RuntimeError match "child watcher"
def test_does_not_reap_foreign_children() -> None: ...     # plain subprocess.Popen(["true"]) still waitable by Popen.wait()
def test_stdin_pipe() -> None: ...                         # ["cat"], write b"x", close, read == b"x"
```

- [ ] **Step 2–4:** FAIL → implement → PASS; write `subprocesses.md` and `examples/job_runner.py`.
- [ ] **Step 5:** Commit `feat: run subprocesses on the event loop`.

---

### Task 9: PyStack — "run things at once" demo

**Files (repo `/Users/robcox/development/python/pystack`, branch `feat/pynet-event-loop`):**
- Modify: `src/pystack/plugins/net_plugin.py`
- Test: `tests/test_integration_plugins.py` (`TestNetPlugin`)
- Modify: `README.md` (Shell Commands table row)

**Interfaces:**
- Consumes: `pynet.loop.run`, `gather`, `pynet.process.create_process`.
- Produces: shell command `overlap` — `overlap [n] [seconds]` (defaults `3`, `1`): runs `n` copies of `sleep <seconds>` concurrently on our loop and returns lines like `ran 3 × sleep 1 at once in 1.02s (one after another would take 3s)`. Bad args → usage string. The handler stays synchronous (`run(...)` inside); no async in Pebble.

- [ ] **Step 1:** Write failing tests `test_overlap_runs_concurrently` (handler with `["3", "0.2"]`; output contains `"3 × sleep 0.2"`; parsed elapsed < 0.5) and `test_overlap_bad_args_returns_usage`.
- [ ] **Step 2–4:** FAIL → implement in `shell_commands()` → PASS; update `PluginInfo.description` and README.
- [ ] **Step 5:** Commit in PyStack: `feat(net): add overlap demo for PyNet's event loop`.

---

### Task 10: Framing

**Files:**
- Create: `src/pynet/framing.py`; modify `errors.py` (+ `FrameTooLargeError(PyNetError)` with `.size`, `.limit`)
- Test: `tests/test_framing.py`
- Docs: `docs/concepts/framing.md` (envelopes with the page count written on the front)

**Interfaces:**
- Consumes: `StreamReader.read_exactly`, `StreamWriter`.
- Produces:
  - `HEADER_SIZE = 4`, `MAX_FRAME_SIZE = 16 * 1024 * 1024`
  - `encode_frame(payload: bytes, *, max_size: int = MAX_FRAME_SIZE) -> bytes` (4-byte big-endian unsigned length + payload; raises `FrameTooLargeError`)
  - `FramedConnection(reader, writer, *, max_frame_size: int = MAX_FRAME_SIZE)`: `async send_frame(payload: bytes)`, `async recv_frame() -> bytes`, `close()`, `async wait_closed()`, `writer` property. EOF exactly at a frame boundary → `ConnectionClosedError("peer closed")`; EOF mid-frame → `ConnectionClosedError` mentioning `"mid-frame"`; oversize header → closes the writer and raises `FrameTooLargeError` without reading the body.

- [ ] **Step 1: Write failing tests** — `test_encode_frame_header` (`encode_frame(b"hi") == b"\x00\x00\x00\x02hi"`), `test_roundtrip_many_frames_in_one_write`, `test_frame_split_across_writes`, `test_empty_frame`, `test_oversize_encode_raises`, `test_oversize_header_closes_connection`, `test_clean_eof_vs_mid_frame_eof`.
- [ ] **Step 2–4:** FAIL → implement → PASS; docs.
- [ ] **Step 5:** Commit `feat: add length-prefixed framing`.

---

### Task 11: Message envelope and codec

**Files:**
- Create: `src/pynet/messaging.py` (codec part); modify `errors.py` (+ `ProtocolError(PyNetError)`)
- Test: `tests/test_messaging_codec.py`
- Docs: start `docs/concepts/messaging.md` (walkie-talkies with call signs) — envelope section

**Interfaces:**
- Produces:
  - `PROTOCOL_VERSION = 1`, `KIND_JSON = b"J"`, `KIND_BINARY = b"B"`
  - `type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]`
  - `class MessageType(StrEnum)`: `REQUEST="request"`, `RESPONSE="response"`, `ERROR="error"`, `PUSH="push"`, `PING="ping"`, `PONG="pong"`
  - `@dataclass(frozen=True) class Message`: `type: MessageType`, `id: int | None = None`, `method: str | None = None`, `body: JsonValue = None`, `attachments: tuple[bytes, ...] = ()`
  - `encode_message(msg: Message) -> list[bytes]` — first element `KIND_JSON + json(envelope)` with keys `v, type, id, method, body, attachments` (count; omitted when 0), followed by `KIND_BINARY + data` per attachment.
  - `class MessageDecoder`: `feed(frame: bytes) -> Message | None` — returns a message once its JSON frame and all declared attachments have arrived. Raises `ProtocolError` for: unknown kind byte, binary frame with no pending envelope, invalid UTF-8/JSON, non-object JSON, `v != PROTOCOL_VERSION` (message contains `"unsupported_version"`), unknown `type`, missing `id` on request/response/error, missing `method` on request/push.

- [ ] **Step 1: Write failing tests** — `test_json_envelope_shape` (decode the JSON of `encode_message(Message(MessageType.REQUEST, id=1, method="echo", body={"x": 1}))[0][1:]` equals `{"v": 1, "type": "request", "id": 1, "method": "echo", "body": {"x": 1}}`), `test_roundtrip_with_attachments` (two attachments incl. `b"\x00\xff"`), `test_decoder_waits_for_attachments`, and one `ProtocolError` test per rule above (parametrize).
- [ ] **Step 2–4:** FAIL → implement → PASS; docs.
- [ ] **Step 5:** Commit `feat: add versioned message envelope with binary attachments`.

---

### Task 12: `Peer` — request/response and push

**Files:**
- Modify: `src/pynet/messaging.py`; `errors.py` (+ `RemoteError(PyNetError)` with `.code`, `.remote_message`)
- Test: `tests/test_peer.py`
- Docs: continue `messaging.md`; add `examples/chat_room.py` (server pushes each message to every peer)

**Interfaces:**
- Consumes: `FramedConnection`, `encode_message`, `MessageDecoder`, `spawn`, `wait_for`, `start_server`, `open_connection`.
- Produces:
  - `DEFAULT_REQUEST_TIMEOUT = 30.0`; also define `DEFAULT_HEARTBEAT_INTERVAL = 5.0` and `DEFAULT_MAX_MISSED_PONGS = 3` here (stored on the peer; Task 13 adds the behaviour). Tests in this task pass `heartbeat_interval=None`.
  - `@dataclass(frozen=True) class Reply`: `body: JsonValue = None`, `attachments: tuple[bytes, ...] = ()`
  - `type Handler = Callable[[Message], Awaitable[JsonValue | Reply]]`
  - Error codes (str constants): `UNKNOWN_METHOD="unknown_method"`, `HANDLER_ERROR="handler_error"`, `BAD_MESSAGE="bad_message"`, `UNSUPPORTED_VERSION="unsupported_version"`; error body `{"code": ..., "message": ...}`
  - `class Peer(conn: FramedConnection, *, heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL, max_missed_pongs: int = DEFAULT_MAX_MISSED_PONGS)`:
    `on(method: str, handler: Handler) -> None`, `start() -> None` (spawns reader task, and heartbeat task from Task 13), `async request(method: str, body: JsonValue = None, *, attachments: Sequence[bytes] = (), timeout: float | None = DEFAULT_REQUEST_TIMEOUT) -> Message`, `async push(method: str, body: JsonValue = None, *, attachments: Sequence[bytes] = ()) -> None`, `on_close(cb: Callable[[BaseException | None], None]) -> None`, `closed: bool`, `async close() -> None`, `async wait_closed() -> None`, `peername: object`
  - `async connect(host: str, port: int, *, setup: Callable[[Peer], None] | None = None, **peer_kwargs: Any) -> Peer` (setup runs before `start()`)
  - `async serve(host: str, port: int, setup: Callable[[Peer], None | Awaitable[None]], **peer_kwargs: Any) -> Server`

Each incoming request runs its handler in its own task (requests on one connection are concurrent). Request ids are per-peer increasing ints. Responses for unknown ids are logged and ignored. A push with no handler is logged and dropped (no reply). A `ProtocolError` from the decoder sends an `error` reply when the bad message had a usable `id`, otherwise logs; the connection stays open except for framing errors. Sends are serialised by a `Lock` so a message's attachment frames are never interleaved with another message's. On close, every pending request future fails with `ConnectionClosedError` and `on_close` callbacks run once with the cause.

- [ ] **Step 1: Write failing tests**

```python
def test_request_response() -> None: ...                     # "echo" handler returns body
def test_reply_with_attachments() -> None: ...
def test_concurrent_requests_on_one_connection() -> None: ... # slow handler(0.2) + fast handler; fast returns first
def test_unknown_method_gives_remote_error() -> None: ...     # RemoteError.code == UNKNOWN_METHOD
def test_handler_error_then_connection_still_works() -> None: # Review Focus 4
    ...  # RemoteError code HANDLER_ERROR; next request succeeds
def test_push_both_directions() -> None: ...
def test_request_timeout() -> None: ...                       # TimeoutError; late response ignored
def test_peer_drop_fails_pending_requests() -> None:          # Review Focus 1
    ...  # server handler sleeps; server closes the socket; client request raises ConnectionClosedError within SHORT
def test_on_close_called_once_with_cause() -> None: ...
```

- [ ] **Step 2–4:** FAIL → implement → PASS; docs + `examples/chat_room.py`.
- [ ] **Step 5:** Commit `feat: add request/response and push messaging`.

---

### Task 13: Heartbeats

**Files:**
- Modify: `src/pynet/messaging.py`; `errors.py` (+ `HeartbeatTimeoutError(ConnectionClosedError)`)
- Test: `tests/test_heartbeat.py`
- Docs: `docs/concepts/heartbeats-and-reconnect.md` (first half: "are you still there?"); `examples/heartbeat_demo.py`

**Interfaces:**
- Produces: heartbeat behaviour for the constants defined in Task 12. With `heartbeat_interval` set, the peer sends `ping` every interval and counts consecutive pings without a `pong`; when the count exceeds `max_missed_pongs` it closes with `HeartbeatTimeoutError`. Any `ping` received is answered with `pong` immediately (handled in the reader, not a handler task). `heartbeat_interval=None` disables sending. Both sides ping by default.

- [ ] **Step 1: Write failing tests (FakeClock via `run(..., clock=clock)` and a helper task that advances it)** — `test_pings_sent_each_interval`, `test_pong_resets_missed_count`, `test_silent_peer_declared_dead` (peer whose reader is paused/never pongs → `on_close` cause is `HeartbeatTimeoutError` after `(max_missed + 1) × interval`), `test_heartbeat_disabled`.
- [ ] **Step 2–4:** FAIL → implement → PASS; docs + example.
- [ ] **Step 5:** Commit `feat: add heartbeats to detect dead peers`.

---

### Task 14: Reconnecting client

**Files:**
- Modify: `src/pynet/messaging.py`; `errors.py` (+ `NotConnectedError(PyNetError)`)
- Test: `tests/test_reconnect.py`
- Docs: finish `heartbeats-and-reconnect.md` (redialling, backoff + jitter diagram)

**Interfaces:**
- Produces:
  - `DEFAULT_INITIAL_BACKOFF = 0.5`, `DEFAULT_MAX_BACKOFF = 30.0`, `DEFAULT_JITTER = 0.1`
  - `backoff_delay(attempt: int, *, initial: float, maximum: float, jitter: float, rng: random.Random) -> float` = `min(maximum, initial * 2**attempt) * (1 + rng.uniform(-jitter, jitter))`
  - `class ConnectionEvent(StrEnum)`: `CONNECTED`, `DISCONNECTED`, `RECONNECTED`
  - `class ReconnectingClient(host: str, port: int, *, setup: Callable[[Peer], None] | None = None, initial_backoff=DEFAULT_INITIAL_BACKOFF, max_backoff=DEFAULT_MAX_BACKOFF, jitter=DEFAULT_JITTER, rng: random.Random | None = None, **peer_kwargs: Any)`:
    `start() -> None`, `async stop() -> None`, `connected: bool`, `peer: Peer | None`, `on_event(cb: Callable[[ConnectionEvent], None]) -> None`, `async wait_connected(timeout: float | None = None) -> None`, `async request(...)` / `async push(...)` (same signature as `Peer`; raise `NotConnectedError` while down — **no queueing or replay**). `setup` re-runs on every new `Peer`. Attempt counter resets after a successful connect. `CONNECTED` fires on the first success, `RECONNECTED` on later ones.

- [ ] **Step 1: Write failing tests** — `test_backoff_doubles_and_caps` (rng with jitter=0: 0.5, 1, 2, …, capped at 30), `test_backoff_jitter_bounds`, `test_connects_when_server_appears_later`, `test_reconnects_after_server_restart` (same port; events `[CONNECTED, DISCONNECTED, RECONNECTED]`; setup handlers work again), `test_request_while_down_raises_not_connected`, `test_stop_cancels_retry_loop`.
- [ ] **Step 2–4:** FAIL → implement → PASS; docs.
- [ ] **Step 5:** Commit `feat: add auto-reconnecting client`.

---

### Task 15: PyStack — message round-trip demo

**Files (PyStack branch `feat/pynet-event-loop`):** `src/pystack/plugins/net_plugin.py`, `tests/test_integration_plugins.py`, `README.md`

**Interfaces:**
- Consumes: `pynet.messaging.serve`, `connect`, `Peer.request`, `pynet.loop.run`.
- Produces: shell command `netecho <text...>`: serves on `127.0.0.1:0` with an `echo` handler, connects, sends one request, returns `echo: <text> (round trip 0.4 ms)`. No args → usage.

- [ ] **Step 1:** Failing tests `test_netecho_round_trip` (output starts with `"echo: hello world"`) and `test_netecho_usage`.
- [ ] **Step 2–4:** FAIL → implement → PASS; README row.
- [ ] **Step 5:** PyStack commit `feat(net): add netecho demo for PyNet messaging`.

---

### Task 16: WebSocket handshake

**Files:**
- Create: `src/pynet/ws/__init__.py`, `src/pynet/ws/handshake.py`; modify `src/pynet/base64.py` (+ `b64_decode_bytes(encoded: str) -> bytes`, `b64_decode` becomes a thin UTF-8 wrapper over it — same public behaviour)
- Test: `tests/test_ws_handshake.py`, extend `tests/test_base64.py`
- Docs: start `docs/concepts/websocket-real.md` (the phone call, for real); add a "See it for real" link in `docs/concepts/websocket.md`

**Interfaces:**
- Produces:
  - `WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"`, `KEY_BYTES = 16`
  - `HandshakeError(ProtocolError)`
  - `compute_accept(key: str) -> str` — `b64_encode(hashlib.sha1((key + WS_GUID).encode()).digest())`
  - `generate_key(randbytes: Callable[[int], bytes] = os.urandom) -> str`
  - `validate_client_handshake(method: str, headers: Mapping[str, str]) -> str` — case-insensitive header names; requires `GET`, `Upgrade: websocket` (case-insensitive value), `Connection` containing token `upgrade`, `Sec-WebSocket-Version: 13`, a key decoding to `KEY_BYTES` bytes; returns the key; otherwise `HandshakeError` naming the problem.
  - `server_handshake_response(key: str) -> bytes` — `HTTP/1.1 101 Switching Protocols` + `Upgrade`, `Connection`, `Sec-WebSocket-Accept` headers + blank line
  - `client_handshake_request(host: str, path: str, key: str) -> bytes`
  - `parse_http_head(raw: bytes) -> tuple[str, str, dict[str, str]]` (start-line method/target or status, headers) — used by the endpoints in Task 18.

- [ ] **Step 1: Write failing tests** — `test_rfc_accept_vector` (`compute_accept("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="`), `test_headers_case_insensitive`, parametrized `test_invalid_handshake_rejected` (wrong method, missing upgrade, version 8, 15-byte key), `test_response_contains_accept`, `test_b64_decode_bytes_roundtrip_binary` (`b"\x00\xff\x10"`), `test_b64_decode_unchanged`.
- [ ] **Step 2–4:** FAIL → implement → PASS; docs.
- [ ] **Step 5:** Commit `feat(ws): add real WebSocket opening handshake`.

---

### Task 17: WebSocket frames

**Files:**
- Create: `src/pynet/ws/frames.py`
- Test: `tests/test_ws_frames.py`
- Docs: continue `websocket-real.md` (frame bit diagram)

**Interfaces:**
- Consumes: `StreamReader.read_exactly`.
- Produces:
  - `class Opcode(IntEnum)`: `CONTINUATION=0x0, TEXT=0x1, BINARY=0x2, CLOSE=0x8, PING=0x9, PONG=0xA`
  - `@dataclass(frozen=True) class Frame`: `fin: bool`, `opcode: Opcode`, `payload: bytes`
  - `MAX_CONTROL_PAYLOAD = 125`, `MASK_KEY_SIZE = 4`
  - `apply_mask(data: bytes, key: bytes) -> bytes`
  - `encode_ws_frame(frame: Frame, *, mask_key: bytes | None = None) -> bytes` — 7/16/64-bit lengths (126 → 2-byte, 127 → 8-byte)
  - `async read_ws_frame(reader: StreamReader, *, max_payload: int, expect_masked: bool) -> Frame` — raises `ProtocolError` for: reserved bits set, unknown opcode, control frame > 125 bytes or not FIN, mask presence ≠ `expect_masked`; raises `FrameTooLargeError` above `max_payload`.

- [ ] **Step 1: Write failing tests** — RFC 6455 §5.7 vectors:

```python
UNMASKED_HELLO = bytes([0x81, 0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F])
MASK = bytes([0x37, 0xFA, 0x21, 0x3D])
MASKED_HELLO = bytes([0x81, 0x85, 0x37, 0xFA, 0x21, 0x3D, 0x7F, 0x9F, 0x4D, 0x51, 0x58])
def test_encode_unmasked_hello() -> None: assert encode_ws_frame(Frame(True, Opcode.TEXT, b"Hello")) == UNMASKED_HELLO
def test_encode_masked_hello() -> None: ...  == MASKED_HELLO
def test_16_and_64_bit_lengths_roundtrip() -> None: ...   # 256-byte and 65536-byte payloads; header bytes 126 / 127
def test_read_masked_frame() -> None: ...                 # via a socketpair stream
# parametrized ProtocolError cases + FrameTooLargeError case
```

- [ ] **Step 2–4:** FAIL → implement → PASS; docs.
- [ ] **Step 5:** Commit `feat(ws): encode and decode WebSocket frames`.

---

### Task 18: WebSocket connection and endpoints

**Files:**
- Create: `src/pynet/ws/connection.py`, `src/pynet/ws/endpoints.py`
- Test: `tests/test_ws_connection.py`
- Docs: finish `websocket-real.md`; `examples/ws_echo.py` (server + a tiny HTML page snippet in the docs showing a browser connecting)

**Interfaces:**
- Consumes: frames + handshake (Tasks 16–17), streams, `Lock`.
- Produces:
  - Close codes: `CLOSE_NORMAL=1000`, `CLOSE_GOING_AWAY=1001`, `CLOSE_PROTOCOL_ERROR=1002`, `CLOSE_UNSUPPORTED_DATA=1003`, `CLOSE_INVALID_UTF8=1007`, `CLOSE_TOO_BIG=1009`; `DEFAULT_WS_MAX_MESSAGE = 1024 * 1024`
  - `class WebSocketClosed(ConnectionClosedError)` with `.code: int`, `.reason: str`
  - `class WebSocket(reader, writer, *, is_client: bool, max_message_size: int = DEFAULT_WS_MAX_MESSAGE)`: `async send_text(text: str)`, `async send_bytes(data: bytes)`, `async ping(data: bytes = b"")`, `async recv() -> str | bytes`, `async close(code: int = CLOSE_NORMAL, reason: str = "")`, `close_code: int | None`, `closed: bool`.
    Clients mask every frame with a fresh `os.urandom(4)` key; servers expect masked frames. `recv()` reassembles continuation frames, answers pings with pongs inline, ignores unsolicited pongs, and on a close frame echoes it then raises `WebSocketClosed`. Protocol violations close with 1002, invalid UTF-8 text with 1007, oversize messages with 1009 — then raise `WebSocketClosed` locally.
  - `async accept_websocket(reader, writer, *, method: str, headers: Mapping[str, str]) -> WebSocket` — validates, writes the 101 response, returns a server-side `WebSocket`; on `HandshakeError` writes `400 Bad Request` and re-raises. (pyweb will call this.)
  - `async serve_websocket(handler: Callable[[WebSocket], Awaitable[None]], host: str, port: int) -> Server` — reads the request head with `read_until(b"\r\n\r\n")`, uses `parse_http_head`, then `accept_websocket`.
  - `async connect_websocket(host: str, port: int, path: str = "/") -> WebSocket` — sends the client handshake, checks status 101 and the accept value.

- [ ] **Step 1: Write failing tests** — `test_text_echo`, `test_binary_echo`, `test_fragmented_message_reassembled` (client writes raw TEXT fin=0 + CONTINUATION fin=1), `test_ping_answered_with_pong`, `test_close_handshake_codes` (client closes 1000 "bye" → server's recv raises `WebSocketClosed(code=1000, reason="bye")`), `test_invalid_utf8_closes_1007`, `test_oversize_message_closes_1009`, `test_unmasked_client_frame_closes_1002`, `test_bad_handshake_gets_400`.
- [ ] **Step 2–4:** FAIL → implement → PASS; docs + example.
- [ ] **Step 5:** Commit `feat(ws): add WebSocket connections, server and client`.

---

### Task 19: PyStack — WebSocket echo demo

**Files (PyStack branch):** `src/pystack/plugins/net_plugin.py`, `tests/test_integration_plugins.py`, `README.md`

**Interfaces:**
- Produces: shell command `wsecho <text...>`: starts `serve_websocket` on `127.0.0.1:0` echoing text, connects with `connect_websocket`, sends the text, returns `ws echo: <text>`. No args → usage.

- [ ] **Step 1:** Failing tests `test_wsecho_round_trip`, `test_wsecho_usage`.
- [ ] **Step 2–4:** FAIL → implement → PASS; README row; update PyStack `docs/index.md` mention of PyNet's real networking.
- [ ] **Step 5:** PyStack commit `feat(net): add wsecho demo for PyNet WebSockets`.

---

### Task 20: Docs wrap-up and learning path

**Files:**
- Create: `docs/concepts/our-loop-vs-asyncio.md` (homemade go-kart vs factory car: a side-by-side table — `EventLoop`↔`asyncio` loop, `Future`, `Task`, `Cancelled`↔`CancelledError`, `timeout`, `StreamReader/Writer`, `create_process`, `run_in_thread`↔`to_thread`; plus what asyncio adds that we skipped)
- Modify: `docs/index.md` (new "Real networking" section + learning path: simulation page → real page, in order event-loop → timers → tasks-and-await → cancellation → sync → threads → self-pipe → subprocesses → streams → framing → messaging → heartbeats → websocket-real → our-loop-vs-asyncio), `README.md` (feature list, quick start using `run` + `start_server`, examples list), `docs/concepts/tcp.md` (link to `streams.md`), `websocket.md` (already linked in Task 16)
- Test: `tests/test_examples.py` — each `examples/*.py` imports cleanly and exposes `main()`; `job_runner.main()` and `echo_server`'s self-test function run to completion under the per-test timeout.

- [ ] **Step 1:** Write `tests/test_examples.py` → run → FAIL if any example lacks `main()`.
- [ ] **Step 2:** Fix examples; write the docs; every "Next up" link resolves (`grep -o '([a-z-]*\.md)' docs/concepts/*.md` all exist).
- [ ] **Step 3:** Full suite: ruff, format, pyright, pytest ≥80% → all pass; `grep -rn "import asyncio" src` → no output.
- [ ] **Step 4:** Commit `docs: add real-networking learning path and asyncio comparison`.
- [ ] **Step 5:** Open the PyNet PR, then the PyStack PR (noting it depends on the PyNet PR merging first).
