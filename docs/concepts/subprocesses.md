# Subprocesses: Helpers and Baby Monitors

## The Analogy

The event loop is a busy **waiter**. Some jobs are far too big for the
waiter -- baking a wedding cake (`make`), washing every dish in the
building (`pytest`). So the waiter hires **helpers**: separate programs
that do a whole job by themselves.

- You hand each helper a **job card** (the command, like
  `["echo", "hi"]`).
- Helpers talk back through **pipes**: one for what you tell them
  (`stdin`), one for what they say (`stdout`), one for complaints
  (`stderr`). These are ordinary [streams](streams.md), so listening to
  a chatty helper never freezes the waiter.
- You do not keep popping your head round the door asking "finished
  yet?". Each helper's room has a **baby monitor**. When a helper
  finishes, the monitor crackles (the kernel sends a `SIGCHLD` signal),
  and you go and collect their result -- the **exit code**: `0` for "all
  good", another number for "something went wrong", or minus a signal
  number (`-9`) if they were stopped with a signal.

The monitor only says "one of the children needs you", never *which*
one -- so you check each room you are responsible for.

## How It Works

```
 your task                  child watcher                       kernel
 ---------                  -------------                       ------
 create_process(args) --> register(pid, loop, on_exit)
   (Popen + our pipes)      + one immediate waitpid check
 await process.wait()                                    helper exits ->
   ...loop serves                                        SIGCHLD
   other tasks...           C signal handler writes a byte
                            to the bell (set_wakeup_fd)
                            night-nurse thread wakes:
                              for each tracked pid:
                                waitpid(pid, WNOHANG)
                            loop.call_soon_threadsafe(on_exit)
 wait() returns the code <-- on_exit(code) runs on the loop
```

- **Our own pipes.** `create_process` makes the pipes with `os.pipe()`,
  gives the child its ends through `subprocess.Popen`, then closes the
  parent's copies of them. `os.pipe()` fds are not inheritable, so when
  three helpers start at once, none of them grabs another's pipe by
  accident (which would stop that pipe ever reaching EOF).
- **We never call `Popen.wait()` or `Popen.poll()`.** The watcher reaps
  the child and we set `popen.returncode` ourselves -- at the very moment
  of reaping, not later on the loop, so it is recorded even if the loop
  closes first -- and the `Popen` object stays quiet when it is thrown away.
- **Only our children.** The watcher calls `waitpid(pid, WNOHANG)` for
  each pid it tracks, never `waitpid(-1)`, so a `subprocess.run()`
  elsewhere in your program still gets to wait for its own child.
- **A bell, not a lock.** A signal can interrupt *any* line of
  main-thread code, even one holding a lock, so our signal handler does
  no real work. `signal.set_wakeup_fd` makes Python's C-level handler
  write a byte to our socketpair the instant the signal arrives -- the
  [self-pipe trick](self-pipe-trick.md) again. A Python-level handler
  alone is not enough: it only runs between bytecodes, so a signal that
  lands just as the main loop goes to sleep in `select` would not be
  heard until something else woke it. A small daemon thread (the "night
  nurse") sleeps on that bell and does the `waitpid` sweep under a
  `threading.Lock`. If someone had the wakeup fd before us, the nurse
  passes the signal bytes on to them.
- **Safety nets.** Right after `register` we check the child once (it
  may have finished before we were listening). And while a loop has
  live children it checks them itself every `CHILD_POLL_INTERVAL` (1
  second), in case a signal goes missing; the check stops as soon as
  the last child is gone, so an idle loop still sleeps.
- **Any thread.** `run()` on the main thread installs the watcher (only
  the main thread may set signal handlers). After that, loops in other
  threads can start processes too. Without a watcher, `create_process`
  raises a `RuntimeError` that says how to install one.
- **Whole groups.** By default each helper starts in a **new session**,
  so it leads its own process group. `kill_group()` sends a signal
  (default `SIGKILL`) to the whole group -- the helper *and* anything it
  started. That matters: in `sh -c "sleep 30 & echo hi; wait"`, killing
  only `sh` leaves `sleep` holding the stdout pipe open, so your read
  would wait 30 seconds for EOF.
- **No stale signals.** `kill()`, `terminate()`, `send_signal()` and
  `kill_group()` do nothing once the child has been reaped, so they can
  never hit a stranger that reused the pid.
- **Tidy up.** `process.close()` closes any pipe still open (for
  example a `stderr` nobody read), throwing away unread output.

> **How the pros do it.** A signal plus `waitpid` is the classic Unix
> way, but signals are awkward: there is only one `SIGCHLD` handler per
> program, and it does not say which child exited. Newer kernels let you
> watch a *process* like a file descriptor instead. On macOS (and the
> BSDs) **kqueue** has `EVFILT_PROC` with `NOTE_EXIT`: you register a
> pid and `kevent` tells you when that exact process exits. On Linux,
> **`pidfd_open(pid)`** (Python's `os.pidfd_open`) returns an fd that
> becomes readable when the process exits, so you can hand it straight
> to `add_reader` -- no signals and no helper thread. asyncio, Trio and
> libuv use these where they can. We use `SIGCHLD` because it works the
> same way on both systems and shows what the kernel is really doing.

## Try It

```python
from pynet.loop import run, wait_for
from pynet.process import create_process


async def main():
    process = await create_process(["sh", "-c", "echo hello; exit 3"])
    print(await process.stdout.read())  # b'hello\n'
    print(await wait_for(process.wait(), 5))  # 3
    process.close()


run(main())
```

`examples/job_runner.py` starts three jobs at once, prints every line
they write with the job's name in front, kills the slow one's whole
group when it runs out of time, and reports exit codes and timings:
`uv run python examples/job_runner.py`.

## When to Use

- Running other programs from an async program: build steps, test
  runners, a job scheduler that runs many commands at once.
- Streaming a command's output as it arrives instead of waiting for it
  to finish.
- Putting a time limit on a command: wrap the work in `timeout(...)`
  and call `kill_group()` when it fires.

If you only need one short command and nothing else is happening, plain
`subprocess.run` is simpler.

## What We Test

- `echo hi` gives `b"hi\n"` and exit code `0`; `exit 3` gives `3`;
  stdout and stderr stay separate; `cat` echoes what we write to stdin.
- `cwd` and `env` reach the child; a missing program raises
  `FileNotFoundError`; no pipes are made when none are asked for.
- `kill()` gives `-SIGKILL` and `terminate()` gives `-SIGTERM`; signals
  after the child has been reaped do nothing.
- `sh -c "sleep 30 & echo hi; wait"`: after `kill_group()`, stdout
  reaches EOF and `wait()` returns within a short timeout.
- Three `sleep 0.3` jobs run at the same time (well under 0.6s).
- Several `wait()` calls get the same code, and a timed-out `wait()`
  does not spoil the next one.
- A loop in a background thread can start processes; without the
  watcher, `create_process` raises an error mentioning the child
  watcher; installing off the main thread raises.
- A plain `subprocess.Popen` child is left for `Popen.wait()`.
- With `SIGCHLD` switched off, the fallback poll still notices the
  exit; the poll stops once no children remain.
- `uninstall()` puts back the previous handler and wakeup fd, and the
  previous wakeup fd still hears its signals while we are installed.
- `StreamReader.close()` closes the fd and wakes a waiting read.

## Next Up

Back to the [Event Loop](event-loop.md) overview, or see how the loop
is woken from other threads in [The Self-Pipe Trick](self-pipe-trick.md).

Once bytes flow, learn how to cut them into messages with
[Framing](framing.md).
