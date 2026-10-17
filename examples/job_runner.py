"""A kitchen with three helpers -- start every job at once and hear each one finish.

Runs three commands side by side on one event loop. Each line a job
prints is shown straight away with the job's name in front (and kept,
like ``tee``), and at the end you get each job's exit code and how long
it took. The slow job goes over its time limit, so its whole process
group is killed -- the shell *and* the ``sleep`` it started.

    uv run python examples/job_runner.py
"""

import time
from dataclasses import dataclass, field

from pynet.loop import gather, run, timeout, wait_for
from pynet.process import Process, create_process
from pynet.streams import IncompleteReadError, StreamReader

JOB_TIMEOUT = 1.0
REAP_TIMEOUT = 5.0
JOBS: dict[str, list[str]] = {
    "greet": ["sh", "-c", "echo hello; sleep 0.2; echo world"],
    "grumble": ["sh", "-c", "echo 'something went wrong' >&2; exit 2"],
    "slowpoke": ["sh", "-c", "echo 'starting a long nap'; sleep 30; echo 'never printed'"],
}


@dataclass
class JobResult:
    """What one job printed, how it ended, and how long it took."""

    name: str
    lines: list[str] = field(default_factory=list[str])
    code: int | None = None
    seconds: float = 0.0
    timed_out: bool = False


async def tee(name: str, label: str, reader: StreamReader, result: JobResult) -> None:
    """Print each line from ``reader`` with the job's name, and keep it."""
    while not reader.at_eof():
        try:
            line = await reader.read_until()
        except IncompleteReadError as error:  # EOF: a last line with no newline
            line = error.partial
        if line:
            text = line.decode(errors="replace").rstrip("\n")
            result.lines.append(text)
            print(f"[{name}]{label} {text}")


def output_pipes(process: Process) -> list[tuple[str, StreamReader | None]]:
    """Return the process's output pipes with the label each one prints."""
    return [("", process.stdout), (" (stderr)", process.stderr)]


async def run_job(name: str, args: list[str]) -> JobResult:
    """Run one job, tee its output, and kill its group if it takes too long."""
    result = JobResult(name)
    start = time.monotonic()
    process = await create_process(args)
    readers = [(label, reader) for label, reader in output_pipes(process) if reader is not None]
    try:
        async with timeout(JOB_TIMEOUT):
            await gather(*(tee(name, label, reader, result) for label, reader in readers))
            result.code = await process.wait()
    except TimeoutError:
        result.timed_out = True
        process.kill_group()
        result.code = await wait_for(process.wait(), REAP_TIMEOUT)
    finally:
        process.close()
    result.seconds = time.monotonic() - start
    return result


async def run_all() -> list[JobResult]:
    """Start every job at once and wait for all of them."""
    results = await gather(*(run_job(name, args) for name, args in JOBS.items()))
    return [result for result in results if isinstance(result, JobResult)]


def main() -> None:
    """Run the jobs and print a little report."""
    start = time.monotonic()
    results = run(run_all())
    print()
    for result in results:
        ending = "timed out, group killed" if result.timed_out else "finished"
        print(f"{result.name:>9}: exit {result.code} after {result.seconds:.2f}s ({ending})")
    print(f"all jobs done in {time.monotonic() - start:.2f}s")


if __name__ == "__main__":
    main()
