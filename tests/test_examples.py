"""Check that every example imports cleanly and exposes ``main()``."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.py"))
ECHO_MESSAGE = b"hello\n"


def load_example(path: Path) -> ModuleType:
    """Import the example at ``path`` (examples/ is not a package)."""
    spec = importlib.util.spec_from_file_location(f"example_{path.stem}", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestExamples:
    """Test the runnable examples."""

    def test_examples_exist(self) -> None:
        """Find at least one example file."""
        assert EXAMPLE_FILES

    @pytest.mark.parametrize("path", EXAMPLE_FILES, ids=lambda p: p.stem)
    def test_exposes_main(self, path: Path) -> None:
        """Import each example and expect a callable ``main``."""
        module = load_example(path)
        assert callable(getattr(module, "main", None))

    def test_echo_server_self_test(self) -> None:
        """Round-trip one message through the echo server's self-test."""
        module = load_example(EXAMPLES_DIR / "echo_server.py")
        assert module.self_test() == ECHO_MESSAGE

    def test_job_runner_main(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Run the job runner to completion and see its report."""
        module = load_example(EXAMPLES_DIR / "job_runner.py")
        module.main()
        assert "all jobs done" in capsys.readouterr().out
