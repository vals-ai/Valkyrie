"""Tests for the installed CLI entrypoint.

Run: uv run pytest tests/unit/cli/test_entry.py
"""

import pytest

from valkyrie.cli import entry
from valkyrie.cli import main as cli_main


def test_entrypoint_maps_keyboard_interrupt_to_shell_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """Interrupted commands must return the standard shell cancellation status.

    Test cases:
    - KeyboardInterrupt raised by Click exits with status 130 instead of a traceback.
    """

    def interrupt() -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_main, "cli", interrupt)

    with pytest.raises(SystemExit) as error:
        entry.main()

    assert error.value.code == 130
