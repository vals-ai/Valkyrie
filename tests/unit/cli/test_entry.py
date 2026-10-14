"""Tests for the installed CLI entrypoint.

Run: uv run pytest tests/unit/cli/test_entry.py

Covers interrupt handling, the lazy command registry, and what the command modules import.
"""

import os
import subprocess
import sys
from pathlib import Path

import click
import pytest

from valkyrie.cli import entry
from valkyrie.cli import main as cli_main
from valkyrie.cli.agent import agent
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.lazy import LazyGroup
from valkyrie.cli.run import run

_SERVER_MODULES = ("tracker", "daytona", "modal", "fastapi", "textual")

_COMMANDS_PROBE = f"""
import sys
import click
from valkyrie.cli.main import cli

def load(group):
    ctx = click.Context(group)
    for name in group.list_commands(ctx):
        if isinstance(command := group.get_command(ctx, name), click.Group):
            load(command)

load(cli)
print(" ".join(name for name in {_SERVER_MODULES!r} if name in sys.modules))
"""


def test_entrypoint_maps_keyboard_interrupt_to_shell_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    """Interrupted commands must return the standard shell cancellation status.

    Test cases:
    - KeyboardInterrupt raised by Click exits with status 130 instead of a traceback.
    """

    def interrupt() -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_main, "cli", interrupt)

    # main() sets this process-wide; setting it here lets monkeypatch restore it.
    monkeypatch.setenv("PYDANTIC_DISABLE_PLUGINS", "__all__")

    with pytest.raises(SystemExit) as error:
        entry.main()

    assert error.value.code == 130


def test_entrypoint_disables_pydantic_plugins_before_commands_load(tmp_path: Path) -> None:
    """`secret --help` defines pydantic models, so Logfire's plugin would import Logfire if plugins were left on."""
    probe = """
import io, sys
from contextlib import redirect_stdout
from valkyrie.cli import entry

with redirect_stdout(io.StringIO()):
    try:
        entry.main()
    except SystemExit:
        pass
print("logfire" in sys.modules)
"""
    environment = {name: value for name, value in os.environ.items() if name != "PYDANTIC_DISABLE_PLUGINS"}
    result = subprocess.run(
        [sys.executable, "-c", probe, "secret", "--help"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "False"


@pytest.mark.parametrize("group", [cli_main.cli, run, agent, benchmark], ids=lambda group: group.name)
def test_lazy_help_matches_imported_commands(group: LazyGroup) -> None:
    """The registry's help listing must match what Click prints after importing every subcommand."""
    ctx = click.Context(group)
    lazy_help = click.HelpFormatter()
    imported_help = click.HelpFormatter()

    group.format_commands(ctx, lazy_help)
    click.Group.format_commands(group, ctx, imported_help)

    assert lazy_help.getvalue() == imported_help.getvalue()


def test_command_modules_do_not_import_server_dependencies(tmp_path: Path) -> None:
    """Imports every command, in a subprocess because this test process has already imported them."""
    probe = subprocess.run(
        [sys.executable, "-c", _COMMANDS_PROBE], cwd=tmp_path, capture_output=True, text=True, check=True
    )

    assert probe.stdout.strip() == ""
