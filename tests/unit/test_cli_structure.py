"""Tests for CLI command package layout.

Run: uv run pytest tests/unit/test_cli_structure.py

Covers the root CLI group registration so command packages stay split by user-facing group.
"""

from click.core import Group

from valkyrie.cli.agent import agent
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.config import config
from valkyrie.cli.main import cli
from valkyrie.cli.run import run


def test_cli_groups_are_registered_from_subpackages() -> None:
    """The root CLI should register command groups exported by their subpackages.

    Test cases:
    - The root command names point at the group objects from the matching subpackages.
    - Each subpackage group has loaded its command handlers.
    """
    expected_groups = {
        "agent": agent,
        "benchmark": benchmark,
        "config": config,
        "run": run,
    }

    for group_name, command_group in expected_groups.items():
        assert cli.commands[group_name] is command_group
        assert isinstance(command_group, Group)
        assert command_group.commands
