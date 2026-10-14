"""A Click group that imports a subcommand's module only when that subcommand runs."""

from collections.abc import Mapping
from importlib import import_module
from typing import Any

import click
from click.utils import make_default_short_help


class LazyGroup(click.Group):
    """Group whose subcommands are registered as `name -> ("module:attribute", short help)`.

    The short help lives in the registry because Click's help listing otherwise imports every
    subcommand to read its docstring. Registry order is the order the reference docs follow; help
    and completion list names sorted, as `click.Group` does.
    """

    def __init__(self, *args: Any, lazy_commands: Mapping[str, tuple[str, str]], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.lazy_commands = dict(lazy_commands)

    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted(self.lazy_commands)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        if cmd_name not in self.lazy_commands:
            return None
        module, attribute = self.lazy_commands[cmd_name][0].split(":")
        return getattr(import_module(module), attribute)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        names = self.list_commands(ctx)
        # Same width budget as click.Group.format_commands, so long help truncates identically.
        limit = formatter.width - 6 - max(map(len, names))
        with formatter.section("Commands"):
            formatter.write_dl([(name, make_default_short_help(self.lazy_commands[name][1], limit)) for name in names])
