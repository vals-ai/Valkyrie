"""CLI views/commands for Valkyrie."""

from typing import Any

import click
from dotenv import load_dotenv

from valkyrie.cli.agent import agent
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.logging import configure_cli_logging
from valkyrie.cli.login import login, logout, token, whoami
from valkyrie.cli.org import org
from valkyrie.cli.provider import provider
from valkyrie.cli.queue import queue
from valkyrie.cli.run import run
from valkyrie.cli.secret import secret
from valkyrie.sdk.errors import ValkyrieConfigError


class _ValkyrieCLI(click.Group):
    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except ValkyrieConfigError as error:
            raise click.ClickException(str(error)) from error


@click.group(cls=_ValkyrieCLI)
def cli():
    """Valkyrie CLI."""
    configure_cli_logging()
    load_dotenv()
    configure_cli_logging()


cli.add_command(run)
cli.add_command(agent)
cli.add_command(benchmark)
cli.add_command(queue)
cli.add_command(secret)
cli.add_command(provider)
cli.add_command(login)
cli.add_command(logout)
cli.add_command(whoami)
cli.add_command(token)
cli.add_command(org)

__all__ = ["cli"]


if __name__ == "__main__":
    cli()
