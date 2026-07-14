"""CLI views/commands for Valkyrie."""

import click
from dotenv import load_dotenv

from valkyrie.cli.agent import agent
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.config import config
from valkyrie.cli.logging import configure_cli_logging
from valkyrie.cli.run import run


@click.group()
def cli():
    """Valkyrie CLI."""
    configure_cli_logging()
    load_dotenv()
    configure_cli_logging()


cli.add_command(run)
cli.add_command(agent)
cli.add_command(benchmark)
cli.add_command(config)

__all__ = ["cli"]


if __name__ == "__main__":
    cli()
