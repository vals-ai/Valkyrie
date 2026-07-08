import click

from valkyrie.cli.config.auth import auth
from valkyrie.cli.config.base import config_remove, init, set
from valkyrie.cli.config.providers import provider
from valkyrie.cli.config.services import service


@click.group()
def config():
    """Config command group"""
    pass


config.add_command(auth)
config.add_command(init)
config.add_command(set)
config.add_command(config_remove)
config.add_command(provider)
config.add_command(service)

__all__ = ["config"]
