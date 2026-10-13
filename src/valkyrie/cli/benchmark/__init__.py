import click

from valkyrie.cli.benchmark.services import service
from valkyrie.cli.benchmark.tasks import tasks


@click.group()
def benchmark():
    """Benchmark command group"""
    pass


benchmark.add_command(tasks)
benchmark.add_command(service)

__all__ = ["benchmark"]
