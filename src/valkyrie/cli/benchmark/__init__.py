import click

from valkyrie.cli.benchmark.tasks import tasks


@click.group()
def benchmark():
    """Benchmark command group"""
    pass


benchmark.add_command(tasks)

__all__ = ["benchmark"]
