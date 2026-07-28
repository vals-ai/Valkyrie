import click

from valkyrie.cli.run.analyze import analyze
from valkyrie.cli.run.errors import errors
from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.list_runs import list_runs
from valkyrie.cli.run.outputs import output_path, outputs
from valkyrie.cli.run.results import results
from valkyrie.cli.run.resume import resume, retry_command
from valkyrie.cli.run.start import start
from valkyrie.cli.run.status import status_runs
from valkyrie.cli.run.stop import stop
from valkyrie.cli.run.update import update


@click.group()
def run():
    """Run command group"""
    pass


run.add_command(analyze)
run.add_command(errors)
run.add_command(fetch)
run.add_command(list_runs)
run.add_command(output_path)
run.add_command(outputs)
run.add_command(results)
run.add_command(resume)
run.add_command(retry_command)
run.add_command(start)
run.add_command(status_runs)
run.add_command(stop)
run.add_command(update)

__all__ = [
    "list_runs",
    "run",
    "start",
    "status_runs",
]
