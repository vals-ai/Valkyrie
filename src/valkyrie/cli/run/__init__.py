import click

from valkyrie.cli.run.analyze import analyze
from valkyrie.cli.run.errors import errors
from valkyrie.cli.run.filter_options import filter_options
from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.list_runs import list_runs
from valkyrie.cli.run.logs import logs
from valkyrie.cli.run.outputs import artifacts, output_path, outputs
from valkyrie.cli.run.results import results
from valkyrie.cli.run.resume import resume, retry_command
from valkyrie.cli.run.start import start
from valkyrie.cli.run.status import status_runs
from valkyrie.cli.run.stop import stop
from valkyrie.cli.run.update import update
from valkyrie.cli.run.tasks import tasks, task, task_artifacts


@click.group()
def run():
    """Run command group"""
    pass


run.add_command(analyze)
run.add_command(errors)
run.add_command(fetch)
run.add_command(filter_options)
run.add_command(list_runs)
run.add_command(logs)
run.add_command(artifacts)
run.add_command(output_path)
run.add_command(outputs)
run.add_command(results)
run.add_command(resume)
run.add_command(retry_command)
run.add_command(start)
run.add_command(status_runs)
run.add_command(stop)
run.add_command(update)
run.add_command(tasks)
run.add_command(task)
run.add_command(task_artifacts)

__all__ = [
    "list_runs",
    "run",
    "start",
    "status_runs",
]
