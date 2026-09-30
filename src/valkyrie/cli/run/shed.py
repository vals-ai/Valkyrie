"""Lower a run's concurrency and force stop the newest tasks above the new limit."""

from uuid import UUID

import click

from valkyrie.cli.display import terminal_safe
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    help=(
        "Lower concurrency for an active run, then force stop the newest building or in-progress tasks until the "
        "run is back at the new limit. Evaluating tasks are never stopped. \n\n"
        "Example:\nvalkyrie run shed 123e4567-e89b-12d3-a456-426614174000 --concurrency 10 --dry-run"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    required=True,
    help="New maximum number of concurrent tasks; must be below the run's current limit",
)
@click.option("--dry-run", is_flag=True, help="Show the tasks that would be stopped without changing the run")
def shed(run_id: UUID, concurrency: int, dry_run: bool) -> None:
    """Lower concurrency, then force stop the newest tasks above the new limit."""
    try:
        with TrackerService() as tracker:
            planned = tracker.shed_benchmark(run_id, concurrency, dry_run=True).task_ids
            _print_tasks(planned, "Would force stop")
            if dry_run:
                return
            if not click.confirm(
                f"Lower run {run_id} concurrency to {concurrency} and force stop the {len(planned)} newest task(s)?"
            ):
                click.echo("Cancelled.")
                return

            response = tracker.shed_benchmark(run_id, concurrency, dry_run=False)
        click.echo(click.style(f"✓ Run concurrency updated to {response.concurrency}.", fg="green", bold=True))
        _print_tasks(response.task_ids, "Force stopped")
    except TrackerServiceError as error:
        raise click.ClickException(str(error)) from error


def _print_tasks(task_ids: list[str], action: str) -> None:
    if not task_ids:
        click.echo("No building or in-progress tasks are above the limit.")
        return
    click.echo(f"{action} {len(task_ids)} task(s):")
    for task_id in task_ids:
        click.echo(f"  {terminal_safe(task_id, preserve_newlines=False)}")
