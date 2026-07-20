"""Update live concurrency for an active benchmark run."""

from uuid import UUID

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    help=(
        "Update concurrency for an active run. \n\n"
        "Example:\nvalkyrie run update 123e4567-e89b-12d3-a456-426614174000 --concurrency 10"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    required=True,
    help="New maximum number of concurrent tasks",
)
def update(run_id: UUID, concurrency: int) -> None:
    """Update the concurrency limit for an active run."""
    try:
        with TrackerService() as tracker:
            response = tracker.update_benchmark_concurrency(run_id, concurrency)
        click.echo(click.style(f"✓ Run concurrency updated to {response.concurrency}.", fg="green", bold=True))
    except TrackerServiceError as e:
        raise click.ClickException(str(e)) from e
