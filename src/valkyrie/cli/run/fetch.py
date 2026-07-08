from uuid import UUID

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_health import check_tracker_service_health
from valkyrie.cli.run.progress import format_benchmark_status, stream_benchmark_status
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    help="Fetch a run by its run id. \n\nExample:\nvalkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates",
)
def fetch(run_id: UUID, connect: bool):
    """
    Fetch a run by its run id.

    Example:
        valkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect
    """

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if connect:
                stream_benchmark_status(tracker, run_id)
            else:
                response = tracker.fetch_benchmark(run_id)
                format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))
