from uuid import UUID

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.progress import format_benchmark_status, stream_benchmark_status
from valkyrie.cli.run.snapshot import fetch_run_metadata, format_run_snapshot_json
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
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json", "jsonl"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. Use json for a snapshot or jsonl with --connect.",
)
def fetch(run_id: UUID, connect: bool, output_format: str):
    """
    Fetch a run by its run id.

    Example:
        valkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect
    """

    if connect and output_format == "json":
        raise click.UsageError("--format json cannot be used with --connect; use --format jsonl.")
    if not connect and output_format == "jsonl":
        raise click.UsageError("--format jsonl requires --connect.")

    try:
        with TrackerService() as tracker:
            if connect:
                if output_format == "jsonl":
                    stream_benchmark_status(tracker, run_id, output_format="jsonl")
                else:
                    stream_benchmark_status(tracker, run_id, show_identity=True)
            else:
                response = tracker.fetch_benchmark(run_id)
                if output_format == "json":
                    metadata = fetch_run_metadata(tracker, run_id)
                    click.echo(format_run_snapshot_json(response, metadata, event="snapshot"))
                else:
                    format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))
