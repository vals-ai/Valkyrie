from pathlib import Path

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_health import check_tracker_service_health
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    name="tasks",
    help="Save task IDs for a benchmark dataset. \n\nExample:\nvalkyrie benchmark tasks swebench --dataset default",
)
@click.argument("benchmark_name", type=str)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Dataset name to fetch from the benchmark service (defaults to 'default')",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential)",
)
@click.option(
    "--ignore-custom-services",
    "--ics",
    is_flag=True,
    help="Ignore custom benchmark services that have been configured. Provides opt-out for custom services.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=False,
    default=None,
    help="Path to save task IDs, one per line. Defaults to <benchmark>-<dataset>-tasks.txt.",
)
def tasks(
    benchmark_name: str,
    dataset: str | None,
    headers: tuple[tuple[str, str]],
    ignore_custom_services: bool,
    output: Path | None,
):
    """
    Save task IDs for a benchmark dataset.
    """
    service_headers: dict[str, str] = {}
    auth_credential = TrackerService.get_benchmark_auth(benchmark_name)
    if auth_credential:
        service_headers["Authorization"] = str(auth_credential)
    for name, value in headers:
        service_headers[name] = value

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            response = tracker.fetch_benchmark_tasks(
                benchmark_name,
                dataset=dataset,
                ignore_custom_services=ignore_custom_services,
                service_headers=service_headers,
            )
            output_path = output or Path(f"{benchmark_name}-{dataset or 'default'}-tasks.txt")
            output_path.write_text("\n".join(response) + "\n", encoding="utf-8")
            click.echo(f"Wrote {len(response)} task IDs to {output_path}")
    except TrackerServiceError as e:
        raise click.ClickException(str(e))
