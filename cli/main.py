"""CLI views/commands for the agentic harness."""

import json
from pathlib import Path
from uuid import UUID

import click
from tracker.types import StartRunResponse

import cli.contract_bundler as bundler
from cli.contract_bundler import BundlerError
from cli.tracker_service import TrackerService, TrackerServiceError
from cli.utils import check_tracker_service_health, format_benchmark_status, format_start_run_response


@click.group()
def cli():
    """Agentic Harness CLI."""
    pass


@cli.command()
@click.option(
    "--contract",
    type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True),
    required=True,
    help="Path to contract directory (e.g., contracts/claude_code)",
)
@click.option(
    "--benchmark",
    type=str,
    required=True,
    help="Name of the benchmark to run (e.g., swebench, finance)",
)
@click.option(
    "--concurrency",
    type=int,
    default=5,
    required=False,
    help="Number of concurrent tasks to run (e.g., 5)",
)
@click.option(
    "--task-ids",
    type=str | None,
    required=False,
    default=None,
    help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908)",
)
def start_benchmark(
    contract: Path,
    benchmark: str,
    concurrency: int,
    task_ids: str | None,
):
    """
    Run an agent on a benchmark.

    Example:
        harness run --contract contracts/claude_code --benchmark swebench
    """
    click.echo(f"Running benchmark: {benchmark}")
    click.echo(f"Contract: {contract}")
    click.echo(f"Concurrency: {concurrency}")

    formatted_task_ids: list[str] | None = None
    if task_ids:
        formatted_task_ids = task_ids.split(",")
        click.echo(f"Discovered {len(formatted_task_ids)} task IDs")

    click.echo()

    click.echo("Validating contract...")
    bundler.validate_contract(contract)

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            click.echo(f"Creating contract bundle for: {contract}")
            click.echo("Zipping bundle...")

            with bundler.create_contract_bundle_stream(contract) as file_stream:
                click.echo("Uploading bundle to tracker service...")
                tracker.upload_contract(contract.name, file_stream)

            click.echo(f"Starting benchmark for: {contract.name}")
            response = tracker.start_run(contract.name, benchmark, concurrency, formatted_task_ids)

            if response.status_code != 200:
                click.echo(click.style("Benchmark failed to start!", fg="red", bold=True))
                click.echo(json.dumps(response.json(), indent=4, default=str))
                return

            format_start_run_response(StartRunResponse.model_validate(response.json()))
    except (BundlerError, TrackerServiceError) as e:
        raise click.ClickException(str(e))


@cli.command()
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
def fetch_benchmark(benchmark_id: UUID):
    """
    Fetch a benchmark by its benchmark id.

    Example:
        harness fetch-benchmark --benchmark-id 123e4567-e89b-12d3-a456-426614174000
    """
    click.echo(f"Fetching benchmark: {benchmark_id}")

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            response = tracker.fetch_benchmark(benchmark_id)

            click.echo(click.style("Benchmark fetched successfully!", fg="green", bold=True))

            format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=True,
    help="Path to save the results (e.g., ./results.json)",
)
def retrieve_results(benchmark_id: UUID, path: Path):
    """
    Retrieve the results of a benchmark by its benchmark id.

    Example:
        harness retrieve-results --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --path ./results.json
    """
    click.echo(f"Retrieving results for benchmark: {benchmark_id}")

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            results_response = tracker.retrieve_results(benchmark_id)
            click.echo(click.style("Results retrieved successfully!", fg="green", bold=True))

            if not path.parent.exists():
                raise click.ClickException(f"'{path.parent}' directory does not exist! Please create it first.")

            if path.exists():
                if not click.confirm(f"File '{path}' already exists. Overwrite?"):
                    raise click.Abort()

            with open(path, "w") as f:
                f.write(results_response.model_dump_json(indent=4))

            click.echo(f"Results saved to '{path}'")
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
