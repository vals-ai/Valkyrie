"""CLI views/commands for the agentic harness."""

from pathlib import Path
from uuid import UUID

import click

import cli.contract_bundler as bundler
from cli.contract_bundler import BundlerError
from cli.tracker_service import TrackerService, TrackerServiceError
from cli.utils import (
    check_tracker_service_health,
    format_benchmark_status,
    format_start_run_response,
    stream_benchmark_status,
)
from services.tracker.src.tracker.types import StartRunResponse


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
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908)",
)
@click.option(
    "--slice",
    "slice_str",
    type=str,
    required=False,
    default=None,
    help="Slice string to use for slicing the benchmark (e.g., 1-10)",
)
def start_benchmark(
    contract: Path,
    benchmark: str,
    concurrency: int,
    task_ids: str | None,
    slice_str: str | None,
):
    """
    Run an agent on a benchmark.

    Example:
        harness run --contract contracts/claude_code --benchmark swebench
    """
    click.echo("Arguments:")
    click.echo(f"  - Benchmark: {benchmark}")
    click.echo(f"  - Contract: {contract}")
    click.echo(f"  - Concurrency: {concurrency}")
    click.echo(f"  - Slice: {slice_str}")
    if task_ids:
        click.echo(f"  - Task IDs: {task_ids[:100]}{'...' if len(task_ids) > 100 else ''}")
    else:
        click.echo("  - Task IDs: all tasks")

    formatted_task_ids: list[str] | None = None
    if task_ids:
        formatted_task_ids = task_ids.split(",")
        click.echo(f"Discovered {len(formatted_task_ids)} task IDs")

    click.echo("Validating contract...", nl=False)
    bundler.validate_contract(contract)

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            click.echo("\r\033[KZipping bundle...", nl=False)

            with bundler.create_contract_bundle_stream(contract) as file_stream:
                click.echo("\r\033[KUploading bundle to tracker service...", nl=False)
                tracker.upload_contract(contract.name, file_stream)

            click.echo(f"\r\033[KStarting benchmark for: {contract.name}...", nl=False)
            response = tracker.start_run(contract.name, benchmark, concurrency, formatted_task_ids, slice_str)

            click.echo("\r\033[K", nl=False)
            if response.status_code != 200:
                click.echo(click.style("Benchmark failed to start!", fg="red", bold=True))
                click.echo(response.text)
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
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream benchmark updates",
)
def fetch_benchmark(benchmark_id: UUID, connect: bool):
    """
    Fetch a benchmark by its benchmark id.

    Example:
        harness fetch-benchmark --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --connect
    """

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if connect:
                stream_benchmark_status(tracker, benchmark_id)
            else:
                click.echo(f"Fetching benchmark: {benchmark_id}")
                response = tracker.fetch_benchmark(benchmark_id)
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
        harness retrieve-results --benchmark-id e532551e-d51b-4912-983d-47695bd24174 --path ./results.json
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


@cli.command()
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
def stop_run(benchmark_id: UUID):
    """
    Stop a benchmark run by its benchmark id.

    Example:
        harness stop-run --benchmark-id 123e4567-e89b-12d3-a456-426614174000
    """
    click.echo(f"Stopping run for benchmark: {benchmark_id}")
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            _ = tracker.stop_run(benchmark_id)
            click.echo(
                click.style(
                    "Run is currently being stopped. Will be stopped when all tasks in flight are finished.",
                    fg="yellow",
                    bold=True,
                )
            )
            click.echo(
                click.style(
                    f"Retrieve results: harness retrieve-results --benchmark-id {benchmark_id} --path ./results.json",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
def resume_run(benchmark_id: UUID):
    """
    Resume a benchmark run by its benchmark id.

    Example:
        harness resume-run --benchmark-id 123e4567-e89b-12d3-a456-426614174000
    """
    click.echo(f"Resuming run for benchmark: {benchmark_id}")
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            _ = tracker.resume_run(benchmark_id)
            click.echo(click.style("Run resumed successfully!", fg="green", bold=True))
            click.echo(
                click.style(
                    f"Track progress: harness fetch-benchmark --benchmark-id {benchmark_id} --connect",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
