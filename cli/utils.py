import json

import click
from tracker.types import FetchBenchmarkResponse, StartRunResponse

from cli.main import TrackerService


def check_tracker_service_health(tracker: TrackerService) -> bool:
    """
    Re-usable utility to check the health of the tracker service.

    Args:
        tracker: TrackerService instance

    Returns:
        bool: True if the tracker service is healthy, False otherwise
    """
    click.echo("\nChecking tracker service health...")
    response = tracker.health_check()
    if response.status_code != 200:
        click.echo(click.style("Tracker service failed to respond!", fg="red", bold=True))
        click.echo(json.dumps(response.json(), indent=4, default=str))
        return False

    click.echo(click.style("Tracker service health check successful!", fg="green", bold=True))
    click.echo(json.dumps(response.json(), indent=4, default=str))

    return True


def format_benchmark_status(benchmark_response: FetchBenchmarkResponse) -> None:
    """
    Format and display benchmark status with a progress bar.

    Args:
        benchmark_response: FetchBenchmarkResponse
    """
    benchmark_name = benchmark_response.benchmark_name
    benchmark_id = benchmark_response.benchmark_id
    details = benchmark_response.details

    status = details.status
    started_at = details.started_at
    total_tasks = details.total_tasks
    finished_tasks = details.finished_tasks

    progress_pct = (finished_tasks / total_tasks * 100) if total_tasks > 0 else 0

    status_colors = {
        "RUNNING": "blue",
        "COMPLETED": "green",
        "FAILED": "red",
        "PENDING": "yellow",
    }
    status_color = status_colors.get(status, "white")

    bar_width = 30
    filled_width = int(bar_width * progress_pct / 100)
    bar = "█" * filled_width + "░" * (bar_width - filled_width)

    click.echo(f"\n{click.style('Benchmark:', bold=True)} {benchmark_name}")
    click.echo(f"{click.style('Started at:', bold=True)} {started_at}")
    click.echo(f"{click.style('Benchmark ID:', bold=True)} {benchmark_id}")
    click.echo()

    status_text = click.style(status, fg=status_color, bold=True)
    click.echo(f"[{bar}] {finished_tasks}/{total_tasks} ({progress_pct:.1f}%) • {status_text}")


def format_start_run_response(start_run_response: StartRunResponse) -> None:
    """
    Format and display the start run response.

    Args:
        data: Dictionary containing StartRunResponse data
    """

    click.echo()
    click.echo(click.style("✓ Benchmark started successfully!", fg="green", bold=True))
    click.echo()
    click.echo("┌─ Benchmark Details " + "─" * 58)
    click.echo(f"│ Benchmark:     {start_run_response.benchmark_name}")
    click.echo(f"│ Contract:      {start_run_response.contract_name}")
    click.echo(f"│ Benchmark ID:  {start_run_response.benchmark_id}")
    click.echo(f"│ Started at:    {start_run_response.started_at}")
    click.echo(f"│ Concurrency:   {start_run_response.concurrency}")
    click.echo(f"│ Total tasks:   {start_run_response.task_count}")
    click.echo("└" + "─" * 79)
    click.echo()
    click.echo(
        click.style(
            f"Track progress: harness fetch-benchmark --benchmark-id {start_run_response.benchmark_id}", fg="cyan"
        )
    )
