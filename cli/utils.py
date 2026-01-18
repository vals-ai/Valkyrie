import json
from uuid import UUID

import click
from tracker.types import FetchBenchmarkResponse, StartRunResponse

from cli.main import TrackerService


class BenchmarkFormatter:
    @staticmethod
    def get_status_color(status_value: str) -> str:
        status_colors = {
            "in_progress": "blue",
            "stopping": "magenta",
            "stopped": "cyan",
            "finished": "cyan",
            "error": "red",
        }
        return status_colors.get(status_value, "white")

    @staticmethod
    def create_progress_bar(finished_tasks: int, total_tasks: int, bar_width: int = 30) -> tuple[str, float]:
        """
        Create a progress bar string and percentage.

        Returns:
            tuple of (bar string, progress percentage)
        """
        progress_pct = (finished_tasks / total_tasks * 100) if total_tasks > 0 else 0
        filled_width = int(bar_width * progress_pct / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        return bar, progress_pct


def check_tracker_service_health(tracker: TrackerService) -> bool:
    """
    Re-usable utility to check the health of the tracker service.

    Args:
        tracker: TrackerService instance

    Returns:
        bool: True if the tracker service is healthy, False otherwise
    """
    response = tracker.health_check()
    if response.status_code != 200:
        click.echo(click.style("Tracker service failed to respond!", fg="red", bold=True))
        click.echo(json.dumps(response.json(), indent=4, default=str))
        return False

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

    bar, progress_pct = BenchmarkFormatter.create_progress_bar(finished_tasks, total_tasks)
    status_color = BenchmarkFormatter.get_status_color(status.value)

    click.echo(f"{click.style('Benchmark:', bold=True)} {benchmark_name}")
    click.echo(f"{click.style('Started at:', bold=True)} {started_at}")
    click.echo(f"{click.style('Benchmark ID:', bold=True)} {benchmark_id}")
    click.echo()

    status_text = click.style(status.value.replace("_", " ").title(), fg=status_color, bold=True)
    click.echo(f"[{bar}] {finished_tasks}/{total_tasks} ({progress_pct:.1f}%) • {status_text}")


def format_start_run_response(start_run_response: StartRunResponse) -> None:
    """
    Format and display the start run response.

    Args:
        data: Dictionary containing StartRunResponse data
    """

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
            f"Track progress: harness fetch-benchmark --benchmark-id {start_run_response.benchmark_id} --connect",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Retrieve results: harness retrieve-results --benchmark-id {start_run_response.benchmark_id} --path ./results.json",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Stop run: harness stop-run --benchmark-id {start_run_response.benchmark_id}",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Resume run: harness resume-run --benchmark-id {start_run_response.benchmark_id}",
            fg="cyan",
        )
    )
    click.echo()


def stream_benchmark_status(tracker: TrackerService, benchmark_id: UUID) -> None:
    """
    Stream and display live benchmark status updates.

    Args:
        tracker: TrackerService instance
        benchmark_id: Benchmark UUID to stream
    """
    click.echo(click.style("Streaming benchmark updates (Ctrl+C to stop)...\n", fg="cyan"))

    try:
        for event in tracker.stream_benchmark(benchmark_id):
            if event.startswith("data:"):
                data_json = event[5:].strip()
                if not data_json:
                    continue

                response = FetchBenchmarkResponse.model_validate_json(data_json)
                details = response.details

                bar, progress_pct = BenchmarkFormatter.create_progress_bar(details.finished_tasks, details.total_tasks)
                status_color = BenchmarkFormatter.get_status_color(details.status.value)
                status_text = click.style(details.status.value.replace("_", " ").title(), fg=status_color, bold=True)

                click.echo(
                    f"\r\033[K[{bar}] {details.finished_tasks}/{details.total_tasks} ({progress_pct:.1f}%) • {status_text}",
                    nl=False,
                )

            elif event.startswith("event: complete"):
                click.echo("\n")
                click.echo(click.style("✓ Benchmark completed!", fg="green", bold=True))
                break

            elif event.startswith("event: error"):
                click.echo("\n")
                click.echo(click.style("✗ Error occurred while streaming", fg="red", bold=True))
                break

            elif event.startswith("event: disconnect"):
                click.echo("\n")
                click.echo(click.style("Disconnected from stream", fg="yellow"))
                break

    except KeyboardInterrupt:
        click.echo("\n")
        click.echo(click.style("Stopped streaming", fg="yellow"))
