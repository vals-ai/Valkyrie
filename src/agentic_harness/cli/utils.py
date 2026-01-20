"""Utility functions for the CLI."""

import json
from uuid import UUID

import click
from tracker.database.models import BenchmarkStatus
from tracker.types import (
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    Order,
    StartRunResponse,
)

from agentic_harness.cli.tracker_service import TrackerService


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


def format_fetch_benchmarks_response(
    fetch_benchmarks_response: FetchBenchmarksResponse,
    current_page: int = 1,
    total_pages: int = 1,
    show_nav: bool = False,
) -> None:
    """
    Format and display benchmarks in a table format.
    NOTE: I'm not an artist (This was created using opus)

    Args:
        fetch_benchmarks_response: FetchBenchmarksResponse containing list of benchmarks
        current_page: Current page number (1-indexed)
        total_pages: Total number of pages
        show_nav: Whether to show navigation hints
    """
    benchmarks = fetch_benchmarks_response.benchmarks

    if not benchmarks:
        click.echo(click.style("No benchmarks found.", fg="yellow"))
        return

    id_width = 36
    name_width = max(len("Benchmark"), max(len(benchmark.name) for benchmark in benchmarks))
    contract_width = max(len("Contract"), max(len(benchmark.contract_name) for benchmark in benchmarks))
    status_width = max(len("Status"), max(len(benchmark.status.value) for benchmark in benchmarks))
    progress_width = 8

    header_line = (
        f"{'ID':<{id_width}}  "
        f"{'Benchmark':<{name_width}}  "
        f"{'Contract':<{contract_width}}  "
        f"{'Status':<{status_width}}  "
        f"{'Progress':>{progress_width}}"
    )
    separator = "─" * len(header_line)

    click.echo()
    click.echo(click.style(header_line, bold=True))
    click.echo(separator)

    for benchmark in benchmarks:
        _, progress_percentage = BenchmarkFormatter.create_progress_bar(benchmark.finished_tasks, benchmark.total_tasks)
        status_color = BenchmarkFormatter.get_status_color(benchmark.status.value)
        status_display = benchmark.status.value.replace("_", " ").title()

        click.echo(
            f"{str(benchmark.id):<{id_width}}  "
            f"{benchmark.name:<{name_width}}  "
            f"{benchmark.contract_name:<{contract_width}}  "
            f"{click.style(status_display, fg=status_color):<{status_width + 9}}  "
            f"{progress_percentage:>{progress_width}.1f}%"
        )

    click.echo(separator)

    total_text = f"Total: {fetch_benchmarks_response.total_count} benchmark(s)"
    if show_nav and total_pages > 1:
        nav_text = click.style(f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit", fg="bright_black")
        padding = (
            len(separator) - len(total_text) - len(f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit")
        )
        click.echo(f"{total_text}{' ' * padding}{nav_text}")
    else:
        click.echo(total_text)


def format_no_benchmarks_found(contract_name: str | None, benchmark_name: str | None, status: str | None) -> None:
    """
    Handle the case where no benchmarks are found matching the specified filters.

    Args:
        contract_name: Contract name filter
        benchmark_name: Benchmark name filter
        status: Status filter
    """
    click.echo()
    click.echo(click.style("No benchmarks found matching the specified filters.", fg="yellow"))
    click.echo()
    if any([contract_name, benchmark_name, status]):
        click.echo("Filters applied:")
        if contract_name:
            click.echo(f"  • Contract: {contract_name}")
        if benchmark_name:
            click.echo(f"  • Benchmark: {benchmark_name}")
        if status:
            click.echo(f"  • Status: {status}")


def paginate_benchmarks(
    tracker: TrackerService,
    contract_name: str | None,
    benchmark_name: str | None,
    status: str | None,
    order_by: str,
    limit: int = 5,
) -> None:
    """
    Interactive paginated display of benchmarks with vim-style navigation.

    Args:
        tracker: TrackerService instance
        contract_name: Optional contract name filter
        benchmark_name: Optional benchmark name filter
        status: Optional status filter
        order_by: Order (asc/desc)
        limit: Number of items per page
    """
    current_page = 1
    offset = 0

    while True:
        request = FetchBenchmarksRequest(
            contract_name=contract_name,
            benchmark_name=benchmark_name,
            status=BenchmarkStatus(status) if status else None,
            order_by=Order(order_by),
            limit=limit,
            offset=offset,
        )

        response = tracker.fetch_benchmarks(request)
        total_count = response.total_count
        total_pages = max(1, (total_count + limit - 1) // limit)

        click.clear()

        if total_count == 0:
            format_no_benchmarks_found(contract_name, benchmark_name, status)
            break

        format_fetch_benchmarks_response(response, current_page, total_pages, show_nav=True)

        if total_pages <= 1:
            break

        char = click.getchar()

        if char == "l" and current_page < total_pages:
            current_page += 1
            offset += limit
        elif char == "h" and current_page > 1:
            current_page -= 1
            offset -= limit
        elif char == "q" or char == "\x03":
            break
