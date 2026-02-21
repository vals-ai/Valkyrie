"""Utility functions for the CLI."""

import json
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import UUID

import click
from httpx import Response
from tracker.database.models import BenchmarkStatus, TaskStatus
from tracker.types import (
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    Order,
    StartBenchmarkResponse,
)

from agentic_harness.cli.tracker_service import TrackerService


def local_time(dt: datetime) -> str:
    """Convert UTC time to users local time"""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


class BenchmarkFormatter:
    STATUS_COLORS = {
        "PENDING": "yellow",
        "BUILDING": "cyan",
        "IN_PROGRESS": "blue",
        "EVALUATING": "magenta",
        "STOPPED": "cyan",
        "STOPPING": "magenta",
        "FINISHED": "green",
        "ERROR": "red",
    }

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

    @staticmethod
    def format_task_breakdown(task_breakdown: dict[TaskStatus, int]) -> str:
        """
        Format task breakdown with colored status counts.

        Returns:
            Formatted string with colored task counts
        """

        # Order we display statuses in
        status_order = [
            TaskStatus.PENDING,
            TaskStatus.BUILDING,
            TaskStatus.IN_PROGRESS,
            TaskStatus.EVALUATING,
            TaskStatus.ERROR,
            TaskStatus.STOPPED,
            TaskStatus.FINISHED,
        ]

        parts: list[str] = []
        for status in status_order:
            count = task_breakdown.get(status, 0)

            if count == 0:
                continue

            color = BenchmarkFormatter.STATUS_COLORS[status]
            label = status.value.replace("_", " ").title()
            colored_part = click.style(f"{label}: {count}", fg=color)
            parts.append(colored_part)

        return f"│ {' │ '.join(parts)} │" if parts else ""


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
    status_color = BenchmarkFormatter.STATUS_COLORS[status.value]

    click.echo(f"{click.style('Benchmark:', bold=True)} {benchmark_name}")
    click.echo(f"{click.style('Started at:', bold=True)} {local_time(started_at)}")
    click.echo(f"{click.style('Benchmark ID:', bold=True)} {benchmark_id}")
    click.echo()

    status_text = click.style(status.value.replace("_", " ").title(), fg=status_color, bold=True)
    click.echo(f"[{bar}] {finished_tasks}/{total_tasks} ({progress_pct:.1f}%) • {status_text}")

    breakdown_text = BenchmarkFormatter.format_task_breakdown(details.task_breakdown)
    click.echo(breakdown_text)


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse) -> None:
    """
    Format and display the start run response.

    Args:
        start_benchmark_response: StartBenchmarkResponse
    """

    click.echo()
    click.echo("┌─ Benchmark Details " + "─" * 58)
    click.echo(f"│ Benchmark:     {start_benchmark_response.benchmark_name}")
    click.echo(f"│ Agent:      {start_benchmark_response.agent_name}")
    click.echo(f"│ Benchmark ID:  {start_benchmark_response.benchmark_id}")
    click.echo(f"│ Started at:    {local_time(start_benchmark_response.started_at)}")
    click.echo(f"│ Max concurrency:   {start_benchmark_response.concurrency}")
    click.echo(f"│ Total tasks:   {start_benchmark_response.task_count}")
    click.echo(f"│ CloudWatch:    {start_benchmark_response.cloudwatch_url}")
    click.echo("└" + "─" * 79)
    click.echo()
    click.echo(
        click.style(
            f"Track progress: harness benchmark fetch --benchmark-id {start_benchmark_response.benchmark_id} --connect",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Retrieve results: harness benchmark results --benchmark-id {start_benchmark_response.benchmark_id} --path ./results.json",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Stop benchmark: harness benchmark stop --benchmark-id {start_benchmark_response.benchmark_id}",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Resume benchmark: harness benchmark resume --benchmark-id {start_benchmark_response.benchmark_id}",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Retry benchmark: harness benchmark retry --benchmark-id {start_benchmark_response.benchmark_id}",
            fg="cyan",
        )
    )
    click.echo(
        click.style(
            f"Fetch agent outputs: harness agent outputs --benchmark-id {start_benchmark_response.benchmark_id} --output-dir .",
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
    click.echo(click.style("Streaming benchmark updates (Ctrl+C to stop)...", fg="cyan"))
    click.echo()

    try:
        for event in tracker.stream_benchmark(benchmark_id):
            if event.startswith("data:"):
                data_json = event[5:].strip()
                if not data_json:
                    continue

                response = FetchBenchmarkResponse.model_validate_json(data_json)
                details = response.details

                bar, progress_pct = BenchmarkFormatter.create_progress_bar(details.finished_tasks, details.total_tasks)
                status_color = BenchmarkFormatter.STATUS_COLORS[details.status.value]
                status_text = click.style(details.status.value.replace("_", " ").title(), fg=status_color, bold=True)

                progress_line = (
                    f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
                )
                breakdown_text = BenchmarkFormatter.format_task_breakdown(details.task_breakdown)

                click.echo(f"\033[F\033[K{progress_line}\n\033[K{breakdown_text}", nl=False)

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
    agent_width = max(len("Agent"), max(len(benchmark.agent_name) for benchmark in benchmarks))
    status_width = max(len("Status"), max(len(benchmark.status.value) for benchmark in benchmarks))
    progress_width = 8

    header_line = (
        f"{'ID':<{id_width}}  "
        f"{'Benchmark':<{name_width}}  "
        f"{'Agent':<{agent_width}}  "
        f"{'Status':<{status_width}}  "
        f"{'Progress':>{progress_width}}"
    )
    separator = "─" * len(header_line)

    click.echo()
    click.echo(click.style(header_line, bold=True))
    click.echo(separator)

    for benchmark in benchmarks:
        _, progress_percentage = BenchmarkFormatter.create_progress_bar(benchmark.finished_tasks, benchmark.total_tasks)
        status_color = BenchmarkFormatter.STATUS_COLORS[benchmark.status.value]
        status_display = benchmark.status.value.replace("_", " ").title()

        click.echo(
            f"{str(benchmark.id):<{id_width}}  "
            f"{benchmark.name:<{name_width}}  "
            f"{benchmark.agent_name:<{agent_width}}  "
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


def format_no_benchmarks_found(agent_name: str | None, benchmark_name: str | None, status: str | None) -> None:
    """
    Handle the case where no benchmarks are found matching the specified filters.

    Args:
        agent_name: Agent name filter
        benchmark_name: Benchmark name filter
        status: Status filter
    """
    click.echo()
    click.echo(click.style("No benchmarks found matching the specified filters.", fg="yellow"))
    click.echo()
    if any([agent_name, benchmark_name, status]):
        click.echo("Filters applied:")
        if agent_name:
            click.echo(f"  • Agent: {agent_name}")
        if benchmark_name:
            click.echo(f"  • Benchmark: {benchmark_name}")
        if status:
            click.echo(f"  • Status: {status}")


def paginate_benchmarks(
    tracker: TrackerService,
    agent_name: str | None,
    benchmark_name: str | None,
    status: str | None,
    order_by: str,
    limit: int = 5,
) -> None:
    """
    Interactive paginated display of benchmarks with vim-style navigation.

    Args:
        tracker: TrackerService instance
        agent_name: Optional agent name filter
        benchmark_name: Optional benchmark name filter
        status: Optional status filter
        order_by: Order (asc/desc)
        limit: Number of items per page
    """
    current_page = 1
    offset = 0

    while True:
        request = FetchBenchmarksRequest(
            agent_name=agent_name,
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
            format_no_benchmarks_found(agent_name, benchmark_name, status)
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


def download_agent_outputs(agent_outputs_response: Response, output_dir: Path) -> None:
    """
    Download agent outputs from a response and extract them to a directory.

    Args:
        agent_outputs_response: Response with agent outputs
        output_dir: Directory to save agent outputs
    """

    # Create the output directory if it doesn't exist
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Download the agent outputs to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
        click.echo("\r\033[KDownloading...", nl=False)

        for chunk in agent_outputs_response.iter_bytes():
            tmp_file.write(chunk)

    click.echo(f"\r\033[KExtracting archives to {output_dir}...", nl=False)

    # Extract the agent outputs to the output directory
    with tarfile.open(tmp_path, "r") as tar:
        tar.extractall(output_dir)

    # Unpack any nested tar.gz files
    nested_tars = list(output_dir.rglob("*.tar.gz"))
    if nested_tars:
        click.echo(f"\r\033[KUnpacking {len(nested_tars)} nested tar.gz files...", nl=False)

        for nested_tar in nested_tars:
            extract_dir = nested_tar.parent / nested_tar.stem.replace(".tar", "")
            extract_dir.mkdir(parents=True, exist_ok=True)

            with tarfile.open(nested_tar, "r:gz") as tar:
                tar.extractall(extract_dir)

            nested_tar.unlink()

    tmp_path.unlink()


def download_final_view(path: Path, final_view: FinalViewResponse) -> None:
    if not path.parent.exists():
        raise click.ClickException(f"'{path.parent}' directory does not exist! Please create it first.")

    if path.exists():
        if not click.confirm(f"File '{path}' already exists. Overwrite?"):
            raise click.Abort()

    with open(path, "w") as f:
        f.write(
            final_view.model_dump_json(
                indent=4,
                exclude_none=True,
                exclude={"benchmark_arguments": {"contract": {"env"}}},
            )
        )

    click.echo(click.style(f"View the  '{path}'", fg="green", bold=True))
