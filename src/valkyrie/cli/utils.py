"""Utility functions for the CLI."""

import asyncio
import json
import shutil
import tarfile
import tempfile
import urllib.request
from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Coroutine, TypeVar
from urllib.parse import urlparse
from uuid import UUID

import click
import yaml
from httpx import Response
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, TaskStatus
from tracker.types import (
    BenchmarkDetails,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    Order,
    StartBenchmarkResponse,
)

from valkyrie.cli.tracker_service import TrackerService

CONFIG_LOCATION: Path = Path("~/.config/valkyrie/valkyrie.yaml").expanduser()

T = TypeVar("T")


class ConfigValue(str, Enum):
    API_KEY = "api_key"
    SLACK_WEBHOOK_SECRET = "webhook"
    AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
    AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"
    S3_BUCKET = "S3_BUCKET"
    SANDBOX_PROVIDER_SECRET_NAME = "SANDBOX_PROVIDER_SECRET_NAME"
    LOG_GROUP = "LOG_GROUP"
    LOG_RETENTION_POLICY = "LOG_RETENTION_POLICY"

    @classmethod
    def from_str(cls, key: str) -> "ConfigValue":
        """Convert string value to enum value, raising if value is not an option."""
        for member in cls:
            if member.value.lower() == key.lower():
                return member

        raise ValueError(f"Invalid config key: {key!r}")


async def run_with_spinner(coro: Coroutine[Any, Any, T], message: str) -> T:
    """Run an async coroutine with an animated spinner.

    Args:
        coro: The coroutine to run
        message: The message to display with the spinner

    Returns:
        The result of the coroutine
    """
    spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    frame_index = 0

    # Truncate message to fit terminal width (leaving room for spinner: 2 chars)
    max_width = shutil.get_terminal_size().columns - 2
    display_message = message if len(message) <= max_width else message[: max_width - 1] + "…"

    async def show_spinner() -> None:
        """Show animated spinner until task completes."""
        nonlocal frame_index
        while not task.done():
            frame = spinner_frames[frame_index % len(spinner_frames)]
            click.echo(f"\r{frame} {display_message}", nl=False)
            frame_index += 1
            await asyncio.sleep(0.1)
        # Clear the line when done
        click.echo("\r\033[K", nl=False)

    task = asyncio.create_task(coro)
    spinner_task = asyncio.create_task(show_spinner())

    try:
        result = await task
    finally:
        # Immediately cancel spinner and clear the line
        spinner_task.cancel()
        try:
            await spinner_task
        except asyncio.CancelledError:
            # Clear the line when spinner is cancelled
            click.echo("\r\033[K", nl=False)

    return result


def _clean_task_ids(task_ids: Iterable[str]) -> list[str]:
    cleaned = [task_id.strip() for task_id in task_ids]
    return list(dict.fromkeys(task_id for task_id in cleaned if task_id))


def read_task_ids_source(source: str) -> list[str]:
    """Read task IDs (one per line) from a local path or an http(s) URL."""
    try:
        if urlparse(source).scheme in {"http", "https"}:
            with urllib.request.urlopen(source) as response:
                content = response.read().decode("utf-8")
        else:
            content = Path(source).read_text(encoding="utf-8")
    except OSError as exc:
        raise click.ClickException(f"Failed to read task ids from {source}: {exc}") from exc

    task_ids = _clean_task_ids(content.splitlines())
    if not task_ids:
        raise click.ClickException(f"No task ids found in {source}")
    return task_ids


def resolve_task_ids(task_ids: str | None = None, task_ids_file: str | None = None) -> list[str] | None:
    if task_ids and task_ids_file:
        raise click.UsageError("--task-ids and --task-ids-file are mutually exclusive")
    if task_ids_file:
        return read_task_ids_source(task_ids_file)
    if task_ids:
        return _clean_task_ids(task_ids.split(",")) or None
    return None


def local_time(dt: datetime) -> str:
    """Convert UTC time to users local time"""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def short_local_time(dt: datetime, include_date: bool = True) -> str:
    """Convert UTC time to a short local time string."""
    fmt = "%m/%d %H:%M" if include_date else "%H:%M"
    return dt.astimezone().strftime(fmt)


def load_config() -> dict[str, str]:
    """Load the Valkyrie configuration from YAML file."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        config = yaml.safe_load(f)

    return config


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

        return f"{' │ '.join(parts)}" if parts else ""


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
    Format and display run status in a box with a progress bar.

    Args:
        benchmark_response: FetchBenchmarkResponse
    """
    details = benchmark_response.details

    bar, progress_pct = BenchmarkFormatter.create_progress_bar(details.finished_tasks, details.total_tasks)
    status_color = BenchmarkFormatter.STATUS_COLORS[details.status.value]
    status_text = click.style(details.status.value.replace("_", " ").title(), fg=status_color, bold=True)
    progress_line = f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
    breakdown_text = BenchmarkFormatter.format_task_breakdown(details.task_breakdown)

    click.echo("┌─ Run Status " + "─" * 66)
    click.echo(f"│ {'Benchmark:':<12} {benchmark_response.benchmark_name}")
    click.echo(f"│ {'Run ID:':<12} {benchmark_response.benchmark_id}")
    if benchmark_response.label:
        click.echo(f"│ {'Label:':<12} {benchmark_response.label}")
    click.echo(f"│ {'Started at:':<12} {local_time(details.started_at)}")
    if benchmark_response.final_score is not None:
        click.echo(f"│ {'Final score:':<12} {benchmark_response.final_score:.1f}%")
    click.echo(f"│ {'S3:':<12} {benchmark_response.s3_bucket_url}")
    analysis_line = _format_docent_analysis(details, benchmark_response.benchmark_id)
    if analysis_line is not None:
        click.echo(f"│ {'Analysis:':<12} {analysis_line}")
    click.echo("├" + "─" * 79)
    click.echo(f"│ {progress_line}")
    if breakdown_text:
        click.echo(f"│ {breakdown_text}")
    click.echo("└" + "─" * 79)


def _format_docent_analysis(details: BenchmarkDetails, run_id: UUID) -> str | None:
    """Render the docent analysis row for `valk run fetch`.

    Returns None for IDLE (no analysis run yet) so the caller can skip the row entirely.
    """

    status = details.docent_reading_status
    if status == DocentReadingStatus.DONE and details.docent_reading_url:
        return details.docent_reading_url
    if status == DocentReadingStatus.RUNNING:
        return "running..."
    if status == DocentReadingStatus.ERROR:
        return f"failed (re-run with `valk run analyze {run_id} --no-cache`)"
    return None


COLUMN_WIDTH = 14


def row(label: str, value: str) -> str:
    return f"│ {label:<{COLUMN_WIDTH}} {value}"


def cont(value: str) -> str:
    """Continuation line (no label) for multi-value fields."""
    return f"│ {' ' * COLUMN_WIDTH} {value}"


def format_run_start_details(
    benchmark: str, dataset: str | None, concurrency: int, slice_str: str | None, task_ids: str | None
) -> None:
    """
    Format and display the start details of a run.

    Args:
        benchmark: The name of the benchmark
        dataset: The name of the dataset
        concurrency: The number of concurrent tasks
        slice_str: The slice of the dataset to use
        task_ids: The IDs of the tasks to run
    """
    click.echo("┌─ Benchmark " + "─" * 67)
    click.echo(row("Name:", benchmark))
    click.echo(row("Dataset:", dataset or "default"))
    click.echo(row("Concurrency:", str(concurrency)))
    if slice_str:
        click.echo(row("Slice:", slice_str))
    if task_ids:
        display = task_ids[:60] + "..." if len(task_ids) > 60 else task_ids
        click.echo(row("Task IDs:", display))
    else:
        click.echo(row("Task IDs:", "all tasks"))
    click.echo("└" + "─" * 79)
    click.echo()


def format_agent_start_details(
    agent: str,
    model: str | None,
    secrets: tuple[tuple[str, str], ...],
    kwargs: tuple[tuple[str, str], ...],
    service_headers: dict[str, str],
    webhook_secret: str | None,
    webhook_intervals: list[int] | None,
) -> None:
    """
    Format and display the start details of an agent.

    Args:
        agent: The name of the agent
        model: The model used by the agent
        secrets: A tuple of secret environment variable and secret name pairs
        kwargs: A tuple of keyword argument pairs
        service_headers: A dictionary of service headers
        webhook_secret: The secret for the webhook
        webhook_intervals: A list of intervals for the webhook
    """
    click.echo("┌─ Agent " + "─" * 71)
    click.echo(row("Agent:", agent))
    if model:
        click.echo(row("Model:", model))
    if secrets:
        for i, (env_var, secret_name) in enumerate(secrets):
            if i == 0:
                click.echo(row("Secrets:", f"{env_var} → {secret_name}"))
            else:
                click.echo(cont(f"{env_var} → {secret_name}"))
    if kwargs:
        click.echo(row("Kwargs:", ", ".join(f"{k}={v}" for k, v in kwargs)))
    if service_headers:
        click.echo(row("Headers:", ", ".join(service_headers.keys())))
    if webhook_secret and webhook_intervals:
        click.echo(row("Notify at:", ", ".join(f"{i}%" for i in webhook_intervals)))
    click.echo("└" + "─" * 79)
    click.echo()


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse) -> None:
    """
    Format and display the start run response.

    Args:
        start_benchmark_response: StartBenchmarkResponse
    """

    rid = start_benchmark_response.benchmark_id

    click.echo()
    click.echo("┌─ Run Details " + "─" * 65)
    click.echo(f"│ {'Benchmark:':<17} {start_benchmark_response.benchmark_name}")
    click.echo(f"│ {'Agent:':<17} {start_benchmark_response.agent_name}")
    click.echo(f"│ {'Run ID:':<17} {rid}")
    click.echo(f"│ {'Started at:':<17} {local_time(start_benchmark_response.started_at)}")
    click.echo(f"│ {'Max concurrency:':<17} {start_benchmark_response.concurrency}")
    click.echo(f"│ {'Total tasks:':<17} {start_benchmark_response.task_count}")
    click.echo(f"│ {'CloudWatch:':<17} {start_benchmark_response.cloudwatch_url}")
    click.echo(f"│ {'S3 Bucket:':<17} {start_benchmark_response.s3_bucket_url}")
    click.echo("├" + "─" * 79)
    click.echo(
        f"│ {'Get results:':<17} " + click.style(f"valkyrie run results {rid} --path ./results-{rid}.json", fg="cyan")
    )
    click.echo(f"│ {'Stop:':<17} " + click.style(f"valkyrie run stop {rid}", fg="cyan"))
    click.echo(f"│ {'Resume:':<17} " + click.style(f"valkyrie run resume {rid}", fg="cyan"))
    click.echo(f"│ {'Retry:':<17} " + click.style(f"valkyrie run retry {rid}", fg="cyan"))
    click.echo(f"│ {'Agent outputs:':<17} " + click.style(f"valkyrie agent outputs {rid} --output-dir .", fg="cyan"))
    click.echo("└" + "─" * 79)


def _stream_next_steps(benchmark_id: UUID, s3_url: str | None = None) -> None:
    """Print next-step commands after a stream ends."""
    rid = benchmark_id
    click.echo(
        f"│ {'Get results:':<17} " + click.style(f"valkyrie run results {rid} --path ./results-{rid}.json", fg="cyan")
    )
    click.echo(f"│ {'Agent outputs:':<17} " + click.style(f"valkyrie agent outputs {rid} --output-dir .", fg="cyan"))
    if s3_url:
        click.echo(f"│ {'S3 view:':<17} " + click.style(s3_url, fg="cyan"))
    click.echo("└" + "─" * 79)


def stream_benchmark_status(tracker: TrackerService, benchmark_id: UUID) -> None:
    """
    Stream and display live run status updates.

    Args:
        tracker: TrackerService instance
        benchmark_id: Run UUID to stream
    """
    initial = tracker.fetch_benchmark(benchmark_id)
    s3_url = initial.s3_bucket_url
    click.echo(click.style("Streaming run updates (Ctrl+C to stop)...", fg="cyan"))

    # Render initial state so there is something visible before the first SSE event
    initial_details = initial.details
    bar, progress_pct = BenchmarkFormatter.create_progress_bar(
        initial_details.finished_tasks, initial_details.total_tasks
    )
    status_color = BenchmarkFormatter.STATUS_COLORS[initial_details.status.value]
    status_text = click.style(initial_details.status.value.replace("_", " ").title(), fg=status_color, bold=True)
    click.echo(
        f"[{bar}] {initial_details.finished_tasks}/{initial_details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
    )
    click.echo(BenchmarkFormatter.format_task_breakdown(initial_details.task_breakdown), nl=False)

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
                click.echo(click.style("✓ Run completed!", fg="green", bold=True))
                click.echo("┌─ Next Steps " + "─" * 66)
                _stream_next_steps(benchmark_id, s3_url)
                break

            elif event.startswith("event: error"):
                click.echo("\n")
                click.echo(click.style("✗ Run errored.", fg="red", bold=True))
                click.echo("┌─ Next Steps " + "─" * 66)
                _stream_next_steps(benchmark_id, s3_url)
                break

            elif event.startswith("event: disconnect"):
                click.echo("\n")
                click.echo(click.style("Disconnected from stream.", fg="yellow"))
                break

    except KeyboardInterrupt:
        click.echo("\n")
        click.echo(click.style("Streaming stopped.", fg="yellow"))
        click.echo("┌─ Next Steps " + "─" * 66)
        _stream_next_steps(benchmark_id, s3_url)


def format_fetch_benchmarks_response(
    fetch_benchmarks_response: FetchBenchmarksResponse,
    current_page: int = 1,
    total_pages: int = 1,
) -> None:
    """
    Format and display runs in a table format.

    Args:
        fetch_benchmarks_response: FetchBenchmarksResponse containing list of runs
        current_page: Current page number (1-indexed)
        total_pages: Total number of pages
    """
    benchmarks = fetch_benchmarks_response.benchmarks

    if not benchmarks:
        click.echo(click.style("No runs found.", fg="yellow"))
        return

    def format_score(score: float | None) -> str:
        return f"{score:.1f}%" if score is not None else "-"

    rows: list[dict[str, str]] = []
    for benchmark in benchmarks:
        _, progress_percentage = BenchmarkFormatter.create_progress_bar(benchmark.finished_tasks, benchmark.total_tasks)

        rows.append(
            {
                "ID": str(benchmark.id),
                "Benchmark": benchmark.name,
                "Agent": benchmark.agent_name,
                "Label": benchmark.label or "-",
                "Started By": benchmark.started_by_email or "—",
                "Model": benchmark.model or "-",
                "Dataset": benchmark.dataset or "default",
                "Status": click.style(
                    benchmark.status.value.replace("_", " ").title(),
                    fg=BenchmarkFormatter.STATUS_COLORS[benchmark.status.value],
                ),
                "Score": format_score(benchmark.final_score),
                "Started / Finished": f"{short_local_time(benchmark.started_at)} / {short_local_time(benchmark.finished_at, include_date=False) if benchmark.finished_at else '-'}",
                "Progress": f"{progress_percentage:.1f}%",
            }
        )

    format_table(
        rows,
        [
            "ID",
            "Benchmark",
            "Agent",
            "Label",
            "Started By",
            "Model",
            "Dataset",
            "Status",
            "Score",
            "Started / Finished",
            "Progress",
        ],
        current_page,
        total_pages,
        fetch_benchmarks_response.total_count,
        "run",
    )


def format_no_benchmarks_found(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    started_by: list[str] | None = None,
) -> None:
    """
    Handle the case where no runs are found matching the specified filters.

    Args:
        agent_name: Agent name filter
        benchmark_name: Benchmark name filter
        model: Model name filter
        dataset: Dataset name filter
        label: Run label filter
        status: Status filter
        started_by: Optional list of starter emails filter
    """
    click.echo()
    click.echo(click.style("No runs found matching the specified filters.", fg="yellow"))
    click.echo()
    if any([agent_name, benchmark_name, model, dataset, label, status, started_by]):
        click.echo("Filters applied:")
        if agent_name:
            click.echo(f"  • Agent: {agent_name}")
        if benchmark_name:
            click.echo(f"  • Benchmark: {benchmark_name}")
        if model:
            click.echo(f"  • Model: {model}")
        if dataset:
            click.echo(f"  • Dataset: {dataset}")
        if label:
            click.echo(f"  • Label: {label}")
        if status:
            click.echo(f"  • Status: {status}")
        if started_by:
            click.echo(f"  • Started By: {', '.join(started_by)}")


def paginate_benchmarks(
    tracker: TrackerService,
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str,
    limit: int = 5,
    started_by: list[str] | None = None,
) -> None:
    """
    Interactive paginated display of runs with vim-style navigation.

    Args:
        tracker: TrackerService instance
        agent_name: Optional agent name filter
        benchmark_name: Optional benchmark name filter
        model: Optional model name filter
        dataset: Optional dataset name filter
        label: Optional run label filter
        status: Optional status filter
        order_by: Order (asc/desc)
        limit: Number of items per page
        started_by: Optional list of starter emails to filter by
    """
    current_page = 1
    offset = 0

    while True:
        request = FetchBenchmarksRequest(
            agent_name=[agent_name] if agent_name else None,
            benchmark_name=[benchmark_name] if benchmark_name else None,
            model=model,
            dataset=dataset,
            label=label,
            status=[BenchmarkStatus(status)] if status else None,
            started_by=started_by,
            order_by=Order(order_by),
            limit=limit,
            offset=offset,
        )

        response = tracker.fetch_benchmarks(request)
        total_count = response.total_count or 0
        total_pages = max(1, (total_count + limit - 1) // limit)

        click.clear()

        if total_count == 0:
            format_no_benchmarks_found(agent_name, benchmark_name, model, dataset, label, status, started_by)
            break

        format_fetch_benchmarks_response(response, current_page, total_pages)

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

    click.echo(click.style(f"Results saved to '{path}'", fg="green", bold=True))


def format_table(
    rows: list[dict[str, str]],
    headers: list[str],
    current_page: int = 1,
    total_pages: int = 1,
    total_count: int | None = None,
    item_name: str = "item",
) -> None:
    """
    Generic table formatter for CLI output.

    Args:
        rows: List of dictionaries with keys matching headers (values can be strings or Click styled text)
        headers: List of column headers
        current_page: Current page number (1-indexed)
        total_pages: Total number of pages
        total_count: Total item count (defaults to len(rows))
        item_name: Name of items for display (e.g., "benchmark", "agent")
    """
    if not rows:
        click.echo(click.style(f"No {item_name}s found.", fg="yellow"))
        return

    if total_count is None:
        total_count = len(rows)

    # Calculate column widths, accounting for styled text (strip ANSI codes for length)
    col_widths = {}
    for header in headers:
        max_width = len(header)
        for row in rows:
            val = str(row.get(header, ""))
            # Remove ANSI color codes for width calculation
            clean_val = click.unstyle(val) if hasattr(click, "unstyle") else val
            max_width = max(max_width, len(clean_val))
        col_widths[header] = max_width

    header_line = "  ".join(f"{h:<{col_widths[h]}}" for h in headers)
    separator = "─" * len(header_line)

    click.echo()
    click.echo(click.style(header_line, bold=True))
    click.echo(separator)

    for row in rows:
        cells: list[str] = []
        for h in headers:
            val = str(row.get(h, ""))
            clean_val = click.unstyle(val) if hasattr(click, "unstyle") else val
            # Pad to column width using clean string length
            padding = col_widths[h] - len(clean_val)
            cells.append(f"{val}{' ' * padding}")
        click.echo("  ".join(cells))

    click.echo(separator)

    total_text = f"Total: {total_count} {item_name}(s)"
    if total_pages > 1:
        nav_text = click.style(f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit", fg="bright_black")
        padding = (
            len(separator) - len(total_text) - len(f"[h] ← prev  {current_page}/{total_pages}  next → [l]  [q] quit")
        )
        click.echo(f"{total_text}{' ' * padding}{nav_text}")
    else:
        click.echo(total_text)


def format_agents_response(
    agents: list[tuple[str, datetime | None]],
    current_page: int,
    total_pages: int,
    total_count: int,
) -> None:
    """
    Format and display agents in a table format.

    Args:
        agents: List of tuples (agent_name, last_modified) for the current page
        current_page: Current page number (1-indexed)
        total_pages: Total number of pages
        total_count: Total agent count across all pages
    """
    rows = [
        {"Agent": name, "Last Modified": local_time(last_modified) if last_modified else "-"}
        for name, last_modified in agents
    ]

    format_table(rows, ["Agent", "Last Modified"], current_page, total_pages, total_count, "agent")


def paginate_agents(agents: list[tuple[str, datetime | None]], limit: int = 10) -> None:
    """
    Interactive paginated display of agents with vim-style navigation.

    Args:
        agents: List of tuples (agent_name, last_modified)
        limit: Number of items per page
    """
    current_page = 1
    offset = 0
    total_count = len(agents)
    total_pages = max(1, (total_count + limit - 1) // limit)

    while True:
        click.clear()

        if total_count == 0:
            click.echo(click.style("No agents found.", fg="yellow"))
            break

        page_agents = agents[offset : offset + limit]
        format_agents_response(page_agents, current_page, total_pages, total_count)

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


def validate_intervals(intervals: tuple[int, ...]) -> list[int]:
    """Validate notification interval values.

    Args:
        intervals: Tuple of percentage thresholds for Slack notifications.

    Raises:
        click.UsageError: If intervals are invalid.

    Returns:
        Validated list of intervals.
    """
    interval_list = list(intervals)
    if len(interval_list) > 3:
        raise click.UsageError("Maximum of 3 intervals allowed.")
    for val in interval_list:
        if val < 5 or val > 100:
            raise click.UsageError(f"Interval {val} out of range. Must be between 5 and 100.")
        if val % 5 != 0:
            raise click.UsageError(f"Interval {val} must be divisible by 5.")
    return interval_list


def resolve_webhook_config(
    intervals: tuple[int, ...], webhook_secret: str | None
) -> tuple[str | None, list[int] | None]:
    """Resolve webhook secret and intervals for a benchmark run.

    Args:
        intervals: User-provided interval flags from CLI.
        webhook_secret: Webhook secret name from config, or None.

    Returns:
        Tuple of (webhook_secret, webhook_intervals) to pass to the tracker.
    """
    if intervals and not webhook_secret:
        click.echo(
            click.style(
                "  Warning: --interval specified but no webhook secret configured. "
                "Run `valkyrie config set webhook <secret-name>` first. Ignoring intervals.",
                fg="yellow",
            )
        )
        return None, None

    if intervals:
        return webhook_secret, validate_intervals(intervals)

    if webhook_secret:
        return webhook_secret, [100]

    return None, None


def paginate_services(services: list[tuple[str, str]], limit: int = 10) -> None:
    """
    Interactive paginated display of services with vim-style navigation.

    Args:
        services: List of tuples (benchmark_name, service_url)
        limit: Number of items per page
    """
    current_page = 1
    offset = 0
    total_count = len(services)
    total_pages = max(1, (total_count + limit - 1) // limit)

    while True:
        click.clear()

        if total_count == 0:
            click.echo(click.style("No custom services have been added.", fg="yellow"))
            break

        page_services = services[offset : offset + limit]
        rows = [{"Benchmark": name, "Service URL": url} for name, url in page_services]
        format_table(rows, ["Benchmark", "Service URL"], current_page, total_pages, total_count, "service")

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
