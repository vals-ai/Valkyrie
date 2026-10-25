"""Run progress rendering and streaming helpers."""

import asyncio
from collections.abc import Iterable
from typing import Literal
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.models import (
    BenchmarkDetails,
    BenchmarkStatus,
    DocentReadingStatus,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    TaskStatus,
)

from valkyrie.cli.display import local_time, progress_percent, terminal_safe
from valkyrie.cli.project import describe_run_spend
from valkyrie.cli.run.snapshot import RunEvent, fetch_run_metadata, format_run_snapshot_json

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

TASK_STATUS_ORDER = (
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
    TaskStatus.ERROR,
    TaskStatus.STOPPED,
    TaskStatus.FINISHED,
)

TERMINAL_STATUSES = {BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED}

OUTCOME_MESSAGES = {
    "complete": click.style("✓ Run completed!", fg="green", bold=True),
    "error": click.style("✗ Run errored.", fg="red", bold=True),
    "stopped": click.style("Run stopped.", fg="cyan", bold=True),
}


def styled_status(status: BenchmarkStatus, *, bold: bool = False) -> str:
    return click.style(status.value.replace("_", " ").title(), fg=STATUS_COLORS[status.value], bold=bold)


def _progress_lines(details: BenchmarkDetails, bar_width: int = 30) -> tuple[str, str]:
    """Return the progress bar line and the colored per-status task breakdown line."""
    percent = progress_percent(details.finished_tasks, details.total_tasks)
    filled = int(bar_width * percent / 100)
    bar = "█" * filled + "░" * (bar_width - filled)
    progress = (
        f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({percent:.1f}%) • "
        f"{styled_status(details.status, bold=True)}"
    )
    breakdown = " │ ".join(
        click.style(f"{status.value.replace('_', ' ').title()}: {count}", fg=STATUS_COLORS[status.value])
        for status in TASK_STATUS_ORDER
        if (count := details.task_breakdown.get(status, 0))
    )
    return progress, breakdown


def _echo_rows(rows: Iterable[tuple[str, str | None]], label_width: int) -> None:
    for label, value in rows:
        if value:
            click.echo(f"│ {label:<{label_width}} {value}")


def _display_run_error(benchmark_response: FetchBenchmarkResponse) -> None:
    if benchmark_response.error_message:
        safe_error = terminal_safe(benchmark_response.error_message, preserve_newlines=True)
        click.secho(f"Error: {safe_error}", fg="red")


def format_benchmark_status(benchmark_response: FetchBenchmarkResponse, spend: str | None = None) -> None:
    """Format and display run status in a box with a progress bar."""
    details = benchmark_response.details
    progress_line, breakdown_line = _progress_lines(details)

    click.echo("┌─ Run Status " + "─" * 66)
    _echo_rows(
        [
            ("Benchmark:", benchmark_response.benchmark_name),
            ("Run ID:", str(benchmark_response.benchmark_id)),
            ("Label:", benchmark_response.label),
            ("Spend:", spend),
            ("Started at:", local_time(details.started_at)),
            (
                "Final score:",
                f"{benchmark_response.final_score:.1f}%" if benchmark_response.final_score is not None else None,
            ),
            ("Artifacts:", benchmark_response.s3_bucket_url),
            ("Analysis:", _format_docent_analysis(details, benchmark_response.benchmark_id)),
        ],
        12,
    )
    click.echo("├" + "─" * 79)
    click.echo(f"│ {progress_line}")
    if breakdown_line:
        click.echo(f"│ {breakdown_line}")
    click.echo("└" + "─" * 79)
    if details.status == BenchmarkStatus.ERROR:
        _display_run_error(benchmark_response)


def format_run_identity(
    benchmark_response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
) -> None:
    """Display stable run identity before connected progress updates."""
    rows: list[tuple[str, str | None]] = [("Benchmark:", benchmark_response.benchmark_name)]
    if metadata is None:
        rows += [("Run ID:", str(benchmark_response.benchmark_id)), ("Label:", benchmark_response.label)]
        rows.append(("Metadata:", "unavailable"))
    else:
        arguments = metadata.benchmark_arguments
        version = arguments.dataset_version
        rows += [
            ("Agent:", arguments.contract.name),
            ("Model:", arguments.contract.model or "-"),
            ("Dataset:", arguments.dataset or "default"),
            ("Dataset version:", version.label or version.id if version else "not pinned"),
            ("Run ID:", str(benchmark_response.benchmark_id)),
            ("Label:", benchmark_response.label),
            ("Started by:", metadata.started_by_email),
            ("Max concurrency:", str(arguments.concurrency)),
            ("Spend:", describe_run_spend(metadata)),
        ]
    rows.append(("Started at:", local_time(benchmark_response.details.started_at)))

    click.echo("┌─ Run Details " + "─" * 65)
    _echo_rows(rows, 17)
    click.echo("└" + "─" * 79)


def _format_docent_analysis(details: BenchmarkDetails, run_id: UUID) -> str | None:
    status = details.docent_reading_status
    if status == DocentReadingStatus.DONE and details.docent_reading_url:
        return details.docent_reading_url
    if status == DocentReadingStatus.RUNNING:
        return "running..."
    if status == DocentReadingStatus.ERROR:
        return f"failed (re-run with `valk run analyze {run_id} --no-cache`)"
    return None


def echo_next_steps(run_id: UUID, *, track: bool = False, outputs: bool = False, s3_url: str | None = None) -> None:
    """Print the follow-up commands for a run inside the Next Steps box."""
    steps = {
        "Track progress:": f"valkyrie run fetch {run_id} --connect" if track else None,
        "Get results:": f"valkyrie run results {run_id} --path ./results-{run_id}.json",
        "Run outputs:": f"valkyrie run outputs {run_id} --output-dir ." if outputs else None,
        "S3 view:": s3_url,
    }
    click.echo("┌─ Next Steps " + "─" * 66)
    _echo_rows(((label, click.style(command, fg="cyan")) for label, command in steps.items() if command), 17)
    click.echo("└" + "─" * 79)


def _completion_event(response: FetchBenchmarkResponse) -> Literal["complete", "error", "stopped"]:
    if response.details.status == BenchmarkStatus.ERROR:
        return "error"
    if response.details.status == BenchmarkStatus.STOPPED:
        return "stopped"
    return "complete"


async def stream_benchmark_status(
    client: ValkyrieClient,
    benchmark_id: UUID,
    *,
    show_identity: bool = False,
    output_format: Literal["text", "jsonl"] = "text",
) -> None:
    """Stream and display live run status updates until the run ends or the user presses Ctrl+C."""
    initial = await client.runs.fetch(benchmark_id)
    jsonl = output_format == "jsonl"
    if not jsonl and initial.details.status in TERMINAL_STATUSES:
        spend = describe_run_spend(await fetch_run_metadata(client, benchmark_id)) if show_identity else None
        format_benchmark_status(initial, spend)
        return

    metadata = await fetch_run_metadata(client, benchmark_id) if show_identity or jsonl else None
    if jsonl:
        click.echo(format_run_snapshot_json(initial, metadata, event="snapshot"))
    else:
        if show_identity:
            format_run_identity(initial, metadata)
        click.echo(click.style("Streaming run updates (Ctrl+C to stop)...", fg="cyan"))
        progress_line, breakdown_line = _progress_lines(initial.details)
        click.echo(f"{progress_line}\n{breakdown_line}", nl=False)

    latest = initial

    def finish(event: RunEvent, message: str | None = None, *, next_steps: bool = True) -> None:
        if jsonl:
            click.echo(format_run_snapshot_json(latest, metadata, event=event))
            return
        click.echo("\n")
        if message:
            click.echo(message)
        if event == "error":
            _display_run_error(latest)
        if next_steps:
            echo_next_steps(benchmark_id, outputs=True, s3_url=initial.s3_bucket_url)

    disconnected = click.style("Disconnected from stream.", fg="yellow")
    try:
        async for event in client.runs.events(benchmark_id):
            if event.run is not None:
                latest = event.run
                if jsonl:
                    click.echo(format_run_snapshot_json(latest, metadata, event="update"))
                else:
                    progress_line, breakdown_line = _progress_lines(latest.details)
                    click.echo(f"\033[F\033[K{progress_line}\n\033[K{breakdown_line}", nl=False)
            elif event.event == "complete":
                outcome = _completion_event(latest)
                finish(outcome, OUTCOME_MESSAGES[outcome])
                return
            elif event.event == "error":
                finish("error", OUTCOME_MESSAGES["error"])
                return
            else:
                finish("disconnect", disconnected, next_steps=False)
                return
    # asyncio.run turns Ctrl+C into a cancellation of this coroutine on the main thread.
    except (KeyboardInterrupt, asyncio.CancelledError):
        finish("interrupted", click.style("Streaming stopped.", fg="yellow"))
        return

    finish("disconnect", disconnected, next_steps=False)
    raise click.ClickException("Run stream ended without a terminal event.")
