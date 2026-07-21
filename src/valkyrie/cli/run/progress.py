"""Run progress rendering and streaming helpers."""

from typing import Literal
from uuid import UUID

import click
from tracker.database.models import BenchmarkStatus, DocentReadingStatus, TaskStatus
from tracker.types import BenchmarkDetails, FetchBenchmarkMetadataResponse, FetchBenchmarkResponse

from valkyrie.cli.display import local_time, terminal_safe
from valkyrie.cli.run.snapshot import fetch_run_metadata, format_run_snapshot_json
from valkyrie.cli.tracker_client import TrackerService


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
        """Create a progress bar string and percentage."""
        progress_pct = (finished_tasks / total_tasks * 100) if total_tasks > 0 else 0
        filled_width = int(bar_width * progress_pct / 100)
        bar = "█" * filled_width + "░" * (bar_width - filled_width)
        return bar, progress_pct

    @staticmethod
    def format_task_breakdown(task_breakdown: dict[TaskStatus, int]) -> str:
        """Format task breakdown with colored status counts."""
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
            parts.append(click.style(f"{label}: {count}", fg=color))

        return f"{' │ '.join(parts)}" if parts else ""


def _display_run_error(benchmark_response: FetchBenchmarkResponse) -> None:
    """Display the stored run error as terminal-safe red text.

    Arguments
    - benchmark_response: Run response that may contain an error message.
    """
    if not benchmark_response.error_message:
        return

    safe_error = terminal_safe(benchmark_response.error_message, preserve_newlines=True)
    click.secho(f"Error: {safe_error}", fg="red")


def format_benchmark_status(benchmark_response: FetchBenchmarkResponse) -> None:
    """Format and display run status in a box with a progress bar."""
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
    if details.status == BenchmarkStatus.ERROR and benchmark_response.error_message:
        _display_run_error(benchmark_response)


def format_run_identity(
    benchmark_response: FetchBenchmarkResponse,
    metadata: FetchBenchmarkMetadataResponse | None,
) -> None:
    """Display stable run identity before connected progress updates."""
    click.echo("┌─ Run Details " + "─" * 65)
    click.echo(f"│ {'Benchmark:':<17} {benchmark_response.benchmark_name}")
    if metadata is not None:
        arguments = metadata.benchmark_arguments
        click.echo(f"│ {'Agent:':<17} {arguments.contract.name}")
        click.echo(f"│ {'Model:':<17} {arguments.contract.model or '-'}")
        click.echo(f"│ {'Dataset:':<17} {arguments.dataset or 'default'}")
    click.echo(f"│ {'Run ID:':<17} {benchmark_response.benchmark_id}")
    if benchmark_response.label:
        click.echo(f"│ {'Label:':<17} {benchmark_response.label}")
    if metadata is not None:
        if metadata.started_by_email:
            click.echo(f"│ {'Started by:':<17} {metadata.started_by_email}")
        click.echo(f"│ {'Max concurrency:':<17} {metadata.benchmark_arguments.concurrency}")
    else:
        click.echo(f"│ {'Metadata:':<17} unavailable")
    click.echo(f"│ {'Started at:':<17} {local_time(benchmark_response.details.started_at)}")
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


def _stream_next_steps(benchmark_id: UUID, s3_url: str | None = None) -> None:
    """Print next-step commands after a stream ends."""
    run_id = benchmark_id
    click.echo(
        f"│ {'Get results:':<17} "
        + click.style(f"valkyrie run results {run_id} --path ./results-{run_id}.json", fg="cyan")
    )
    click.echo(f"│ {'Run outputs:':<17} " + click.style(f"valkyrie run outputs {run_id} --output-dir .", fg="cyan"))
    if s3_url:
        click.echo(f"│ {'S3 view:':<17} " + click.style(s3_url, fg="cyan"))
    click.echo("└" + "─" * 79)


def _completion_event(response: FetchBenchmarkResponse) -> Literal["complete", "error", "stopped"]:
    if response.details.status == BenchmarkStatus.ERROR:
        return "error"
    if response.details.status == BenchmarkStatus.STOPPED:
        return "stopped"
    return "complete"


def stream_benchmark_status(
    tracker: TrackerService,
    benchmark_id: UUID,
    *,
    show_identity: bool = False,
    output_format: Literal["text", "jsonl"] = "text",
) -> None:
    """Stream and display live run status updates."""
    initial = tracker.fetch_benchmark(benchmark_id)
    if output_format == "text" and initial.details.status in {
        BenchmarkStatus.FINISHED,
        BenchmarkStatus.ERROR,
        BenchmarkStatus.STOPPED,
    }:
        format_benchmark_status(initial)
        return

    s3_url = initial.s3_bucket_url
    metadata = fetch_run_metadata(tracker, benchmark_id) if show_identity or output_format == "jsonl" else None
    if output_format == "jsonl":
        click.echo(format_run_snapshot_json(initial, metadata, event="snapshot"))
    else:
        if show_identity:
            format_run_identity(initial, metadata)
        click.echo(click.style("Streaming run updates (Ctrl+C to stop)...", fg="cyan"))

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

    latest = initial

    try:
        for event in tracker.stream_benchmark(benchmark_id):
            if event.startswith("data:"):
                data_json = event[5:].strip()
                if not data_json:
                    continue

                response = FetchBenchmarkResponse.model_validate_json(data_json)
                latest = response
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(response, metadata, event="update"))
                    continue

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
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(latest, metadata, event=_completion_event(latest)))
                else:
                    click.echo("\n")
                    completion_event = _completion_event(latest)
                    if completion_event == "error":
                        click.echo(click.style("✗ Run errored.", fg="red", bold=True))
                        _display_run_error(latest)
                    elif completion_event == "stopped":
                        click.echo(click.style("Run stopped.", fg="cyan", bold=True))
                    else:
                        click.echo(click.style("✓ Run completed!", fg="green", bold=True))
                    click.echo("┌─ Next Steps " + "─" * 66)
                    _stream_next_steps(benchmark_id, s3_url)
                break

            elif event.startswith("event: error"):
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(latest, metadata, event="error"))
                else:
                    click.echo("\n")
                    click.echo(click.style("✗ Run errored.", fg="red", bold=True))
                    _display_run_error(latest)
                    click.echo("┌─ Next Steps " + "─" * 66)
                    _stream_next_steps(benchmark_id, s3_url)
                break

            elif event.startswith("event: disconnect"):
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(latest, metadata, event="disconnect"))
                else:
                    click.echo("\n")
                    click.echo(click.style("Disconnected from stream.", fg="yellow"))
                break
        else:
            if output_format == "jsonl":
                click.echo(format_run_snapshot_json(latest, metadata, event="disconnect"))
            else:
                click.echo("\n")
                click.echo(click.style("Disconnected from stream.", fg="yellow"))
            raise click.ClickException("Run stream ended without a terminal event.")

    except KeyboardInterrupt:
        if output_format == "jsonl":
            click.echo(format_run_snapshot_json(latest, metadata, event="interrupted"))
        else:
            click.echo("\n")
            click.echo(click.style("Streaming stopped.", fg="yellow"))
            click.echo("┌─ Next Steps " + "─" * 66)
            _stream_next_steps(benchmark_id, s3_url)
