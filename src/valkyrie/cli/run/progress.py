"""Run progress rendering and streaming helpers."""

from typing import Literal
from uuid import UUID

import click
from tracker.database.models import DocentReadingStatus, TaskStatus
from tracker.types import GetRunResponse, RunDetails, RunMetadataResponse, RunStatus

from valkyrie.cli.display import local_time
from valkyrie.cli.run.snapshot import fetch_run_metadata, format_run_snapshot_json
from valkyrie.cli.tracker_client import TrackerService


class RunFormatter:
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

            color = RunFormatter.STATUS_COLORS[status]
            label = status.value.replace("_", " ").title()
            parts.append(click.style(f"{label}: {count}", fg=color))

        return f"{' │ '.join(parts)}" if parts else ""


def format_run_status(run_response: GetRunResponse) -> None:
    """Format and display run status in a box with a progress bar."""
    details = run_response.details

    bar, progress_pct = RunFormatter.create_progress_bar(details.finished_tasks, details.total_tasks)
    status_color = RunFormatter.STATUS_COLORS[details.status.value]
    status_text = click.style(details.status.value.replace("_", " ").title(), fg=status_color, bold=True)
    progress_line = f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
    breakdown_text = RunFormatter.format_task_breakdown(details.task_breakdown)

    click.echo("┌─ Run Status " + "─" * 66)
    click.echo(f"│ {'Benchmark:':<12} {run_response.benchmark_name}")
    click.echo(f"│ {'Run ID:':<12} {run_response.run_id}")
    if run_response.label:
        click.echo(f"│ {'Label:':<12} {run_response.label}")
    click.echo(f"│ {'Started at:':<12} {local_time(details.started_at)}")
    if run_response.final_score is not None:
        click.echo(f"│ {'Final score:':<12} {run_response.final_score:.1f}%")
    click.echo(f"│ {'S3:':<12} {run_response.s3_bucket_url}")
    analysis_line = _format_docent_analysis(details, run_response.run_id)
    if analysis_line is not None:
        click.echo(f"│ {'Analysis:':<12} {analysis_line}")
    click.echo("├" + "─" * 79)
    click.echo(f"│ {progress_line}")
    if breakdown_text:
        click.echo(f"│ {breakdown_text}")
    click.echo("└" + "─" * 79)


def format_run_identity(
    run_response: GetRunResponse,
    metadata: RunMetadataResponse | None,
) -> None:
    """Display stable run identity before connected progress updates."""
    click.echo("┌─ Run Details " + "─" * 65)
    click.echo(f"│ {'Benchmark:':<17} {run_response.benchmark_name}")
    if metadata is not None:
        arguments = metadata.run_arguments
        click.echo(f"│ {'Agent:':<17} {arguments.contract.name}")
        click.echo(f"│ {'Model:':<17} {arguments.contract.model or '-'}")
        click.echo(f"│ {'Dataset:':<17} {arguments.dataset or 'default'}")
    click.echo(f"│ {'Run ID:':<17} {run_response.run_id}")
    if run_response.label:
        click.echo(f"│ {'Label:':<17} {run_response.label}")
    if metadata is not None:
        if metadata.started_by_email:
            click.echo(f"│ {'Started by:':<17} {metadata.started_by_email}")
        click.echo(f"│ {'Max concurrency:':<17} {metadata.run_arguments.concurrency}")
    else:
        click.echo(f"│ {'Metadata:':<17} unavailable")
    click.echo(f"│ {'Started at:':<17} {local_time(run_response.details.started_at)}")
    click.echo("└" + "─" * 79)


def _format_docent_analysis(details: RunDetails, run_id: UUID) -> str | None:
    status = details.docent_reading_status
    if status == DocentReadingStatus.DONE and details.docent_reading_url:
        return details.docent_reading_url
    if status == DocentReadingStatus.RUNNING:
        return "running..."
    if status == DocentReadingStatus.ERROR:
        return f"failed (re-run with `valk run analyze {run_id} --no-cache`)"
    return None


def _stream_next_steps(run_id: UUID, s3_url: str | None = None) -> None:
    """Print next-step commands after a stream ends."""
    click.echo(
        f"│ {'Get results:':<17} "
        + click.style(f"valkyrie run results {run_id} --path ./results-{run_id}.json", fg="cyan")
    )
    click.echo(f"│ {'Run outputs:':<17} " + click.style(f"valkyrie run outputs {run_id} --output-dir .", fg="cyan"))
    if s3_url:
        click.echo(f"│ {'S3 view:':<17} " + click.style(s3_url, fg="cyan"))
    click.echo("└" + "─" * 79)


def _completion_event(response: GetRunResponse) -> Literal["complete", "error", "stopped"]:
    if response.details.status == RunStatus.ERROR:
        return "error"
    if response.details.status == RunStatus.STOPPED:
        return "stopped"
    return "complete"


def stream_run_status(
    tracker: TrackerService,
    run_id: UUID,
    *,
    show_identity: bool = False,
    output_format: Literal["text", "jsonl"] = "text",
) -> None:
    """Stream and display live run status updates."""
    initial = tracker.fetch_run(run_id)
    s3_url = initial.s3_bucket_url
    metadata = fetch_run_metadata(tracker, run_id) if show_identity or output_format == "jsonl" else None
    if output_format == "jsonl":
        click.echo(format_run_snapshot_json(initial, metadata, event="snapshot"))
    else:
        if show_identity:
            format_run_identity(initial, metadata)
        click.echo(click.style("Streaming run updates (Ctrl+C to stop)...", fg="cyan"))

        initial_details = initial.details
        bar, progress_pct = RunFormatter.create_progress_bar(
            initial_details.finished_tasks, initial_details.total_tasks
        )
        status_color = RunFormatter.STATUS_COLORS[initial_details.status.value]
        status_text = click.style(initial_details.status.value.replace("_", " ").title(), fg=status_color, bold=True)
        click.echo(
            f"[{bar}] {initial_details.finished_tasks}/{initial_details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
        )
        click.echo(RunFormatter.format_task_breakdown(initial_details.task_breakdown), nl=False)

    latest = initial

    try:
        for event in tracker.stream_run(run_id):
            if event.startswith("data:"):
                data_json = event[5:].strip()
                if not data_json:
                    continue

                response = GetRunResponse.model_validate_json(data_json)
                latest = response
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(response, metadata, event="update"))
                    continue

                details = response.details

                bar, progress_pct = RunFormatter.create_progress_bar(details.finished_tasks, details.total_tasks)
                status_color = RunFormatter.STATUS_COLORS[details.status.value]
                status_text = click.style(details.status.value.replace("_", " ").title(), fg=status_color, bold=True)

                progress_line = (
                    f"[{bar}] {details.finished_tasks}/{details.total_tasks} ({progress_pct:.1f}%) • {status_text}"
                )
                breakdown_text = RunFormatter.format_task_breakdown(details.task_breakdown)

                click.echo(f"\033[F\033[K{progress_line}\n\033[K{breakdown_text}", nl=False)

            elif event.startswith("event: complete"):
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(latest, metadata, event=_completion_event(latest)))
                else:
                    click.echo("\n")
                    click.echo(click.style("✓ Run completed!", fg="green", bold=True))
                    click.echo("┌─ Next Steps " + "─" * 66)
                    _stream_next_steps(run_id, s3_url)
                break

            elif event.startswith("event: error"):
                if output_format == "jsonl":
                    click.echo(format_run_snapshot_json(latest, metadata, event="error"))
                else:
                    click.echo("\n")
                    click.echo(click.style("✗ Run errored.", fg="red", bold=True))
                    click.echo("┌─ Next Steps " + "─" * 66)
                    _stream_next_steps(run_id, s3_url)
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
            _stream_next_steps(run_id, s3_url)
