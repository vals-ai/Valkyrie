"""Run progress rendering and streaming helpers."""

from uuid import UUID

import click
from tracker.database.models import DocentReadingStatus, TaskStatus
from tracker.types import BenchmarkDetails, FetchBenchmarkResponse

from valkyrie.cli.display import local_time
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


def stream_benchmark_status(tracker: TrackerService, benchmark_id: UUID) -> None:
    """Stream and display live run status updates."""
    initial = tracker.fetch_benchmark(benchmark_id)
    s3_url = initial.s3_bucket_url
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
