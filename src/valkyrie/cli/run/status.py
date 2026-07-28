from uuid import UUID

import click
from tracker.types import BenchmarkStatusEntry

from valkyrie.cli.display import format_table
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.progress import BenchmarkFormatter
from valkyrie.cli.run.snapshot import format_run_status_json
from valkyrie.cli.tracker_client import TrackerService

_STATUS_BATCH_SIZE = 50


@click.command(
    name="status",
    help=(
        "Fetch lightweight status for multiple run IDs.\n\n"
        "Example:\n"
        "valkyrie run status --ids "
        "123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001 --format json"
    ),
)
@click.option(
    "--ids",
    "run_ids_value",
    required=True,
    help="Comma-separated run UUIDs. Duplicates are ignored while preserving order.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
def status_runs(run_ids_value: str, output_format: str) -> None:
    """Fetch lightweight progress for a deterministic set of run IDs."""
    run_ids = parse_run_ids(run_ids_value)

    try:
        with TrackerService() as tracker:
            entries = fetch_status_entries(tracker, run_ids)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))

    entries_by_id = {entry.id: entry for entry in entries}
    ordered_entries = [entries_by_id[run_id] for run_id in run_ids if run_id in entries_by_id]
    missing_run_ids = [run_id for run_id in run_ids if run_id not in entries_by_id]

    if output_format == "json":
        click.echo(
            format_run_status_json(
                ordered_entries,
                missing_run_ids,
                requested_count=len(run_ids),
            )
        )
    else:
        format_status_table(ordered_entries)

    if missing_run_ids:
        raise click.ClickException(f"{len(missing_run_ids)} requested run ID(s) were not found or are not accessible.")


def parse_run_ids(value: str) -> list[UUID]:
    """Parse comma-separated UUIDs and de-duplicate them in input order."""
    raw_ids = [part.strip() for part in value.split(",") if part.strip()]
    if not raw_ids:
        raise click.BadParameter("provide at least one run UUID", param_hint="--ids")

    run_ids: list[UUID] = []
    seen: set[UUID] = set()
    for raw_id in raw_ids:
        try:
            run_id = UUID(raw_id)
        except ValueError as e:
            raise click.BadParameter(f"invalid run UUID: {raw_id}", param_hint="--ids") from e
        if run_id not in seen:
            seen.add(run_id)
            run_ids.append(run_id)
    return run_ids


def fetch_status_entries(tracker: TrackerService, run_ids: list[UUID]) -> list[BenchmarkStatusEntry]:
    """Fetch status entries in URL-safe batches without writing partial output."""
    entries: list[BenchmarkStatusEntry] = []
    for start in range(0, len(run_ids), _STATUS_BATCH_SIZE):
        response = tracker.fetch_benchmark_statuses(run_ids[start : start + _STATUS_BATCH_SIZE])
        entries.extend(response.entries)
    return entries


def format_status_table(entries: list[BenchmarkStatusEntry]) -> None:
    """Render lightweight multi-run status for humans."""
    rows: list[dict[str, str]] = []
    for entry in entries:
        _, progress_percent = BenchmarkFormatter.create_progress_bar(entry.finished_tasks, entry.total_tasks)
        rows.append(
            {
                "ID": str(entry.id),
                "Status": click.style(
                    entry.status.value.replace("_", " ").title(),
                    fg=BenchmarkFormatter.STATUS_COLORS[entry.status.value],
                ),
                "Progress": f"{entry.finished_tasks}/{entry.total_tasks} ({progress_percent:.1f}%)",
            }
        )
    format_table(rows, ["ID", "Status", "Progress"], total_count=len(entries), item_name="run")
