"""Lower a run's concurrency and force stop the newest tasks above the new limit."""

import asyncio
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient, ValkyrieSDKError
from valkyrie.sdk.models import FetchTasksRequest, TaskStatus, TaskSummary

from valkyrie.cli.display import format_table, terminal_safe
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.runtime_config import config_location, tracker_service_url
from valkyrie.cli.tracker_client import TrackerService

_ACTIVE_STATUSES = [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
_STOPPABLE_STATUSES = [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS]


@click.command(
    help=(
        "Lower concurrency for an active run, then force stop the newest building or in-progress tasks until the "
        "run is back at the new limit. Evaluating tasks are never stopped. \n\n"
        "Example:\nvalkyrie run shed 123e4567-e89b-12d3-a456-426614174000 --concurrency 10 --dry-run"
    )
)
@click.argument("run_id", type=UUID)
@click.option(
    "--concurrency",
    type=click.IntRange(min=1),
    required=True,
    help="New maximum number of concurrent tasks",
)
@click.option("--dry-run", is_flag=True, help="Show the tasks that would be stopped without changing the run")
def shed(run_id: UUID, concurrency: int, dry_run: bool) -> None:
    """Lower concurrency, then force stop the newest tasks above the new limit."""
    try:
        planned = _newest_over_limit(asyncio.run(_active_tasks(run_id)), concurrency)
        if dry_run:
            _print_tasks(planned, "Would force stop")
            return
        if not click.confirm(
            f"Set concurrency to {concurrency} and force stop the {len(planned)} newest task(s) in run {run_id}?"
        ):
            click.echo("Cancelled.")
            return

        with TrackerService() as tracker:
            response = tracker.update_benchmark_concurrency(run_id, concurrency)
            click.echo(click.style(f"✓ Run concurrency updated to {response.concurrency}.", fg="green", bold=True))
            victims = _newest_over_limit(asyncio.run(_active_tasks(run_id)), response.concurrency)
            if victims:
                _ = tracker.stop_benchmark(run_id, force=True, task_ids=[task.task_id for task in victims])
        _print_tasks(victims, "Force stopped")
    except (TrackerServiceError, ValkyrieSDKError) as error:
        raise click.ClickException(str(error)) from error


def _newest_over_limit(active: list[TaskSummary], concurrency: int) -> list[TaskSummary]:
    """Return the newest stoppable tasks that keep the run above ``concurrency``.

    Tasks are admitted oldest ``started_at`` first, so the largest ``started_at`` values are the newest admissions.
    """
    stoppable = sorted(
        (task for task in active if task.status in _STOPPABLE_STATUSES),
        key=lambda task: (task.started_at, task.id),
        reverse=True,
    )
    return stoppable[: max(len(active) - concurrency, 0)]


async def _active_tasks(run_id: UUID) -> list[TaskSummary]:
    """List active tasks, keyed by task id because status changes can move tasks between offset pages."""
    request = FetchTasksRequest(status=_ACTIVE_STATUSES, limit=500)
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        tasks = {task.task_id: task async for task in client.benchmarks.iter_tasks(run_id, request)}
    return list(tasks.values())


def _print_tasks(tasks: list[TaskSummary], action: str) -> None:
    if not tasks:
        click.echo("No building or in-progress tasks are above the limit.")
        return
    rows = [
        {"Task": terminal_safe(task.task_id, preserve_newlines=False), "Status": task.status.value} for task in tasks
    ]
    click.echo(f"{action}:")
    format_table(rows, ["Task", "Status"], total_count=len(tasks), item_name="task")
