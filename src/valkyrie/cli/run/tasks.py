"""Inspect task state and artifact links through Tracker."""

import asyncio
import json
from uuid import UUID

import click
from pydantic import BaseModel
from valkyrie.sdk import ValkyrieClient, ValkyrieSDKError
from valkyrie.sdk.models import (
    FetchTasksRequest,
    Order,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TasksResponse,
    TaskStatus,
)

from valkyrie.cli.display import format_table, terminal_safe
from valkyrie.cli.runtime_config import config_location, tracker_service_url


@click.command(name="tasks")
@click.argument("run_id", type=UUID)
@click.option(
    "--status",
    multiple=True,
    type=click.Choice([status.value for status in TaskStatus], case_sensitive=False),
    help="Filter by task status; repeat for multiple statuses.",
)
@click.option("--search", help="Match part of a task ID.")
@click.option(
    "--sort",
    type=click.Choice(["task_id", "started_at", "duration", "status"]),
    default="started_at",
    show_default=True,
)
@click.option("--order-by", type=click.Choice(["asc", "desc"], case_sensitive=False), default="desc", show_default=True)
@click.option("--limit", type=click.IntRange(1, 500), default=50, show_default=True)
@click.option("--offset", type=click.IntRange(min=0), default=0, show_default=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def tasks(
    run_id: UUID,
    status: tuple[str, ...],
    search: str | None,
    sort: str,
    order_by: str,
    limit: int,
    offset: int,
    output_format: str,
) -> None:
    """List one page of tasks, including their status and error messages."""
    try:
        request = FetchTasksRequest.model_validate(
            {
                "status": list(status) or None,
                "task_id_search": search,
                "sort": sort,
                "sort_dir": Order(order_by),
                "limit": limit,
                "offset": offset,
            }
        )
        response = asyncio.run(_tasks(run_id, request))
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    rows = [
        {
            "Task": terminal_safe(task.task_id, preserve_newlines=False),
            "Status": task.status.value,
            "Error": terminal_safe(task.error_message or "", preserve_newlines=False),
        }
        for task in response.tasks
    ]
    format_table(rows, ["Task", "Status", "Error"], total_count=response.total_count, item_name="task")


async def _tasks(run_id: UUID, request: FetchTasksRequest) -> TasksResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        return await client.benchmarks.tasks(run_id, request)


@click.command(name="task")
@click.argument("run_id", type=UUID)
@click.argument("task_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def task(run_id: UUID, task_id: str, output_format: str) -> None:
    """Inspect one task's status, evaluation result, and failure reason."""
    _show_task(run_id, task_id, output_format, artifacts=False)


@click.command(name="task-artifacts")
@click.argument("run_id", type=UUID)
@click.argument("task_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def task_artifacts(run_id: UUID, task_id: str, output_format: str) -> None:
    """Get temporary download and log links for one task."""
    _show_task(run_id, task_id, output_format, artifacts=True)


def _show_task(run_id: UUID, task_id: str, output_format: str, *, artifacts: bool) -> None:
    try:
        response = asyncio.run(_task(run_id, task_id, artifacts=artifacts))
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    _print_detail(response, output_format)


def _print_detail(response: BaseModel, output_format: str) -> None:
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    for key, value in response.model_dump(mode="json").items():
        rendered = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
        click.echo(f"{key}: {terminal_safe(rendered, preserve_newlines=False)}")


async def _task(run_id: UUID, task_id: str, *, artifacts: bool) -> SingleTaskResponse | TaskArtifactsResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        if artifacts:
            return await client.benchmarks.artifacts(run_id, task_id)
        return await client.benchmarks.task(run_id, task_id)
