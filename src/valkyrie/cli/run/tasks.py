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
    RollbackTaskResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskResultsResponse,
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


@click.command(name="task-history")
@click.argument("run_id", type=UUID)
@click.argument("task_id")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def task_history(run_id: UUID, task_id: str, output_format: str) -> None:
    """List every kept evaluation attempt for one task, newest first."""
    try:
        response = asyncio.run(_task_history(run_id, task_id))
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    click.echo(f"task_id: {terminal_safe(response.task_id, preserve_newlines=False)}")
    click.echo(f"status: {response.status.value}")
    rows = [
        {
            "Result ID": str(entry.id),
            "Evaluated At": entry.created_at.isoformat(),
            "Current": "yes" if entry.current else "",
            "Result": terminal_safe(json.dumps(entry.result, ensure_ascii=False), preserve_newlines=False),
        }
        for entry in response.results
    ]
    format_table(rows, ["Result ID", "Evaluated At", "Current", "Result"], item_name="evaluation attempt")


@click.command(name="rollback-task")
@click.argument("run_id", type=UUID)
@click.argument("task_id")
@click.option(
    "--result-id",
    type=UUID,
    default=None,
    help="Evaluation to restore (see `task-history`). Defaults to the attempt before the current one.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def rollback_task(run_id: UUID, task_id: str, result_id: UUID | None, output_format: str) -> None:
    """Undo a rerun: make an earlier evaluation the task's current result and restore its artifacts.

    The run's final score is discarded; run `valk run resume RUN_ID` afterwards to recompute it.
    """
    try:
        response = asyncio.run(_rollback_task(run_id, task_id, result_id))
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    _print_detail(response, output_format)
    if output_format == "text":
        if not response.artifacts_versioned:
            click.echo("Warning: bucket is not versioned; S3 artifacts were left unchanged.", err=True)
        click.echo(f"Run `valk run resume {run_id}` to recompute the final score.", err=True)


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


async def _task_history(run_id: UUID, task_id: str) -> TaskResultsResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        return await client.benchmarks.task_results(run_id, task_id)


async def _rollback_task(run_id: UUID, task_id: str, result_id: UUID | None) -> RollbackTaskResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        return await client.benchmarks.rollback_task(run_id, task_id, result_id)
