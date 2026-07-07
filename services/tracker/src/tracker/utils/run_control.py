"""Operations that stop, resume, or retry a run and tear down its sandboxes."""

import traceback
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from benchmark_service import (
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from sqlmodel import Session, asc, col, func, or_, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.sandbox import delete_sandbox
from tracker.types import (
    AWSCredentials,
)

from tracker.utils.resources import fetch_sandbox_provider_config

logger = get_logger(__name__)


async def initiate_stop_benchmark(benchmark_row: Benchmark, session: Session, force: bool, org: Org) -> None:
    """
    Sets the flags to initiate the stopping process for a benchmark.

    Benchmark - Stopping status
    Tasks - Stopped status

    NOTE: Tasks that have already started will continue to run and finish.
    """
    try:
        # Update all rows where tasks are pending or building to stopped
        result = session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
            .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.EVALUATING]))
            .values(status=TaskStatus.STOPPED)
        )
        session.commit()

        # If we have stopped any tasks or are forcing the benchmark to stop, set the benchmark status to stopping
        if result.rowcount > 0 or force:
            benchmark_row.status = BenchmarkStatus.STOPPING
            session.add(benchmark_row)
            session.commit()
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping run {benchmark_row.id}: {str(e)}") from e


async def stop_sandbox(sandbox: Sandbox, provider: SandboxProvider) -> str | None:
    try:
        await delete_sandbox(sandbox, provider)
        return None
    except SandboxNotFoundError:
        logger.warning(f"Sandbox `{sandbox.name}` has already been terminated")
        return None
    except Exception as e:
        return f"{str(e)}: {traceback.format_exc()}"


async def sandbox_generator(benchmark_row: Benchmark, provider: SandboxProvider) -> AsyncGenerator[Sandbox, None]:
    """
    Generator that yields all sandboxes for a given benchmark.
    """
    query = SandboxQuery(labels={"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id)})
    async for sandbox in provider.list_sandboxes(query):
        yield sandbox


async def force_stop_sandboxes(
    benchmark_row: Benchmark,
    session: Session,
    sandbox_provider_secret_name: str,
    aws: AWSCredentials,
    org: Org,
    sandbox_provider: str = "daytona",
) -> None:
    """
    Stops and deletes all sandboxes which are in progress or evaluating.
    NOTE: If task is not in progress but sandbox exists, we kill it and leave the task status as is.

    Raises:
        TrackerServiceError: If there are any errors stopping the sandboxes
    """
    benchmark_service = benchmark_row.benchmark_service()
    provider = benchmark_service.get_sandbox_provider(
        fetch_sandbox_provider_config(sandbox_provider_secret_name, aws, sandbox_provider)
    )

    # Update all tasks being processed to stopped
    session.exec(
        update(Task)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).in_([TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]))
        .values(status=TaskStatus.STOPPED)
    )

    session.commit()

    # Iterate through each running sandbox and stop it, collecting error messages
    results: dict[str, str | None] = {}
    try:
        async for sandbox in sandbox_generator(benchmark_row, provider):
            result = await stop_sandbox(sandbox, provider)
            results[sandbox.name] = result
    finally:
        await benchmark_service.close()

    error_message: str = "\n".join(
        f"{task_alias}: {error_message}" for task_alias, error_message in results.items() if error_message
    )

    # If all tasks are already in a stopped state, we need to update the final status here since the worker has exited
    finished_statuses: list[TaskStatus] = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    tasks_still_running: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).notin_(finished_statuses))
    ).one()

    if not tasks_still_running:
        benchmark_row.status = BenchmarkStatus.STOPPED
        session.add(benchmark_row)
        session.commit()

    if error_message:
        raise TrackerServiceError(f"Unexpected errors stopping sandboxes:\n{error_message}")


async def reset_to_in_progress_status(
    benchmark_row: Benchmark,
    session: Session,
    benchmark_service: BenchmarkServiceClient,
    retry: bool,
    retry_mode: RetryMode,
    rerun_task_ids: list[str],
    org: Org,
) -> list[str]:
    """
    Resets valid tasks to in progress and to allow for retrying or resuming the benchmark.

    Retry: we reset objects with an error status ontop of the stopped status
    Rerun Task IDs: even if task has been finished we restart it. If the task has no
        row yet, a fresh PENDING row is created when valid in the current dataset.

    Benchmark - In progress status
    Tasks - Pending status, or Evaluating status when retrying durable eval state

    NOTE: Will raise if benchmark is in a stopped state with no stopped tasks.
    """
    try:
        existing_rows = session.exec(
            select(Task)
            .where(*_retry_task_filters(benchmark_row, retry, rerun_task_ids, org))
            .order_by(asc(Task.started_at))
        ).all()
        existing_by_task_id: dict[str, Task] = {task.task_id: task for task in existing_rows}

        if benchmark_row.status == BenchmarkStatus.IN_PROGRESS:
            missing_task_ids = [task_id for task_id in rerun_task_ids if task_id not in existing_by_task_id]
            if missing_task_ids:
                raise TrackerServiceError(
                    f"{', '.join(missing_task_ids)} cannot be retried while run {benchmark_row.id} is in progress because they are not in ERROR status"
                )
            new_task_ids = []
        else:
            new_task_ids = [tid for tid in rerun_task_ids if tid not in existing_by_task_id]

        # Allow re-running the end of the benchmark without running any tasks
        if not existing_rows and not new_task_ids:
            return []

        # Verify the task ids are still valid before priming to resume
        # Raises if any task ids are invalid
        all_requested_task_ids = [task.task_id for task in existing_rows] + new_task_ids
        verify_response = await benchmark_service.verify_task_ids(
            task_ids=all_requested_task_ids, slice_str=None, dataset=benchmark_row.arguments.dataset
        )

        # Can already be in progress when retrying errored tasks while the run is ongoing.
        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            benchmark_row.status = BenchmarkStatus.IN_PROGRESS
            session.add(benchmark_row)
            session.commit()

        for task in existing_rows:
            task.status = (
                TaskStatus.EVALUATING
                if retry_mode == RetryMode.AUTO and task.eval_resume_state is not None
                else TaskStatus.PENDING
            )
            task.started_at = datetime.now(ZoneInfo("UTC"))
            task.finished_at = None
            if retry_mode == RetryMode.FROM_SCRATCH:
                task.eval_resume_state = None
            session.add(task)

        for task_id in new_task_ids:
            session.add(Task(org_id=org.id, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.PENDING))

        session.commit()

        return verify_response.task_ids
    except (TrackerServiceError, BenchmarkServiceError):
        raise
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error resuming run {benchmark_row.id}: {str(e)}") from e


def _retry_task_filters(benchmark_row: Benchmark, retry: bool, rerun_task_ids: list[str], org: Org) -> list[Any]:
    """Select retryable rows.

    Active retries on in-progress runs are limited to ERROR tasks. Finished tasks must wait until the run is terminal.
    """
    filters = [
        col(Task.benchmark) == benchmark_row.id,
        col(Task.org_id) == org.id,
    ]
    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS:
        filters.append(col(Task.status) == TaskStatus.ERROR)
        if rerun_task_ids:
            filters.append(col(Task.task_id).in_(rerun_task_ids))
        return filters

    if retry and rerun_task_ids:
        filters.append(col(Task.task_id).in_(rerun_task_ids))
        return filters

    retry_statuses = [TaskStatus.STOPPED]
    if retry:
        retry_statuses.append(TaskStatus.ERROR)

    filters.append(or_(col(Task.status).in_(retry_statuses), col(Task.task_id).in_(rerun_task_ids)))
    return filters
