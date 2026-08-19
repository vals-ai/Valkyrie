"""Operations that stop, resume, or retry a run and tear down its sandboxes."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from benchmark_service import (
    Sandbox,
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
from tracker.executor.dispatch_control import terminalize_active_dispatches
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.sandbox import delete_sandbox
from tracker.aws.runtime import AWSRuntime

from tracker.utils.resources import fetch_benchmark_row, fetch_sandbox_provider_config

logger = get_logger(__name__)


def apply_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
) -> None:
    """Apply the Stop state transition without committing the transaction."""
    # Stop and recovery both update the benchmark and its tasks. Lock the benchmark
    # first so every lifecycle transition uses the same lock order.
    fetch_benchmark_row(benchmark_row.id, session, org, for_update=True)

    stoppable_statuses = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.EVALUATING]
    if force:
        stoppable_statuses.append(TaskStatus.IN_PROGRESS)

    task_update = (
        update(Task)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status).in_(stoppable_statuses))
    )
    if task_ids:
        task_update = task_update.where(col(Task.task_id).in_(task_ids))

    result = session.exec(task_update.values(status=TaskStatus.STOPPED))

    if force:
        active_tasks = session.exec(
            select(func.count(col(Task.id)))
            .where(col(Task.benchmark) == benchmark_row.id)
            .where(col(Task.org_id) == org.id)
            .where(
                col(Task.status).in_(
                    [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
                )
            )
        ).one()
        if active_tasks == 0:
            benchmark_row.status = BenchmarkStatus.STOPPED
            terminalize_active_dispatches(session, benchmark_row.id)
            session.add(benchmark_row)
    elif task_ids is None and result.rowcount > 0:
        benchmark_row.status = BenchmarkStatus.STOPPING
        session.add(benchmark_row)


async def initiate_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
) -> None:
    """Initiate Stop without interrupting work that already started unless forced."""
    try:
        apply_stop_benchmark(benchmark_row, session, force, org, task_ids)
        session.commit()
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping run {benchmark_row.id}: {str(e)}") from e


async def stop_sandbox(sandbox: Sandbox, provider: SandboxProvider, org: Org) -> None:
    try:
        await delete_sandbox(sandbox, provider, initiated_by="force_stop", org_id=str(org.id))
    except Exception:
        logger.exception("Failed to send force-stop signal for sandbox %s", sandbox.name)


async def sandbox_generator(
    benchmark_row: Benchmark,
    provider: SandboxProvider,
    task_ids: list[str] | None = None,
) -> AsyncGenerator[Sandbox, None]:
    """
    Generator that yields all sandboxes for a given benchmark.
    """
    labels = {"Benchmark": benchmark_row.name, "Id": str(benchmark_row.id)}
    queries = (
        [SandboxQuery(labels={**labels, "Task": task_id}) for task_id in task_ids]
        if task_ids
        else [SandboxQuery(labels=labels)]
    )
    seen_sandbox_ids: set[str] = set()
    for query in queries:
        async for sandbox in provider.list_sandboxes(query):
            if sandbox.id in seen_sandbox_ids:
                continue
            seen_sandbox_ids.add(sandbox.id)
            yield sandbox


async def force_stop_sandboxes(
    benchmark_row: Benchmark,
    sandbox_provider_secret_name: str,
    aws_runtime: AWSRuntime,
    org: Org,
    sandbox_provider: str = "daytona",
    task_ids: list[str] | None = None,
) -> None:
    """Send provider kill signals without coupling provider teardown to DB state."""
    benchmark_service = benchmark_row.benchmark_service()
    try:
        provider = benchmark_service.get_sandbox_provider(
            fetch_sandbox_provider_config(sandbox_provider_secret_name, aws_runtime.clients, sandbox_provider)
        )
        sandboxes = [sandbox async for sandbox in sandbox_generator(benchmark_row, provider, task_ids=task_ids)]
        await asyncio.gather(*(stop_sandbox(sandbox, provider, org) for sandbox in sandboxes))
    except Exception:
        logger.exception("Unable to send force-stop signals for benchmark %s", benchmark_row.id)
    finally:
        try:
            await benchmark_service.close()
        except Exception:
            logger.exception("Unable to close provider client for benchmark %s", benchmark_row.id)


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
        # Serialize retries with final-score persistence for this benchmark.
        benchmark_row = fetch_benchmark_row(benchmark_row.id, session, org, for_update=True)
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

        old_evaluation = benchmark_row.final_evaluation
        if old_evaluation is not None:
            benchmark_row.final_evaluation = None
            session.delete(old_evaluation)

        # Retry/resume always starts a new active execution.
        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.finished_at = None
        session.add(benchmark_row)

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
