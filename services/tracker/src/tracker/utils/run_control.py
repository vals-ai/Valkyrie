"""Operations that stop, resume, or retry a run and tear down its sandboxes."""

import asyncio
from collections.abc import AsyncGenerator
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from benchmark_service import (
    Sandbox,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.client import BenchmarkServiceError
from sqlmodel import Session, asc, col, func, or_, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_control import terminalize_active_dispatches
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.sandbox import delete_sandbox
from tracker.runtime.services import RuntimeServices

from tracker.utils.resources import fetch_benchmark_row

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
    runtime: RuntimeServices,
    org: Org,
    task_ids: list[str] | None = None,
) -> None:
    """Send provider kill signals without coupling provider teardown to DB state."""
    try:
        config = await runtime.get_sandbox_provider_config()
        async with runtime.get_sandbox_provider(config) as provider:
            sandboxes = [sandbox async for sandbox in sandbox_generator(benchmark_row, provider, task_ids=task_ids)]
            await asyncio.gather(*(stop_sandbox(sandbox, provider, org) for sandbox in sandboxes))
    except Exception:
        logger.exception("Unable to send force-stop signals for benchmark %s", benchmark_row.id)


@dataclass(frozen=True)
class RetryState:
    """Verification inputs and eligibility version, never session-owned objects."""

    task_ids: tuple[str, ...]
    version: str


def _retry_candidates(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    rerun_task_ids: list[str],
    org: Org,
    *,
    for_update: bool = False,
) -> tuple[list[Task], list[str]]:
    query = (
        select(Task)
        .where(*_retry_task_filters(benchmark_row, retry, rerun_task_ids, org))
        .order_by(asc(Task.started_at), asc(Task.id))
    )
    if for_update:
        query = query.with_for_update()
    existing_rows = list(session.exec(query).all())
    existing_ids = {task.task_id for task in existing_rows}
    new_task_ids = [task_id for task_id in rerun_task_ids if task_id not in existing_ids]
    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and new_task_ids:
        raise TrackerServiceError(
            f"{', '.join(new_task_ids)} cannot be retried while run {benchmark_row.id} is in progress because they are not in ERROR status"
        )
    return existing_rows, new_task_ids


def prepare_retry_state(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    rerun_task_ids: list[str],
    org: Org,
    *,
    queued_recovery: bool = False,
    for_update: bool = False,
) -> RetryState:
    """Read a retry snapshot without acquiring locks or changing lifecycle state."""
    if queued_recovery:
        query = (
            select(Task)
            .where(Task.benchmark == benchmark_row.id, Task.org_id == org.id)
            .where(
                col(Task.status).in_(
                    [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
                )
            )
            .order_by(asc(Task.started_at), asc(Task.id))
        )
        if for_update:
            query = query.with_for_update()
        rows = list(session.exec(query).all())
        new_task_ids: list[str] = []
    else:
        rows, new_task_ids = _retry_candidates(
            benchmark_row, session, retry, rerun_task_ids, org, for_update=for_update
        )
    version = json.dumps(
        {
            "dispatch": session.exec(
                select(ExecutorDispatch.id)
                .where(ExecutorDispatch.benchmark_id == benchmark_row.id)
                .order_by(col(ExecutorDispatch.created_at).desc(), col(ExecutorDispatch.id).desc())
                .limit(1)
            ).first(),
            "status": benchmark_row.status,
            "started_at": benchmark_row.started_at,
            "finished_at": benchmark_row.finished_at,
            "release": benchmark_row.current_execution_release_id,
            "name": benchmark_row.name,
            "destination": benchmark_row.custom_benchmark_service,
            "dataset": benchmark_row.arguments.dataset,
            "queue_pool_id": benchmark_row.arguments.queue_pool_id,
            "aws_managed": benchmark_row.aws_managed,
            "tasks": [(row.id, row.task_id, row.status, row.started_at, row.eval_resume_state) for row in rows],
        },
        default=str,
        sort_keys=True,
    )
    return RetryState(tuple([row.task_id for row in rows] + new_task_ids), version)


def reset_to_in_progress_status(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    retry_mode: RetryMode,
    rerun_task_ids: list[str],
    org: Org,
    verified_task_ids: list[str],
) -> list[str]:
    """Apply an externally verified retry inside the caller's locked transaction.

    Retry resets error/stopped tasks; new valid task IDs receive fresh PENDING rows.
    Benchmark becomes IN_PROGRESS; durable evaluation tasks retain EVALUATING.
    """
    try:
        # Serialize retries with final-score persistence for this benchmark.
        benchmark_row = fetch_benchmark_row(benchmark_row.id, session, org, for_update=True)
        existing_rows, new_task_ids = _retry_candidates(
            benchmark_row, session, retry, rerun_task_ids, org, for_update=True
        )

        # Allow re-running the end of the benchmark without running any tasks
        if not existing_rows and not new_task_ids:
            if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
                old_evaluation = benchmark_row.final_evaluation
                if old_evaluation is not None:
                    benchmark_row.final_evaluation = None
                    session.delete(old_evaluation)
                benchmark_row.status = BenchmarkStatus.IN_PROGRESS
                benchmark_row.finished_at = None
                session.add(benchmark_row)
            return []

        old_evaluation = benchmark_row.final_evaluation
        if old_evaluation is not None:
            benchmark_row.final_evaluation = None
            session.delete(old_evaluation)

        # Retry/resume always starts a new active execution.
        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            benchmark_row.status = BenchmarkStatus.IN_PROGRESS
            benchmark_row.error_message = None
        benchmark_row.finished_at = None
        session.add(benchmark_row)

        for task in existing_rows:
            task.status = (
                TaskStatus.EVALUATING
                if retry_mode == RetryMode.AUTO and task.eval_resume_state is not None
                else TaskStatus.PENDING
            )
            retry_started_at = datetime.now(ZoneInfo("UTC"))
            comparable_retry_started_at = retry_started_at
            if task.started_at.tzinfo is None and retry_started_at.tzinfo is not None:
                comparable_retry_started_at = retry_started_at.replace(tzinfo=None)
            if comparable_retry_started_at <= task.started_at:
                retry_started_at = task.started_at + timedelta(microseconds=1)
            task.started_at = retry_started_at
            task.finished_at = None
            if retry_mode == RetryMode.FROM_SCRATCH:
                task.eval_resume_state = None
            session.add(task)

        for task_id in new_task_ids:
            session.add(Task(org_id=org.id, task_id=task_id, benchmark=benchmark_row.id, status=TaskStatus.PENDING))

        return verified_task_ids
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
