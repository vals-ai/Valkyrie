"""Database-only stop, retry, and resume persistence operations."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import inspect
from sqlalchemy.orm.base import NO_VALUE
from sqlmodel import Session, col, func, or_, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    FinalEvaluation,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.database.repositories.benchmark import BenchmarkRepository
from tracker.database.repositories.task import TaskRepository
from tracker.exceptions import TrackerServiceError


@dataclass(frozen=True)
class RetrySelection:
    """Locked benchmark and the task rows/IDs selected for a retry or resume."""

    benchmark: Benchmark
    existing_tasks: list[Task]
    new_task_ids: list[str]


class RunControlRepository:
    """Persist run-control state transitions without owning the transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._benchmarks = BenchmarkRepository(session)
        self._tasks = TaskRepository(session)

    def lock_benchmark(self, benchmark_id: UUID, org_id: UUID) -> Benchmark | None:
        """Return an organization-owned benchmark while acquiring its row lock."""
        return self._benchmarks.get_for_org(benchmark_id, org_id, for_update=True)

    def apply_stop(
        self,
        benchmark: Benchmark,
        org_id: UUID,
        *,
        force: bool,
        task_ids: Sequence[str] | None,
    ) -> int:
        """Stop graceful-stop tasks and optionally transition the whole run to STOPPING."""
        self._require_benchmark_owner(benchmark, org_id)
        task_update = (
            update(Task)
            .where(col(Task.benchmark) == benchmark.id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status).in_([TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.EVALUATING]))
        )
        if task_ids is not None:
            task_update = task_update.where(col(Task.task_id).in_(task_ids))

        result = self._session.exec(task_update.values(status=TaskStatus.STOPPED))
        affected = result.rowcount
        if task_ids is None and (affected > 0 or force):
            benchmark.status = BenchmarkStatus.STOPPING
            self._session.add(benchmark)

        return affected

    def stop_active_tasks(
        self,
        benchmark_id: UUID,
        org_id: UUID,
        *,
        task_ids: Sequence[str] | None,
    ) -> int:
        """Force-stop building, in-progress, and evaluating tasks in one benchmark."""
        task_update = (
            update(Task)
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status).in_([TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]))
        )
        if task_ids is not None:
            task_update = task_update.where(col(Task.task_id).in_(task_ids))
        return self._session.exec(task_update.values(status=TaskStatus.STOPPED)).rowcount

    def count_nonterminal_tasks(self, benchmark_id: UUID, org_id: UUID) -> int:
        """Count tasks that are not finished, errored, or stopped."""
        terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
        return self._session.exec(
            select(func.count(col(Task.id)))
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status).notin_(terminal_statuses))
        ).one()

    def mark_stopped(self, benchmark: Benchmark) -> None:
        """Mark a locked benchmark stopped without committing the transaction."""
        benchmark.status = BenchmarkStatus.STOPPED
        self._session.add(benchmark)

    def select_retryable(
        self,
        benchmark: Benchmark,
        org_id: UUID,
        *,
        retry: bool,
        rerun_task_ids: Sequence[str],
    ) -> RetrySelection:
        """Select retryable tasks using the existing retry status matrix."""
        self._require_benchmark_owner(benchmark, org_id)
        filters = [
            col(Task.benchmark) == benchmark.id,
            col(Task.org_id) == org_id,
        ]
        requested_ids = list(rerun_task_ids)
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            filters.append(col(Task.status) == TaskStatus.ERROR)
            if requested_ids:
                filters.append(col(Task.task_id).in_(requested_ids))
        elif retry and requested_ids:
            filters.append(col(Task.task_id).in_(requested_ids))
        else:
            retry_statuses = [TaskStatus.STOPPED]
            if retry:
                retry_statuses.append(TaskStatus.ERROR)
            filters.append(or_(col(Task.status).in_(retry_statuses), col(Task.task_id).in_(requested_ids)))

        existing_tasks = list(
            self._session.exec(select(Task).where(*filters).order_by(col(Task.started_at), col(Task.task_id))).all()
        )
        existing_ids = {task.task_id for task in existing_tasks}
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            missing_task_ids = [task_id for task_id in requested_ids if task_id not in existing_ids]
            if missing_task_ids:
                raise TrackerServiceError(
                    f"{', '.join(missing_task_ids)} cannot be retried while run {benchmark.id} is in progress "
                    "because they are not in ERROR status"
                )
            new_task_ids: list[str] = []
        else:
            new_task_ids = list(dict.fromkeys(task_id for task_id in requested_ids if task_id not in existing_ids))

        return RetrySelection(benchmark, existing_tasks, new_task_ids)

    def apply_retry(
        self,
        selection: RetrySelection,
        org_id: UUID,
        *,
        retry_mode: RetryMode,
    ) -> None:
        """Apply retry state after the caller verifies task IDs externally."""
        benchmark = selection.benchmark
        self._require_benchmark_owner(benchmark, org_id)
        if not selection.existing_tasks and not selection.new_task_ids:
            return

        evaluations = self._session.exec(
            select(FinalEvaluation)
            .where(FinalEvaluation.benchmark == benchmark.id)
            .where(FinalEvaluation.org_id == org_id)
        ).all()
        for evaluation in evaluations:
            self._session.delete(evaluation)
        benchmark_state = inspect(benchmark)
        loaded_evaluation = (
            benchmark_state.attrs.final_evaluation.loaded_value if benchmark_state is not None else NO_VALUE
        )
        if evaluations and isinstance(loaded_evaluation, FinalEvaluation) and loaded_evaluation.org_id == org_id:
            benchmark.final_evaluation = None

        if benchmark.status != BenchmarkStatus.IN_PROGRESS:
            benchmark.status = BenchmarkStatus.IN_PROGRESS
        benchmark.finished_at = None
        self._session.add(benchmark)

        for task in selection.existing_tasks:
            task.status = (
                TaskStatus.EVALUATING
                if retry_mode == RetryMode.AUTO and task.eval_resume_state is not None
                else TaskStatus.PENDING
            )
            task.started_at = datetime.now(ZoneInfo("UTC"))
            task.finished_at = None
            if retry_mode == RetryMode.FROM_SCRATCH:
                task.eval_resume_state = None
            self._session.add(task)

        self._tasks.create_missing_task_rows(
            benchmark.id,
            selection.new_task_ids,
            org_id,
            status=TaskStatus.PENDING,
        )

    @staticmethod
    def _require_benchmark_owner(benchmark: Benchmark, org_id: UUID) -> None:
        if benchmark.org_id != org_id:
            raise TrackerServiceError(f"Benchmark {benchmark.id} does not belong to organization {org_id}")
