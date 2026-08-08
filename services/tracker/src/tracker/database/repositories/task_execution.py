"""Authority-fenced task execution persistence operations."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.sql.dml import Update
from sqlmodel import Session, col, select, update

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.execution_authority import ExecutionAuthority


class TaskExecutionRepository:
    """Persist executor task state within a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def lock_execution_authority(
        self,
        authority: ExecutionAuthority,
        *,
        require_in_progress: bool = True,
    ) -> Benchmark:
        """Lock the benchmark and prove that the exact dispatch remains authoritative."""
        benchmark = self._session.exec(
            select(Benchmark).where(col(Benchmark.id) == authority.benchmark_id).with_for_update()
        ).one_or_none()
        if (
            benchmark is None
            or benchmark.status == BenchmarkStatus.STOPPED
            or (require_in_progress and benchmark.status not in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING))
        ):
            raise ExecutionAuthorityRevoked("Executor benchmark authority was revoked")

        dispatch_id = self._session.exec(
            select(ExecutorDispatch.id)
            .where(col(ExecutorDispatch.id) == authority.dispatch_id)
            .where(col(ExecutorDispatch.benchmark_id) == authority.benchmark_id)
            .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
            .with_for_update()
        ).one_or_none()
        if dispatch_id is None:
            raise ExecutionAuthorityRevoked("Executor dispatch authority was revoked")
        return benchmark

    def get_for_execution(self, task_id: UUID, org_id: UUID) -> Task | None:
        """Return a task only when it belongs to the requested organization."""
        return self._session.exec(select(Task).where(col(Task.id) == task_id).where(col(Task.org_id) == org_id)).first()

    def get_for_benchmark(self, benchmark_id: UUID, task_id: str, org_id: UUID) -> Task | None:
        """Return a task only when its benchmark and organization both match."""
        return self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.task_id) == task_id)
            .where(col(Task.org_id) == org_id)
        ).first()

    def is_current(
        self,
        task_id: UUID,
        org_id: UUID,
        authority: ExecutionAuthority,
        expected_started_at: datetime,
    ) -> bool:
        """Check authority and attempt identity without committing the read transaction."""
        try:
            self.lock_execution_authority(authority)
            task = self.get_for_execution(task_id, org_id)
            is_current = (
                task is not None
                and task.benchmark == authority.benchmark_id
                and task.status != TaskStatus.STOPPED
                and task.started_at == expected_started_at
            )
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        self._session.rollback()
        return is_current

    def save_eval_resume_state(
        self,
        task_id: UUID,
        org_id: UUID,
        state: dict[str, Any],
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime | None = None,
    ) -> bool:
        """Save resumable evaluator state if the task attempt is still writable."""
        try:
            self.lock_execution_authority(authority)
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        task_update = (
            update(Task)
            .where(col(Task.id) == task_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.benchmark) == authority.benchmark_id)
            .where(col(Task.status) != TaskStatus.STOPPED)
        )
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)

        result = self._session.exec(task_update.values(eval_resume_state=state))
        if result.rowcount == 0:
            self._session.rollback()
            return False

        return True

    def transition_status(
        self,
        task_id: UUID,
        org_id: UUID,
        to_status: TaskStatus,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime | None = None,
        expected_status: TaskStatus | None = None,
    ) -> bool:
        """Transition a task status with authority and attempt compare-and-set guards."""
        try:
            self.lock_execution_authority(authority)
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        task_update = (
            update(Task)
            .where(col(Task.id) == task_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.benchmark) == authority.benchmark_id)
        )
        if to_status != TaskStatus.STOPPED:
            task_update = task_update.where(col(Task.status) != TaskStatus.STOPPED)
        if expected_status is not None:
            task_update = task_update.where(col(Task.status) == expected_status)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)

        values: dict[str, TaskStatus | datetime] = {"status": to_status}
        if to_status in (TaskStatus.FINISHED, TaskStatus.ERROR):
            values["finished_at"] = datetime.now(UTC)
        result = self._session.exec(task_update.values(**values))
        if result.rowcount == 0:
            self._session.rollback()
            return False

        return True

    def record_error(
        self,
        task_id: UUID,
        org_id: UUID,
        error_message: str,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime | None = None,
        expected_status: TaskStatus | None = None,
    ) -> bool:
        """Atomically record an error and transition its task to ``ERROR``."""
        try:
            self.lock_execution_authority(authority)
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        task = self.get_for_execution(task_id, org_id)
        if task is None or task.benchmark != authority.benchmark_id:
            self._session.rollback()
            return False
        if not self._transition_without_authority_lock(
            task_id,
            org_id,
            TaskStatus.ERROR,
            authority,
            expected_started_at=expected_started_at,
            expected_status=expected_status,
        ):
            return False

        self._session.add(ErrorResult(org_id=org_id, task=task_id, error_message=error_message))
        return True

    def attach_breakdown_and_transition(
        self,
        task_id: UUID,
        org_id: UUID,
        breakdown: TaskBreakdown,
        to_status: TaskStatus,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime,
        expected_status: TaskStatus | None = None,
    ) -> bool:
        """Atomically attach a breakdown and transition the task."""
        try:
            self.lock_execution_authority(authority)
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        self._session.add(breakdown)
        self._session.flush()
        task_update = self._task_update(
            task_id,
            org_id,
            to_status,
            authority,
            expected_started_at=expected_started_at,
            expected_status=expected_status,
        ).values(task_breakdown=breakdown.id)
        result = self._session.exec(task_update)
        if result.rowcount == 0:
            self._session.rollback()
            return False

        return True

    def record_evaluation_and_finish(
        self,
        task_id: UUID,
        org_id: UUID,
        result: EvaluationResult,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime,
        evaluation_run_duration: float | None = None,
        sandbox_run_duration: float | None = None,
        expected_status: TaskStatus | None = TaskStatus.EVALUATING,
    ) -> bool:
        """Atomically record an evaluation, update durations, and finish the task."""
        try:
            self.lock_execution_authority(authority)
        except ExecutionAuthorityRevoked:
            self._session.rollback()
            return False

        task = self.get_for_execution(task_id, org_id)
        if task is None or task.benchmark != authority.benchmark_id:
            self._session.rollback()
            return False

        existing_breakdown = None
        if task.task_breakdown is not None:
            existing_breakdown = self._session.get(TaskBreakdown, task.task_breakdown)
            if existing_breakdown is None:
                self._session.rollback()
                return False

        if not self._transition_without_authority_lock(
            task_id,
            org_id,
            TaskStatus.FINISHED,
            authority,
            expected_started_at=expected_started_at,
            expected_status=expected_status,
        ):
            return False

        result.org_id = org_id
        result.task = task_id
        self._session.add(result)
        if existing_breakdown is not None:
            duration_values: dict[str, float] = {}
            if evaluation_run_duration is not None:
                duration_values["evaluation_run_duration"] = evaluation_run_duration
            if sandbox_run_duration is not None:
                duration_values["sandbox_run_duration"] = sandbox_run_duration
            if duration_values:
                update_result = self._session.exec(
                    update(TaskBreakdown)
                    .where(col(TaskBreakdown.id) == existing_breakdown.id)
                    .values(**duration_values)
                )
                if update_result.rowcount == 0:
                    self._session.rollback()
                    return False

        return True

    def _task_update(
        self,
        task_id: UUID,
        org_id: UUID,
        to_status: TaskStatus,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime | None,
        expected_status: TaskStatus | None,
    ) -> Update:
        """Build the shared task status compare-and-set statement."""
        task_update = update(Task).where(col(Task.id) == task_id).where(col(Task.org_id) == org_id)
        task_update = task_update.where(col(Task.benchmark) == authority.benchmark_id)
        if to_status != TaskStatus.STOPPED:
            task_update = task_update.where(col(Task.status) != TaskStatus.STOPPED)
        if expected_status is not None:
            task_update = task_update.where(col(Task.status) == expected_status)
        if expected_started_at is not None:
            task_update = task_update.where(col(Task.started_at) == expected_started_at)

        values: dict[str, TaskStatus | datetime] = {"status": to_status}
        if to_status in (TaskStatus.FINISHED, TaskStatus.ERROR):
            values["finished_at"] = datetime.now(UTC)

        return task_update.values(**values)

    def _transition_without_authority_lock(
        self,
        task_id: UUID,
        org_id: UUID,
        to_status: TaskStatus,
        authority: ExecutionAuthority,
        *,
        expected_started_at: datetime | None,
        expected_status: TaskStatus | None,
    ) -> bool:
        """Apply a status CAS after the caller has acquired the authority fence."""
        result = self._session.exec(
            self._task_update(
                task_id,
                org_id,
                to_status,
                authority,
                expected_started_at=expected_started_at,
                expected_status=expected_status,
            )
        )
        if result.rowcount == 0:
            self._session.rollback()
            return False

        return True
