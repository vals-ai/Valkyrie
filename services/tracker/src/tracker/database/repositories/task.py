"""Task and terminal-result persistence operations."""

from collections.abc import Sequence
from uuid import UUID

from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Task,
    TaskStatus,
)


class TaskRepository:
    """Read and write task data through named operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, task_id: UUID) -> Task | None:
        """Return a task by primary key without applying organization scope."""
        return self._session.get(Task, task_id)

    def get_by_id_for_org(self, task_id: UUID, org_id: UUID) -> Task | None:
        """Return a task by primary key only when it belongs to the organization."""
        return self._session.exec(select(Task).where(Task.id == task_id).where(Task.org_id == org_id)).first()

    def get_for_benchmark(self, benchmark_id: UUID, task_id: str, org_id: UUID) -> Task | None:
        """Return a task only when its benchmark and organization both match."""
        return self._session.exec(
            select(Task)
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .where(Task.task_id == task_id)
        ).first()

    def get_nonterminal_for_benchmark(
        self,
        benchmark_id: UUID,
        org_id: UUID,
        *,
        task_ids: Sequence[str] | None = None,
    ) -> list[Task]:
        """Return organization-owned benchmark tasks that have not reached a terminal status."""
        terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
        statement = (
            select(Task)
            .join(Benchmark)
            .where(Task.benchmark == benchmark_id)
            .where(Benchmark.org_id == org_id)
            .where(Task.org_id == org_id)
            .where(col(Task.status).notin_(terminal_statuses))
        )
        if task_ids is not None:
            statement = statement.where(col(Task.task_id).in_(task_ids))
        return list(self._session.exec(statement).all())

    def get_existing_task_ids(self, benchmark_id: UUID, task_ids: Sequence[str], org_id: UUID) -> set[str]:
        """Return requested task IDs that belong to the organization-owned benchmark."""
        if not task_ids:
            return set()

        return set(
            self._session.exec(
                select(Task.task_id)
                .join(Benchmark)
                .where(Task.benchmark == benchmark_id)
                .where(Benchmark.org_id == org_id)
                .where(Task.org_id == org_id)
                .where(col(Task.task_id).in_(task_ids))
            ).all()
        )

    def create_missing_task_rows(
        self,
        benchmark_id: UUID,
        task_ids: Sequence[str],
        org_id: UUID,
        *,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> list[Task]:
        """Create missing rows for an organization-owned benchmark in request order."""
        requested_task_ids = list(dict.fromkeys(task_ids))
        if not requested_task_ids:
            return []

        benchmark_exists = self._session.exec(
            select(Benchmark.id).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org_id).with_for_update()
        ).first()
        if benchmark_exists is None:
            return []

        existing_task_ids = self.get_existing_task_ids(benchmark_id, requested_task_ids, org_id)
        existing_rows = self._session.exec(
            select(Task)
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .where(col(Task.task_id).in_(existing_task_ids))
        ).all()

        rows_by_task_id = {task.task_id: task for task in existing_rows}
        for task_id in requested_task_ids:
            if task_id not in existing_task_ids:
                task = Task(org_id=org_id, task_id=task_id, benchmark=benchmark_id, status=status)
                self._session.add(task)
                rows_by_task_id[task_id] = task

        self._session.flush()

        return [rows_by_task_id[task_id] for task_id in requested_task_ids]

    def get_runnable_for_benchmark(
        self,
        benchmark_id: UUID,
        task_ids: Sequence[str],
        org_id: UUID,
        *,
        statuses: Sequence[TaskStatus] = (TaskStatus.PENDING, TaskStatus.EVALUATING),
    ) -> list[tuple[str, Task]]:
        """Return requested runnable task rows in caller-provided order."""
        requested_task_ids = list(dict.fromkeys(task_ids))
        if not requested_task_ids or not statuses:
            return []

        task_rows = self._session.exec(
            select(Task.task_id, Task)
            .join(Benchmark)
            .where(Task.benchmark == benchmark_id)
            .where(Benchmark.org_id == org_id)
            .where(Task.org_id == org_id)
            .where(col(Task.task_id).in_(requested_task_ids))
            .where(col(Task.status).in_(statuses))
        ).all()

        task_rows_by_id = {task_id: task_row for task_id, task_row in task_rows}
        return [(task_id, task_rows_by_id[task_id]) for task_id in requested_task_ids if task_id in task_rows_by_id]

    def get_terminal_result(self, task: Task, org_id: UUID) -> tuple[EvaluationResult | None, str | None]:
        """Return the newest evaluation result or error message for a terminal task."""
        if task.org_id != org_id or task.status not in (TaskStatus.FINISHED, TaskStatus.ERROR):
            return None, None

        if task.status == TaskStatus.FINISHED:
            result = self._session.exec(
                select(EvaluationResult)
                .where(EvaluationResult.task == task.id)
                .where(EvaluationResult.org_id == org_id)
                .order_by(col(EvaluationResult.created_at).desc())
            ).first()
            return result, None

        error_message = self._session.exec(
            select(ErrorResult.error_message)
            .where(ErrorResult.task == task.id)
            .where(ErrorResult.org_id == org_id)
            .order_by(col(ErrorResult.created_at).desc())
        ).first()

        return None, error_message
