"""Benchmark and benchmark-task read operations."""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from sqlalchemy import case
from sqlalchemy.orm import contains_eager
from sqlmodel import Session, col, desc, func, select

from tracker.database.models import (
    Benchmark,
    ErrorResult,
    FinalEvaluation,
    Task,
    TaskStatus,
)

_STATUS_SORT_PRIORITY = case(
    {
        TaskStatus.ERROR: 6,
        TaskStatus.STOPPED: 5,
        TaskStatus.FINISHED: 4,
        TaskStatus.EVALUATING: 3,
        TaskStatus.IN_PROGRESS: 2,
        TaskStatus.BUILDING: 1,
        TaskStatus.PENDING: 0,
    },
    value=col(Task.status),
    else_=-1,
)


@dataclass(frozen=True)
class TaskPage:
    """A page of tasks with the newest error message for each row."""

    rows: list[tuple[Task, str | None]]
    total_count: int


class BenchmarkRepository:
    """Read benchmark data through named, organization-scoped operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, benchmark_id: UUID) -> Benchmark | None:
        """Return a benchmark by primary key without applying organization scope."""
        return self._session.get(Benchmark, benchmark_id)

    def get_for_org(self, benchmark_id: UUID, org_id: UUID, *, for_update: bool = False) -> Benchmark | None:
        """Return a benchmark only when it belongs to the requested organization."""
        statement = select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org_id)
        if for_update:
            statement = statement.with_for_update().execution_options(populate_existing=True)
        return self._session.exec(statement).one_or_none()

    def get_for_org_with_final_evaluation(self, benchmark_id: UUID, org_id: UUID) -> Benchmark | None:
        """Return an organization-owned benchmark with its scoped final evaluation eagerly loaded."""
        statement = (
            select(Benchmark)
            .outerjoin(
                FinalEvaluation,
                (col(FinalEvaluation.benchmark) == col(Benchmark.id)) & (col(FinalEvaluation.org_id) == org_id),
            )
            .options(contains_eager(Benchmark.final_evaluation))
            .where(Benchmark.id == benchmark_id)
            .where(Benchmark.org_id == org_id)
            .execution_options(populate_existing=True)
        )
        return self._session.exec(statement).unique().one_or_none()

    def get_for_ids(self, benchmark_ids: list[UUID], org_id: UUID) -> list[Benchmark]:
        """Return matching organization-owned benchmarks for status polling."""
        if not benchmark_ids:
            return []
        return list(
            self._session.exec(
                select(Benchmark).where(Benchmark.org_id == org_id).where(col(Benchmark.id).in_(benchmark_ids))
            ).all()
        )

    def get_task_state_counts(self, benchmark_id: UUID, org_id: UUID) -> dict[TaskStatus, int]:
        """Count tasks by status for an organization-owned benchmark."""
        rows = self._session.exec(
            select(col(Task.status), func.count(col(Task.id)))
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .group_by(col(Task.status))
        ).all()
        return {status: count for status, count in rows}

    def get_final_score(self, benchmark_id: UUID, org_id: UUID) -> float | None:
        """Return the final score for an organization-owned benchmark."""
        score = self._session.exec(
            select(FinalEvaluation.final_score)
            .where(FinalEvaluation.benchmark == benchmark_id)
            .where(FinalEvaluation.org_id == org_id)
        ).first()
        return score if score is not None else None

    def list_tasks(
        self,
        benchmark_id: UUID,
        org_id: UUID,
        *,
        statuses: list[TaskStatus],
        task_id_search: str | None,
        sort: Literal["task_id", "started_at", "duration", "status"],
        sort_dir: Literal["asc", "desc"],
        limit: int,
        offset: int,
    ) -> TaskPage:
        """Return a filtered, sorted page of tasks and their newest error messages."""
        filters = [col(Task.benchmark) == benchmark_id, col(Task.org_id) == org_id]
        if statuses:
            filters.append(col(Task.status).in_(statuses))
        if task_id_search:
            escaped_search = _escape_sql_like_pattern(task_id_search)
            filters.append(col(Task.task_id).ilike(f"%{escaped_search}%", escape="\\"))

        latest_error_message = (
            select(ErrorResult.error_message)
            .where(ErrorResult.task == Task.id)
            .where(ErrorResult.org_id == org_id)
            .order_by(desc(ErrorResult.created_at))
            .limit(1)
            .scalar_subquery()
        )
        sort_expression = {
            "task_id": col(Task.task_id),
            "started_at": col(Task.started_at),
            "duration": func.coalesce(col(Task.finished_at), func.now()) - col(Task.started_at),
            "status": _STATUS_SORT_PRIORITY,
        }[sort]
        primary_order = sort_expression.asc() if sort_dir == "asc" else sort_expression.desc()
        order_by = [primary_order, col(Task.started_at).desc()]

        rows = self._session.exec(
            select(Task, latest_error_message).where(*filters).order_by(*order_by).limit(limit).offset(offset)
        ).all()
        total_count = self._session.exec(select(func.count(col(Task.id))).where(*filters)).one()
        return TaskPage(rows=list(rows), total_count=total_count)

    def get_filter_options(self, org_id: UUID) -> tuple[list[str], list[str]]:
        """Return distinct benchmark and agent names for an organization."""
        benchmark_names = sorted(
            set(self._session.exec(select(Benchmark.name).where(Benchmark.org_id == org_id).distinct()).all())
        )
        benchmarks = self._session.exec(select(Benchmark).where(Benchmark.org_id == org_id)).all()
        agent_names = sorted(
            {
                benchmark.arguments.contract.name
                for benchmark in benchmarks
                if benchmark.arguments and benchmark.arguments.contract.name
            }
        )
        return benchmark_names, agent_names

    def get_task_status_counts(
        self,
        benchmark_ids: list[UUID],
        org_id: UUID,
    ) -> dict[UUID, dict[TaskStatus, int]]:
        """Return task status counts grouped by organization-owned benchmark."""
        if not benchmark_ids:
            return {}
        rows = self._session.exec(
            select(Task.benchmark, Task.status, func.count())
            .where(col(Task.org_id) == org_id)
            .where(col(Task.benchmark).in_(benchmark_ids))
            .group_by(col(Task.benchmark), col(Task.status))
        ).all()
        counts: dict[UUID, dict[TaskStatus, int]] = {}
        for benchmark_id, status, count in rows:
            counts.setdefault(benchmark_id, {})[TaskStatus(status)] = count
        return counts


def _escape_sql_like_pattern(value: str) -> str:
    """Escape SQL LIKE wildcards so search input is treated literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
