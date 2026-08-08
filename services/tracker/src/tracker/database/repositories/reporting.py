"""Read-model queries for benchmark reporting and listings."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence, cast
from uuid import UUID

from sqlalchemy import JSON, literal, tuple_, type_coerce
from sqlalchemy.orm import selectinload, with_loader_criteria
from sqlmodel import Session, asc, col, desc, func, or_, select

from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    FinalEvaluation,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.types import AverageTaskBreakdown, FetchBenchmarksRequest, Order


@dataclass(frozen=True)
class BenchmarkPage:
    """A page of organization-scoped benchmarks and pagination metadata."""

    rows: Sequence[Benchmark]
    total_count: int | None
    has_next_page: bool


@dataclass(frozen=True)
class BenchmarkTaskCounts:
    """Task totals and status counts for an organization-owned benchmark."""

    total_tasks: int
    finished_tasks: int
    failed_tasks: int
    status_counts: dict[TaskStatus, int]


class ReportingRepository:
    """Read benchmark reporting data through named organization-scoped queries."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def fetch_evaluation_results(self, benchmark_id: UUID, org_id: UUID) -> dict[str, dict[str, Any]]:
        """Select the latest successful evaluation result and history for each finished task."""
        statement = (
            select(EvaluationResult, Task.id, Task.task_id, TaskBreakdown)
            .join(Task, col(EvaluationResult.task) == col(Task.id))
            .outerjoin(TaskBreakdown, col(Task.task_breakdown) == col(TaskBreakdown.id))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .where(EvaluationResult.org_id == org_id)
            .where(Task.status == TaskStatus.FINISHED)
            .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
        )

        results = cast(
            Sequence[tuple[EvaluationResult, UUID, str, TaskBreakdown | None]],
            self._session.exec(statement).all(),  # pyright: ignore[reportUnknownArgumentType]
        )

        latest_results: list[tuple[EvaluationResult, UUID, str, TaskBreakdown | None]] = []
        seen_task_row_ids: set[UUID] = set()
        for row in results:
            task_row_id = row[1]
            if task_row_id not in seen_task_row_ids:
                latest_results.append(row)
                seen_task_row_ids.add(task_row_id)

        histories = self._fetch_result_histories(
            list(seen_task_row_ids),
            {evaluation_result.id for evaluation_result, _task_row_id, _task_id, _breakdown in latest_results},
            org_id,
        )

        evaluation_results: dict[str, dict[str, Any]] = {}
        for evaluation_result, task_row_id, task_id, task_breakdown in latest_results:
            result_data = dict(evaluation_result.result)
            result_data["agent_caused_exit_reason"] = evaluation_result.agent_caused_exit_reason
            if task_breakdown is not None:
                result_data["task_breakdown"] = task_breakdown.model_dump()
            task_history = histories.get(task_row_id, [])
            result_data["attempts"] = len(task_history) + 1
            if task_history:
                result_data["history"] = task_history
            evaluation_results[task_id] = result_data

        return evaluation_results

    def _fetch_result_histories(
        self,
        task_row_ids: Sequence[UUID],
        current_evaluation_result_ids: set[UUID],
        org_id: UUID,
    ) -> dict[UUID, list[dict[str, Any]]]:
        """Fetch prior evaluation and error attempts for a set of tasks."""
        evaluation_statement = cast(
            Any,
            select(EvaluationResult.id, EvaluationResult.task, EvaluationResult.created_at, EvaluationResult.result)
            .where(col(EvaluationResult.task).in_(task_row_ids))
            .where(col(EvaluationResult.org_id) == org_id),
        )
        if current_evaluation_result_ids:
            evaluation_statement = evaluation_statement.where(
                col(EvaluationResult.id).notin_(current_evaluation_result_ids)
            )

        evaluation_rows = cast(
            Sequence[tuple[UUID, UUID, datetime, dict[str, Any]]],
            cast(Any, self._session.exec(evaluation_statement)).all(),
        )
        error_rows = self._session.exec(
            select(ErrorResult.task, ErrorResult.created_at, ErrorResult.error_message)
            .where(col(ErrorResult.task).in_(task_row_ids))
            .where(col(ErrorResult.org_id) == org_id)
        ).all()

        histories: dict[UUID, list[dict[str, Any]]] = {}
        for _result_id, task_row_id, created_at, result in evaluation_rows:
            histories.setdefault(task_row_id, []).append({"created_at": created_at.isoformat(), "result": dict(result)})
        for task_row_id, created_at, error_message in error_rows:
            histories.setdefault(task_row_id, []).append(
                {"created_at": created_at.isoformat(), "error_message": error_message}
            )

        return {
            task_row_id: sorted(entries, key=lambda entry: entry["created_at"], reverse=True)
            for task_row_id, entries in histories.items()
            if entries
        }

    def get_benchmark_task_counts(self, benchmark_id: UUID, org_id: UUID) -> BenchmarkTaskCounts:
        """Count tasks by status for an organization-owned benchmark."""
        rows = self._session.exec(
            select(Task.status, func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .group_by(Task.status)
        ).all()
        status_counts = {TaskStatus(status): count for status, count in rows}

        return BenchmarkTaskCounts(
            total_tasks=sum(status_counts.values()),
            finished_tasks=sum(
                status_counts.get(status, 0) for status in (TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)
            ),
            failed_tasks=status_counts.get(TaskStatus.ERROR, 0),
            status_counts=status_counts,
        )

    def fetch_final_score_inputs(self, benchmark_id: UUID, org_id: UUID) -> dict[str, dict[str, Any] | None]:
        """Return each task's latest evaluation result, or None when it is unfinished."""
        task_rows = cast(
            Sequence[tuple[UUID, str, TaskStatus]],
            self._session.exec(
                select(Task.id, Task.task_id, Task.status)
                .where(Task.benchmark == benchmark_id)
                .where(Task.org_id == org_id)
            ).all(),
        )
        finished_task_ids = [
            task_row_id for task_row_id, _task_id, status in task_rows if status == TaskStatus.FINISHED
        ]
        result_rows: Sequence[tuple[UUID, dict[str, Any]]] = []
        if finished_task_ids:
            result_statement = cast(
                Any,
                select(EvaluationResult.task, EvaluationResult.result)
                .where(col(EvaluationResult.task).in_(finished_task_ids))
                .where(EvaluationResult.org_id == org_id)
                .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id)),
            )
            result_rows = cast(
                Sequence[tuple[UUID, dict[str, Any]]],
                cast(Any, self._session.exec(result_statement)).all(),
            )

        latest_results: dict[UUID, dict[str, Any]] = {}
        for task_row_id, result in result_rows:
            latest_results.setdefault(task_row_id, result)

        return {
            task_id: latest_results.get(task_row_id) if status == TaskStatus.FINISHED else None
            for task_row_id, task_id, status in task_rows
        }

    def fetch_average_task_breakdown(self, benchmark_id: UUID, org_id: UUID) -> AverageTaskBreakdown | None:
        """Fetch average task metrics for an organization-owned benchmark."""
        row = self._session.exec(
            select(
                func.avg(TaskBreakdown.sandbox_build_duration),
                func.avg(TaskBreakdown.agent_run_duration),
                func.avg(TaskBreakdown.evaluation_run_duration),
                func.avg(TaskBreakdown.sandbox_run_duration),
            )
            .join(Task, col(Task.task_breakdown) == col(TaskBreakdown.id))
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
        ).one()

        if all(value is None for value in row):
            return None

        return AverageTaskBreakdown(
            sandbox_build_duration=row[0],
            agent_run_duration=row[1],
            evaluation_run_duration=row[2],
            sandbox_run_duration=row[3],
        )

    def fetch_filtered_benchmark_rows(
        self,
        request: FetchBenchmarksRequest,
        org_id: UUID,
        *,
        cursor: tuple[datetime, UUID] | None,
    ) -> BenchmarkPage:
        """Fetch filtered benchmarks using the requested offset or keyset pagination mode."""
        query = (
            select(Benchmark)
            .where(Benchmark.org_id == org_id)
            .options(
                selectinload(Benchmark.final_evaluation),
                with_loader_criteria(FinalEvaluation, cast(Any, col(FinalEvaluation.org_id) == org_id)),
            )
            .execution_options(populate_existing=True)
        )
        arguments_json = type_coerce(col(Benchmark.arguments), JSON)

        if request.agent_name:
            if len(request.agent_name) == 1:
                query = query.where(arguments_json["contract"]["name"].as_string() == request.agent_name[0])
            else:
                query = query.where(col(arguments_json["contract"]["name"].as_string()).in_(request.agent_name))

        if request.model:
            query = query.where(arguments_json["contract"]["model"].as_string() == request.model)

        if request.dataset:
            dataset_value = arguments_json["dataset"].as_string()
            if request.dataset == "default":
                query = query.where(or_(dataset_value == "default", dataset_value.is_(None)))
            else:
                query = query.where(dataset_value == request.dataset)

        if request.label is not None:
            query = query.where(func.lower(Benchmark.label) == request.label.lower())

        if request.benchmark_name:
            if len(request.benchmark_name) == 1:
                query = query.where(Benchmark.name == request.benchmark_name[0])
            else:
                query = query.where(col(Benchmark.name).in_(request.benchmark_name))

        if request.status:
            if len(request.status) == 1:
                query = query.where(Benchmark.status == request.status[0])
            else:
                query = query.where(col(Benchmark.status).in_(request.status))

        if request.started_after is not None:
            query = query.where(Benchmark.started_at > request.started_after)

        if request.started_before is not None:
            query = query.where(Benchmark.started_at < request.started_before)

        if request.started_by:
            normalized_emails = [email.strip().lower() for email in request.started_by if email and email.strip()]
            if normalized_emails:
                query = query.where(col(Benchmark.started_by_email).in_(normalized_emails))

        if request.order_by == Order.DESC:
            query = query.order_by(desc(Benchmark.started_at), desc(Benchmark.id))
        else:
            query = query.order_by(asc(Benchmark.started_at), asc(Benchmark.id))

        if request.cursor is not None:
            if cursor is not None:
                cursor_started_at, cursor_id = cursor
                if request.order_by == Order.DESC:
                    query = query.where(
                        tuple_(col(Benchmark.started_at), col(Benchmark.id))
                        < tuple_(literal(cursor_started_at), literal(str(cursor_id)))
                    )
                else:
                    query = query.where(
                        tuple_(col(Benchmark.started_at), col(Benchmark.id))
                        > tuple_(literal(cursor_started_at), literal(str(cursor_id)))
                    )

            rows = list(self._session.exec(query.limit(request.limit + 1)).all())
            has_next_page = len(rows) > request.limit
            if has_next_page:
                rows = rows[: request.limit]

            return BenchmarkPage(rows=rows, total_count=None, has_next_page=has_next_page)

        total_count = self._session.exec(select(func.count()).select_from(query.subquery())).one()
        if not total_count:
            return BenchmarkPage(rows=[], total_count=0, has_next_page=False)

        rows = list(self._session.exec(query.limit(request.limit).offset(request.offset)).all())
        return BenchmarkPage(rows=rows, total_count=total_count, has_next_page=False)

    def get_stopped_task_count(self, benchmark_id: UUID, org_id: UUID) -> int:
        """Count stopped tasks for an organization-owned benchmark."""
        return self._session.exec(
            select(func.count(col(Task.id)))
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == org_id)
            .where(col(Task.status) == TaskStatus.STOPPED)
        ).one()

    def get_task_errors(self, benchmark_id: UUID, org_id: UUID) -> dict[str, str] | None:
        """Return the newest error message for each errored task in a benchmark."""
        error_rows = self._session.exec(
            select(Task.task_id, ErrorResult.error_message)
            .outerjoin(
                ErrorResult,
                (col(ErrorResult.task) == col(Task.id)) & (col(ErrorResult.org_id) == org_id),
            )
            .where(Task.benchmark == benchmark_id)
            .where(Task.org_id == org_id)
            .where(Task.status == TaskStatus.ERROR)
            .order_by(col(Task.id), col(ErrorResult.created_at).desc())
        ).all()
        if not error_rows:
            return None

        errors_by_task_id: dict[str, str] = {}
        for task_id, error_message in error_rows:
            errors_by_task_id.setdefault(task_id, error_message or "No error message was provided")

        return errors_by_task_id
