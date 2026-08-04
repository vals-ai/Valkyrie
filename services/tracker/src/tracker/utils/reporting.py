"""Read-only queries that fetch and shape run data for API responses and listings."""

import asyncio
from asyncio import CancelledError
import base64
import binascii
import io
import json
from collections.abc import AsyncGenerator, Buffer
from datetime import datetime
from functools import cached_property
from typing import Any, NamedTuple, Sequence, cast
from uuid import UUID

from sqlalchemy import JSON, literal, tuple_, type_coerce
from sqlalchemy.orm import selectinload
from sqlmodel import Session, asc, case, col, desc, func, or_, select

from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    create_benchmark_url,
    upload_to_s3,
)
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.scoping import scoped_select
from tracker.logging import get_logger
from tracker.types import (
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FinalViewResponse,
    GetRunResponse,
    HarnessConfig,
    Order,
)

logger = get_logger(__name__)


def _history_result(created_at: datetime, result: dict[str, Any]) -> dict[str, Any]:
    return {"created_at": created_at.isoformat(), "result": dict(result)}


def _history_error(created_at: datetime, error_message: str) -> dict[str, Any]:
    return {"created_at": created_at.isoformat(), "error_message": error_message}


class TaskCounts(NamedTuple):
    total_tasks: int
    finished_tasks: int
    failed_tasks: int


class RunContext:
    _run_row: Benchmark
    _session: Session
    _org: Org

    def __init__(self, run_row: Benchmark, session: Session, org: Org):
        self._run_row = run_row
        self._session = session
        self._org = org

    @property
    def _status(self) -> BenchmarkStatus:
        return self._run_row.status

    @cached_property
    def _task_counts(self) -> TaskCounts:
        finished_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]

        statement = (
            select(
                func.count().label("total_tasks"),
                func.count(case((col(Task.status).in_(finished_statuses), 1))).label("finished_tasks"),
                func.count(case((Task.status == TaskStatus.ERROR, 1))).label("failed_tasks"),
            )
            .select_from(Task)
            .where(Task.benchmark == self._run_row.id)
            .where(Task.org_id == self._org.id)
        )

        result = self._session.exec(statement).one()

        task_counts = TaskCounts(total_tasks=result[0], finished_tasks=result[1], failed_tasks=result[2])

        return task_counts

    @property
    def _task_breakdown(self) -> dict[TaskStatus, int]:
        """
        Returns a mapping between the task status and the number of tasks in that status

        Provides a breakdown of the run.
        """
        statement = (
            select(Task.status, func.count(col(Task.id)))
            .select_from(Task)
            .where(Task.benchmark == self._run_row.id)
            .where(Task.org_id == self._org.id)
            .group_by(Task.status)
            .having(func.count(col(Task.id)) > 0)  # Exclude all with count of 0
        )

        result = self._session.exec(statement).all()

        return {TaskStatus(status): count for status, count in result}

    @cached_property
    def run_details(self) -> BenchmarkDetails:
        return BenchmarkDetails(
            status=self._status,
            started_at=self._run_row.started_at,
            total_tasks=self._task_counts.total_tasks,
            finished_tasks=self._task_counts.finished_tasks,
            task_breakdown=self._task_breakdown,
            docent_reading_status=self._run_row.docent_reading_status,
            docent_reading_url=self._run_row.docent_reading_url,
        )


def _fetch_result_histories(
    session: Session, task_row_ids: Sequence[UUID], current_evaluation_result_ids: set[UUID], org_id: UUID
) -> dict[UUID, list[dict[str, Any]]]:
    """
    Fetch the history of evaluation + error results for the provided task ids, mixing them with the evaluation results we already have.
    """
    # Prep query to fetch all evaluation results for the given tasks
    evaluation_statement = cast(
        Any,
        select(EvaluationResult.id, EvaluationResult.task, EvaluationResult.created_at, EvaluationResult.result)
        .where(col(EvaluationResult.task).in_(task_row_ids))
        .where(col(EvaluationResult.org_id) == org_id),
    )

    # Exclude evaluation results we have already collected
    if current_evaluation_result_ids:
        evaluation_statement = evaluation_statement.where(
            col(EvaluationResult.id).notin_(current_evaluation_result_ids)
        )

    # Fetched evaluation results
    evaluation_query_result = cast(Any, session.exec(evaluation_statement))
    evaluation_rows = cast(
        Sequence[tuple[UUID, UUID, datetime, dict[str, Any]]],
        evaluation_query_result.all(),
    )

    # Fetch all of the error results from the provided task_rows (A task can have a error message and a evaluation result depending on if its been reran)
    error_rows = session.exec(
        select(ErrorResult.task, ErrorResult.created_at, ErrorResult.error_message)
        .where(col(ErrorResult.task).in_(task_row_ids))
        .where(col(ErrorResult.org_id) == org_id)
    ).all()

    # Create a mapping of the task row, time stamp of when the result for the row was created, and the resulting row
    # We do this so that we can easily sort it downstream
    histories: dict[UUID, list[dict[str, Any]]] = {}
    for _result_id, task_row_id, created_at, result in evaluation_rows:
        histories.setdefault(task_row_id, []).append(_history_result(created_at, result))
    for task_row_id, created_at, error_message in error_rows:
        histories.setdefault(task_row_id, []).append(_history_error(created_at, error_message))

    return {
        task_row_id: sorted(entries, key=lambda entry: entry["created_at"], reverse=True)
        for task_row_id, entries in histories.items()
        if entries
    }


def fetch_evaluation_results(run_id: UUID, session: Session, org_id: UUID) -> dict[str, dict[str, Any]]:
    """Select the latest successful evaluation result for each finished task."""
    statement = (
        select(EvaluationResult, Task.id, Task.task_id, TaskBreakdown)
        .join(Task, col(EvaluationResult.task) == col(Task.id))
        .outerjoin(TaskBreakdown, col(Task.task_breakdown) == col(TaskBreakdown.id))
        .where(Task.benchmark == run_id)
        .where(Task.org_id == org_id)
        .where(Task.status == TaskStatus.FINISHED)
        .order_by(desc(EvaluationResult.created_at))
    )

    results = cast(
        Sequence[tuple[EvaluationResult, UUID, str, TaskBreakdown | None]],
        session.exec(statement).all(),  # pyright: ignore[reportUnknownArgumentType]
    )

    latest_results: list[tuple[EvaluationResult, UUID, str, TaskBreakdown | None]] = []
    seen_task_row_ids: set[UUID] = set()
    for row in results:
        task_row_id = row[1]
        if task_row_id not in seen_task_row_ids:
            latest_results.append(row)
            seen_task_row_ids.add(task_row_id)

    histories = _fetch_result_histories(
        session,
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


def fetch_average_task_breakdown(run_id: UUID, session: Session, org_id: UUID) -> AverageTaskBreakdown | None:
    """
    Fetch the average task breakdown for a run.

    Returns None if there are no task metrics available for the run.
    """
    row = session.exec(
        select(
            func.avg(TaskBreakdown.sandbox_build_duration),
            func.avg(TaskBreakdown.agent_run_duration),
            func.avg(TaskBreakdown.evaluation_run_duration),
            func.avg(TaskBreakdown.sandbox_run_duration),
        )
        .join(Task, col(Task.task_breakdown) == col(TaskBreakdown.id))
        .where(Task.benchmark == run_id)
        .where(Task.org_id == org_id)
    ).one()

    if all(v is None for v in row):
        return None

    return AverageTaskBreakdown(
        sandbox_build_duration=row[0],
        agent_run_duration=row[1],
        evaluation_run_duration=row[2],
        sandbox_run_duration=row[3],
    )


async def stream_run_results(
    run_id: UUID,
    session: Session,
    harness_config: HarnessConfig,
    org: Org,
    *,
    canonical: bool = False,
) -> AsyncGenerator[str]:
    """Generate Server-Sent Events with live run updates."""
    PULL_INTERVAL = 5

    EVENT_COMPLETE = "event: complete\n\n"
    EVENT_ERROR = "event: error\ndata:"
    DATA_PREFIX = "data:"
    DISCONNECT = "event: disconnect\n\n"

    try:
        while True:
            with Session(bind=session.bind) as fresh_session:
                fresh_run = fresh_session.get(Benchmark, run_id)
                if not fresh_run or fresh_run.org_id != org.id:
                    yield f"{EVENT_ERROR} {json.dumps({'error': 'Run not found'})}\n\n"
                    break

                fresh_session.refresh(fresh_run)
                run_context = RunContext(fresh_run, fresh_session, org)

                response_data = FetchBenchmarkResponse(
                    benchmark_name=fresh_run.name,
                    benchmark_id=fresh_run.id,
                    details=run_context.run_details,
                    s3_bucket_url=create_benchmark_url(
                        str(fresh_run.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
                    ),
                    label=fresh_run.label,
                    executor_release_id=fresh_run.executor_release_id,
                    current_execution_release_id=fresh_run.current_execution_release_id,
                    executor_artifact_digest=fresh_run.executor_artifact_digest,
                    executor_protocol_version=fresh_run.executor_protocol_version,
                    final_score=fresh_run.final_evaluation.final_score if fresh_run.final_evaluation else None,
                    error_message=fresh_run.error_message if fresh_run.status == BenchmarkStatus.ERROR else None,
                )

                response_json = serialize_run_snapshot(response_data, canonical=canonical)
                yield f"{DATA_PREFIX} {response_json}\n\n"

                if fresh_run.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(PULL_INTERVAL)

    except CancelledError:
        logger.info(f"Client disconnected from run {run_id} stream")
        yield DISCONNECT


def serialize_run_snapshot(response: FetchBenchmarkResponse, *, canonical: bool) -> str:
    """Serialize one SSE snapshot without changing the stored or legacy result shape."""
    if canonical:
        return GetRunResponse.from_legacy(response).model_dump_json()
    return response.model_dump_json()


def encode_cursor(started_at: datetime, row_id: UUID) -> str:
    """Encode a keyset pagination cursor from a started_at timestamp and row id."""
    payload = json.dumps({"started_at": started_at.isoformat(), "id": str(row_id)})
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a keyset pagination cursor into a started_at timestamp and row id."""
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        return datetime.fromisoformat(payload["started_at"]), UUID(payload["id"])
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("Invalid benchmark cursor") from exc


def fetch_filtered_run_rows(
    request: FetchBenchmarksRequest, session: Session, org: Org
) -> tuple[Sequence[Benchmark], int | None, str | None]:
    """
    Create a query that fetches physical Benchmark rows representing runs.

    When request.cursor is a non-empty string, uses keyset pagination (tuple comparison on
    started_at, id) and returns next_cursor. total_count is None in this path.

    When request.cursor is None or empty, uses legacy offset/limit pagination and returns
    total_count. next_cursor is None in this path.

    Args:
        request: FetchBenchmarksRequest

    Returns:
        tuple[Sequence[Benchmark], int | None, str | None]
        Sequence of run rows, optional total count, optional next cursor

    """

    query = scoped_select(Benchmark, org).options(selectinload(Benchmark.final_evaluation))

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
        normalized_emails = [s.strip().lower() for s in request.started_by if s and s.strip()]
        if normalized_emails:
            query = query.where(col(Benchmark.started_by_email).in_(normalized_emails))

    if request.order_by == Order.DESC:
        query = query.order_by(desc(Benchmark.started_at), desc(Benchmark.id))
    else:
        query = query.order_by(asc(Benchmark.started_at), asc(Benchmark.id))

    # Keyset cursor path — skip offset/limit and total_count computation.
    # cursor="" means first page of keyset mode; non-empty cursor means subsequent page.
    if request.cursor is not None:
        if request.cursor:
            cursor_started_at, cursor_id = decode_cursor(request.cursor)

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

        # Fetch one extra row to detect whether there is a next page
        query = query.limit(request.limit + 1)
        run_rows: Sequence[Benchmark] = session.exec(query).all()

        next_cursor: str | None = None
        if len(run_rows) > request.limit:
            run_rows = run_rows[: request.limit]
            last_row = run_rows[-1]
            next_cursor = encode_cursor(last_row.started_at, last_row.id)

        return run_rows, None, next_cursor

    # Legacy offset/limit path — compute total_count for backward compat
    total_count = session.exec(select(func.count()).select_from(query.subquery())).one()

    if not total_count:
        return [], 0, None

    query = query.limit(request.limit).offset(request.offset)
    run_rows = session.exec(query).all()

    return run_rows, total_count, None


def build_run_table_rows(run_rows: Sequence[Benchmark], session: Session) -> list[BenchmarkTableRow]:
    """Build legacy table-row DTOs for a page of run rows.

    Caller must have eager-loaded `final_evaluation`. Avoids the N+1 of
    Benchmark.create_benchmark_table_row in a loop.
    """
    if not run_rows:
        return []

    run_ids = [run_row.id for run_row in run_rows]

    count_rows = session.exec(
        select(Task.benchmark, Task.status, func.count())
        .where(col(Task.benchmark).in_(run_ids))
        .group_by(col(Task.benchmark), col(Task.status))
    ).all()
    counts_by_run: dict[UUID, dict[TaskStatus, int]] = {}
    for run_id, status, count in count_rows:
        counts_by_run.setdefault(run_id, {})[TaskStatus(status)] = count

    rows: list[BenchmarkTableRow] = []
    for run_row in run_rows:
        counts = counts_by_run.get(run_row.id, {})
        rows.append(
            BenchmarkTableRow(
                id=run_row.id,
                name=run_row.name,
                agent_name=run_row.arguments.contract.name,
                label=run_row.label,
                model=run_row.arguments.contract.model,
                dataset=run_row.arguments.dataset or "default",
                executor_release_id=run_row.executor_release_id,
                current_execution_release_id=run_row.current_execution_release_id,
                executor_artifact_digest=run_row.executor_artifact_digest,
                executor_protocol_version=run_row.executor_protocol_version,
                error_message=run_row.error_message if run_row.status == BenchmarkStatus.ERROR else None,
                started_by_email=run_row.started_by_email,
                started_at=run_row.started_at,
                finished_at=run_row.finished_at,
                status=run_row.status,
                total_tasks=sum(counts.values()),
                finished_tasks=(
                    counts.get(TaskStatus.FINISHED, 0)
                    + counts.get(TaskStatus.ERROR, 0)
                    + counts.get(TaskStatus.STOPPED, 0)
                ),
                task_state_counts={k.value: v for k, v in counts.items()},
                final_score=run_row.final_evaluation.final_score if run_row.final_evaluation else None,
            )
        )
    return rows


class YieldingWriter(io.RawIOBase):
    """
    Custom writer that collects bytes and returns them to stream.
    """

    def __init__(self):
        super().__init__()
        self._buffer: bytearray = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, b: Buffer) -> int:
        data = bytes(b)
        self._buffer.extend(data)

        return len(data)

    def pop(self) -> bytes:
        if not self._buffer:
            return b""

        chunk = bytes(self._buffer)
        self._buffer.clear()

        return chunk


def create_final_view(run_row: Benchmark, session: Session, org: Org) -> FinalViewResponse:
    """Create the legacy-compatible stored final view for a run."""
    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == run_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    final_view: FinalViewResponse = FinalViewResponse(
        benchmark_name=run_row.name,
        status=run_row.status,
        error_message=run_row.error_message,
        benchmark_id=run_row.id,
        benchmark_arguments=run_row.arguments,
        started_at=run_row.started_at,
        finished_at=run_row.finished_at,
        tasks_stopped=tasks_stopped or None,  # NOTE: Only include if the run has stopped tasks.
        final_evaluation=run_row.final_evaluation,
        evaluation_results=run_row.fetch_evaluation_results(session),
        task_errors=run_row.fetch_tasks_with_errors(session),
        average_task_breakdown=fetch_average_task_breakdown(run_row.id, session, org.id),
    )

    return final_view


async def upload_final_view(run_row: Benchmark, final_view: FinalViewResponse, harness_config: HarnessConfig) -> str:
    """Upload the final view to the existing physical S3 prefix and return its key."""
    s3_key = f"{S3_BENCHMARKS_PREFIX}/{run_row.id}/{run_row.name}.json"
    await upload_to_s3(
        final_view.model_dump_json(indent=4, exclude_none=True).encode(),
        s3_key,
        harness_config.aws,
        harness_config.s3_bucket,
    )

    return s3_key
