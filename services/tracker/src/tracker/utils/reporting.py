"""Read-only queries that fetch and shape run data for API responses and listings."""

import asyncio
import base64
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
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.types import (
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FinalViewResponse,
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


class BenchmarkContext:
    _benchmark_row: Benchmark
    _session: Session
    _org: Org

    def __init__(self, benchmark_row: Benchmark, session: Session, org: Org):
        self._benchmark_row = benchmark_row
        self._session = session
        self._org = org

    @property
    def _status(self) -> BenchmarkStatus:
        return self._benchmark_row.status

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
            .where(Task.benchmark == self._benchmark_row.id)
            .where(Task.org_id == self._org.id)
        )

        result = self._session.exec(statement).one()

        task_counts = TaskCounts(total_tasks=result[0], finished_tasks=result[1], failed_tasks=result[2])

        return task_counts

    @property
    def _task_breakdown(self) -> dict[TaskStatus, int]:
        """
        Returns a mapping between the task status and the number of tasks in that status

        Provides a breakdown of the benchmark.
        """
        statement = (
            select(Task.status, func.count(col(Task.id)))
            .select_from(Task)
            .where(Task.benchmark == self._benchmark_row.id)
            .where(Task.org_id == self._org.id)
            .group_by(Task.status)
            .having(func.count(col(Task.id)) > 0)  # Exclude all with count of 0
        )

        result = self._session.exec(statement).all()

        if not result:
            raise TrackerServiceError(
                f"No tasks have been discovered for run {self._benchmark_row.id}, cannot provide task breakdown"
            )

        return {TaskStatus(status): count for status, count in result}

    @cached_property
    def benchmark_details(self) -> BenchmarkDetails:
        return BenchmarkDetails(
            status=self._status,
            started_at=self._benchmark_row.started_at,
            total_tasks=self._task_counts.total_tasks,
            finished_tasks=self._task_counts.finished_tasks,
            task_breakdown=self._task_breakdown,
            docent_reading_status=self._benchmark_row.docent_reading_status,
            docent_reading_url=self._benchmark_row.docent_reading_url,
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


def fetch_evaluation_results(benchmark_id: UUID, session: Session, org_id: UUID) -> dict[str, dict[str, Any]]:
    """Select the latest successful evaluation result for each finished task."""
    statement = (
        select(EvaluationResult, Task.id, Task.task_id, TaskBreakdown)
        .join(Task, col(EvaluationResult.task) == col(Task.id))
        .outerjoin(TaskBreakdown, col(Task.task_breakdown) == col(TaskBreakdown.id))
        .where(Task.benchmark == benchmark_id)
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


def fetch_average_task_breakdown(benchmark_id: UUID, session: Session, org_id: UUID) -> AverageTaskBreakdown | None:
    """
    Fetch the average task breakdown for a given benchmark.

    Returns None if there are no task metrics available for the benchmark.
    """
    row = session.exec(
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

    if all(v is None for v in row):
        return None

    return AverageTaskBreakdown(
        sandbox_build_duration=row[0],
        agent_run_duration=row[1],
        evaluation_run_duration=row[2],
        sandbox_run_duration=row[3],
    )


async def stream_benchmark_results(
    benchmark_id: UUID, session: Session, harness_config: HarnessConfig, org: Org
) -> AsyncGenerator[str]:
    """
    Generate Server-Sent Events with benchmark updates. User connects to this when they want to view live updates of a benchmark.

    Usage from client:
        curl -X GET http://<endpoint>/stream-benchmark-results/<benchmark_id>?connect=true

    Returns:
        AsyncGenerator[str]
    """
    PULL_INTERVAL = 5

    EVENT_COMPLETE = "event: complete\n\n"
    EVENT_ERROR = "event: error\ndata:"
    DATA_PREFIX = "data:"
    DISCONNECT = "event: disconnect\n\n"

    try:
        while True:
            with Session(bind=session.bind) as fresh_session:
                fresh_benchmark = fresh_session.get(Benchmark, benchmark_id)
                if not fresh_benchmark or fresh_benchmark.org_id != org.id:
                    yield f"{EVENT_ERROR} {json.dumps({'error': 'Run not found'})}\n\n"
                    break

                fresh_session.refresh(fresh_benchmark)
                benchmark_context = BenchmarkContext(fresh_benchmark, fresh_session, org)

                response_data = FetchBenchmarkResponse(
                    benchmark_name=fresh_benchmark.name,
                    benchmark_id=fresh_benchmark.id,
                    details=benchmark_context.benchmark_details,
                    s3_bucket_url=create_benchmark_url(
                        str(fresh_benchmark.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
                    ),
                    label=fresh_benchmark.label,
                    final_score=fresh_benchmark.final_evaluation.final_score
                    if fresh_benchmark.final_evaluation
                    else None,
                )

                yield f"{DATA_PREFIX} {response_data.model_dump_json()}\n\n"

                if fresh_benchmark.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(PULL_INTERVAL)

    except asyncio.CancelledError:
        logger.info(f"Client disconnected from benchmark {benchmark_id} stream")
        yield DISCONNECT


def encode_cursor(started_at: datetime, row_id: UUID) -> str:
    """Encode a keyset pagination cursor from a started_at timestamp and row id."""
    payload = json.dumps({"started_at": started_at.isoformat(), "id": str(row_id)})
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Decode a keyset pagination cursor into a started_at timestamp and row id."""
    padded = cursor + "=" * (-len(cursor) % 4)
    payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    return datetime.fromisoformat(payload["started_at"]), UUID(payload["id"])


def fetch_filtered_benchmark_rows(
    request: FetchBenchmarksRequest, session: Session, org: Org
) -> tuple[Sequence[Benchmark], int | None, str | None]:
    """
    Creates a query to fetch benchmark rows from the database based on the fetch benchmark request.

    When request.cursor is a non-empty string, uses keyset pagination (tuple comparison on
    started_at, id) and returns next_cursor. total_count is None in this path.

    When request.cursor is None or empty, uses legacy offset/limit pagination and returns
    total_count. next_cursor is None in this path.

    Args:
        request: FetchBenchmarksRequest

    Returns:
        tuple[Sequence[Benchmark], int | None, str | None]
        Sequence of benchmark rows, optional total count, optional next cursor

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
        benchmark_rows: Sequence[Benchmark] = session.exec(query).all()

        next_cursor: str | None = None
        if len(benchmark_rows) > request.limit:
            benchmark_rows = benchmark_rows[: request.limit]
            last_row = benchmark_rows[-1]
            next_cursor = encode_cursor(last_row.started_at, last_row.id)

        return benchmark_rows, None, next_cursor

    # Legacy offset/limit path — compute total_count for backward compat
    total_count = session.exec(select(func.count()).select_from(query.subquery())).one()

    if not total_count:
        return [], 0, None

    query = query.limit(request.limit).offset(request.offset)
    benchmark_rows = session.exec(query).all()

    return benchmark_rows, total_count, None


def build_benchmark_table_rows(benchmarks: Sequence[Benchmark], session: Session) -> list[BenchmarkTableRow]:
    """Batch-load task counts + run-by emails for a page of benchmarks.

    Caller must have eager-loaded `final_evaluation`. Avoids the N+1 of
    Benchmark.create_benchmark_table_row in a loop.
    """
    if not benchmarks:
        return []

    bench_ids = [b.id for b in benchmarks]

    count_rows = session.exec(
        select(Task.benchmark, Task.status, func.count())
        .where(col(Task.benchmark).in_(bench_ids))
        .group_by(col(Task.benchmark), col(Task.status))
    ).all()
    counts_by_bench: dict[UUID, dict[TaskStatus, int]] = {}
    for bench_id, status, count in count_rows:
        counts_by_bench.setdefault(bench_id, {})[TaskStatus(status)] = count

    rows: list[BenchmarkTableRow] = []
    for b in benchmarks:
        counts = counts_by_bench.get(b.id, {})
        rows.append(
            BenchmarkTableRow(
                id=b.id,
                name=b.name,
                agent_name=b.arguments.contract.name,
                label=b.label,
                model=b.arguments.contract.model,
                dataset=b.arguments.dataset or "default",
                started_by_email=b.started_by_email,
                started_at=b.started_at,
                finished_at=b.finished_at,
                status=b.status,
                total_tasks=sum(counts.values()),
                finished_tasks=(
                    counts.get(TaskStatus.FINISHED, 0)
                    + counts.get(TaskStatus.ERROR, 0)
                    + counts.get(TaskStatus.STOPPED, 0)
                ),
                task_state_counts={k.value: v for k, v in counts.items()},
                final_score=b.final_evaluation.final_score if b.final_evaluation else None,
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


def create_final_view(benchmark_row: Benchmark, session: Session, org: Org) -> FinalViewResponse:
    """Creates final view of a benchmark that includes metadata about evaluations and score"""
    tasks_stopped: int = session.exec(
        select(func.count(col(Task.id)))
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.status) == TaskStatus.STOPPED)
    ).one()

    final_view: FinalViewResponse = FinalViewResponse(
        benchmark_name=benchmark_row.name,
        status=benchmark_row.status,
        error_message=benchmark_row.error_message,
        benchmark_id=benchmark_row.id,
        benchmark_arguments=benchmark_row.arguments,
        started_at=benchmark_row.started_at,
        finished_at=benchmark_row.finished_at,
        tasks_stopped=tasks_stopped or None,  # NOTE: Only include if we stopped the benchmark
        final_evaluation=benchmark_row.final_evaluation,
        evaluation_results=benchmark_row.fetch_evaluation_results(session),
        task_errors=benchmark_row.fetch_tasks_with_errors(session),
        average_task_breakdown=fetch_average_task_breakdown(benchmark_row.id, session, org.id),
    )

    return final_view


async def upload_final_view(
    benchmark_row: Benchmark, final_view: FinalViewResponse, harness_config: HarnessConfig
) -> str:
    """Uploads the final view to the root of the benchmark folder and returns the s3 key"""
    s3_key = f"{S3_BENCHMARKS_PREFIX}/{benchmark_row.id}/{benchmark_row.name}.json"
    await upload_to_s3(
        final_view.model_dump_json(indent=4, exclude_none=True).encode(),
        s3_key,
        harness_config.aws,
        harness_config.s3_bucket,
    )

    return s3_key
