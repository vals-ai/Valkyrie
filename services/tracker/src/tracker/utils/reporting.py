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
from typing import NamedTuple, Sequence
from uuid import UUID

from sqlmodel import Session

from tracker.aws.s3 import (
    S3_BENCHMARKS_PREFIX,
    create_benchmark_url,
    upload_to_s3,
)
from tracker.database.models import Benchmark, BenchmarkStatus, Org, TaskStatus
from tracker.database.repositories import BenchmarkRepository, BenchmarkTaskCounts, ReportingRepository
from tracker.logging import get_logger
from tracker.types import (
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
)

logger = get_logger(__name__)


class TaskCounts(NamedTuple):
    total_tasks: int
    finished_tasks: int
    failed_tasks: int


class BenchmarkContext:
    _benchmark_row: Benchmark
    _reporting_repository: ReportingRepository
    _org: Org

    def __init__(self, benchmark_row: Benchmark, reporting_repository: ReportingRepository, org: Org):
        self._benchmark_row = benchmark_row
        self._reporting_repository = reporting_repository
        self._org = org

    @property
    def _status(self) -> BenchmarkStatus:
        return self._benchmark_row.status

    @cached_property
    def _task_counts_report(self) -> BenchmarkTaskCounts:
        return self._reporting_repository.get_benchmark_task_counts(self._benchmark_row.id, self._org.id)

    @property
    def _task_counts(self) -> TaskCounts:
        report = self._task_counts_report

        return TaskCounts(
            total_tasks=report.total_tasks,
            finished_tasks=report.finished_tasks,
            failed_tasks=report.failed_tasks,
        )

    @property
    def _task_breakdown(self) -> dict[TaskStatus, int]:
        """Return the benchmark's task counts grouped by status."""
        return self._task_counts_report.status_counts

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
                benchmark_repository = BenchmarkRepository(fresh_session)
                fresh_benchmark = benchmark_repository.get_for_org(benchmark_id, org.id)
                if fresh_benchmark is None:
                    yield f"{EVENT_ERROR} {json.dumps({'error': 'Run not found'})}\n\n"
                    break

                reporting_repository = ReportingRepository(fresh_session)
                benchmark_context = BenchmarkContext(fresh_benchmark, reporting_repository, org)

                response_data = FetchBenchmarkResponse(
                    benchmark_name=fresh_benchmark.name,
                    benchmark_id=fresh_benchmark.id,
                    details=benchmark_context.benchmark_details,
                    s3_bucket_url=create_benchmark_url(
                        str(fresh_benchmark.id), harness_config.aws.aws_default_region, harness_config.s3_bucket
                    ),
                    label=fresh_benchmark.label,
                    executor_release_id=fresh_benchmark.executor_release_id,
                    current_execution_release_id=fresh_benchmark.current_execution_release_id,
                    executor_artifact_digest=fresh_benchmark.executor_artifact_digest,
                    executor_protocol_version=fresh_benchmark.executor_protocol_version,
                    final_score=benchmark_repository.get_final_score(fresh_benchmark.id, org.id),
                    error_message=fresh_benchmark.error_message
                    if fresh_benchmark.status == BenchmarkStatus.ERROR
                    else None,
                )

                yield f"{DATA_PREFIX} {response_data.model_dump_json()}\n\n"

                if fresh_benchmark.status in [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]:
                    yield EVENT_COMPLETE
                    break

            await asyncio.sleep(PULL_INTERVAL)

    except CancelledError:
        logger.info(f"Client disconnected from benchmark {benchmark_id} stream")
        yield DISCONNECT


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


def fetch_filtered_benchmark_rows(
    request: FetchBenchmarksRequest, reporting_repository: ReportingRepository, org: Org
) -> tuple[Sequence[Benchmark], int | None, str | None]:
    """Fetch filtered benchmark rows through the reporting repository."""
    cursor = decode_cursor(request.cursor) if request.cursor else None
    page = reporting_repository.fetch_filtered_benchmark_rows(request, org.id, cursor=cursor)

    next_cursor: str | None = None
    if page.has_next_page:
        last_row = page.rows[-1]
        next_cursor = encode_cursor(last_row.started_at, last_row.id)

    return page.rows, page.total_count, next_cursor


def build_benchmark_table_rows(
    benchmarks: Sequence[Benchmark], benchmark_repository: BenchmarkRepository
) -> list[BenchmarkTableRow]:
    """Batch-load task counts + run-by emails for a page of benchmarks.

    Caller must have eager-loaded `final_evaluation`.
    """
    if not benchmarks:
        return []

    bench_ids = [benchmark.id for benchmark in benchmarks]
    counts_by_bench = benchmark_repository.get_task_status_counts(bench_ids, benchmarks[0].org_id)

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
                executor_release_id=b.executor_release_id,
                current_execution_release_id=b.current_execution_release_id,
                executor_artifact_digest=b.executor_artifact_digest,
                executor_protocol_version=b.executor_protocol_version,
                error_message=b.error_message if b.status == BenchmarkStatus.ERROR else None,
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


def create_final_view(
    benchmark_row: Benchmark, reporting_repository: ReportingRepository, org: Org
) -> FinalViewResponse:
    """Create the final view of a benchmark with evaluation metadata and score."""
    tasks_stopped = reporting_repository.get_stopped_task_count(benchmark_row.id, org.id)

    final_view = FinalViewResponse(
        benchmark_name=benchmark_row.name,
        status=benchmark_row.status,
        error_message=benchmark_row.error_message,
        benchmark_id=benchmark_row.id,
        benchmark_arguments=benchmark_row.arguments,
        started_at=benchmark_row.started_at,
        finished_at=benchmark_row.finished_at,
        tasks_stopped=tasks_stopped or None,
        final_evaluation=benchmark_row.final_evaluation,
        evaluation_results=reporting_repository.fetch_evaluation_results(benchmark_row.id, org.id),
        task_errors=reporting_repository.get_task_errors(benchmark_row.id, org.id),
        average_task_breakdown=reporting_repository.fetch_average_task_breakdown(benchmark_row.id, org.id),
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
