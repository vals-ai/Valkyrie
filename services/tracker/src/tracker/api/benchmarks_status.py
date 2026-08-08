"""GET /benchmarks/status — lightweight polling for live status updates."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from tracker.api.parsing import parse_csv
from tracker.auth import get_current_org
from tracker.database.dependencies import BenchmarkRepositoryDep
from tracker.database.models import Org, TaskStatus
from tracker.types import BenchmarkStatusEntry, BenchmarkStatusResponse

router = APIRouter(prefix="/benchmarks")


@router.get("/status", response_model=BenchmarkStatusResponse)
def get_benchmarks_status(
    benchmark_repository: BenchmarkRepositoryDep,
    ids: str = Query(default=""),
    org: Org = Depends(get_current_org),
) -> BenchmarkStatusResponse:
    """Return status + task-count breakdown for the listed benchmark ids."""
    parsed_ids = parse_csv(ids, UUID)
    if not parsed_ids:
        return BenchmarkStatusResponse(entries=[])

    benchmarks = benchmark_repository.get_for_ids(parsed_ids, org.id)
    counts_by_benchmark_id = benchmark_repository.get_task_status_counts(
        [benchmark.id for benchmark in benchmarks],
        org.id,
    )

    entries: list[BenchmarkStatusEntry] = []
    for benchmark in benchmarks:
        counts = counts_by_benchmark_id.get(benchmark.id, {})
        entries.append(
            BenchmarkStatusEntry(
                id=benchmark.id,
                status=benchmark.status,
                finished_at=benchmark.finished_at,
                executor_release_id=benchmark.executor_release_id,
                current_execution_release_id=benchmark.current_execution_release_id,
                executor_artifact_digest=benchmark.executor_artifact_digest,
                executor_protocol_version=benchmark.executor_protocol_version,
                total_tasks=sum(counts.values()),
                finished_tasks=(
                    counts.get(TaskStatus.FINISHED, 0)
                    + counts.get(TaskStatus.ERROR, 0)
                    + counts.get(TaskStatus.STOPPED, 0)
                ),
                task_state_counts={status.value: count for status, count in counts.items()},
            )
        )

    return BenchmarkStatusResponse(entries=entries)
