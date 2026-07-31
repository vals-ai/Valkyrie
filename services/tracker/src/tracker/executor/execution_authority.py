"""Database-time authority fence for executor-originated writes."""

from dataclasses import dataclass
from uuid import UUID

from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatch, ExecutorDispatchStatus
from tracker.exceptions import ExecutionAuthorityRevoked


@dataclass(frozen=True)
class ExecutionAuthority:
    benchmark_id: UUID
    dispatch_id: UUID


def lock_execution_authority(
    session: Session,
    authority: ExecutionAuthority,
    *,
    require_in_progress: bool = True,
) -> Benchmark:
    """Lock the benchmark and prove that the exact dispatch remains authoritative."""
    benchmark = session.exec(
        select(Benchmark).where(col(Benchmark.id) == authority.benchmark_id).with_for_update()
    ).one_or_none()
    if (
        benchmark is None
        or benchmark.status == BenchmarkStatus.STOPPED
        or (require_in_progress and benchmark.status not in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING))
    ):
        raise ExecutionAuthorityRevoked("Executor benchmark authority was revoked")

    dispatch_id = session.exec(
        select(ExecutorDispatch.id)
        .where(col(ExecutorDispatch.id) == authority.dispatch_id)
        .where(col(ExecutorDispatch.benchmark_id) == authority.benchmark_id)
        .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
        .with_for_update()
    ).one_or_none()
    if dispatch_id is None:
        raise ExecutionAuthorityRevoked("Executor dispatch authority was revoked")
    return benchmark
