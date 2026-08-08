"""Canonical transaction owners for executor dispatch lifecycle."""

from uuid import UUID

from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatch, ExecutorDispatchKind
from tracker.database.repositories import EnqueueFailureResolution, ExecutorControlRepository
from tracker.executor.release_control import create_executor_dispatch


def admit_start_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch_id: UUID,
    executor_control_repository: ExecutorControlRepository,
) -> ExecutorDispatch:
    """Select the active release and persist one start dispatch."""
    with session.no_autoflush:
        release = executor_control_repository.select_active_release(for_update=True)
    executor_control_repository.pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=dispatch_id,
    )
    return executor_control_repository.stage_dispatch(dispatch)


def admit_recovery_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    pre_action_status: BenchmarkStatus,
    dispatch_id: UUID,
    kind: ExecutorDispatchKind,
    executor_control_repository: ExecutorControlRepository,
) -> ExecutorDispatch:
    """Persist additive in-progress work or replace a terminal execution."""
    with session.no_autoflush:
        executor_control_repository.lock_executor_admission()
        if pre_action_status == BenchmarkStatus.IN_PROGRESS:
            release = executor_control_repository.resolve_current_execution_release(benchmark, for_update=True)
        else:
            release = executor_control_repository.select_active_release(for_update=True)
    if pre_action_status != BenchmarkStatus.IN_PROGRESS:
        executor_control_repository.stage_benchmark_recovery(benchmark, release)
        executor_control_repository.terminalize_active_dispatches(benchmark.id)

    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        kind,
        dispatch_id=dispatch_id,
    )
    return executor_control_repository.stage_dispatch(dispatch)


__all__ = [
    "EnqueueFailureResolution",
    "admit_recovery_dispatch",
    "admit_start_dispatch",
]
