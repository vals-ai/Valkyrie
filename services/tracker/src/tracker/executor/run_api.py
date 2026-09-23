"""Claim-scoped run snapshots and retry-safe task initialization."""

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from sqlmodel import Session, select

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatch, Org, Task
from tracker.executor.dispatch_api import DispatchConflict, as_utc, lock_claimed_dispatch
from tracker.executor.task_state import ensure_task_rows, load_task_rows


@dataclass(frozen=True)
class RunState:
    benchmark: Benchmark
    org: Org
    current: bool
    tasks: Sequence[tuple[str, Task]]
    task_counts: dict[str, int]


def _check_task_assignment(dispatch: ExecutorDispatch, task_ids: Sequence[str]) -> None:
    if dispatch.assigned_task_ids is None:
        raise DispatchConflict("Executor dispatch has no persisted task assignment")
    if not set(task_ids).issubset(dispatch.assigned_task_ids):
        raise DispatchConflict("Requested tasks are not assigned to this executor dispatch")


def _read_state(
    session: Session, benchmark: Benchmark, org: Org, task_ids: Sequence[str], *, current: bool
) -> RunState:
    tasks = load_task_rows(session, benchmark, org, task_ids)
    if len(tasks) != len(task_ids):
        raise DispatchConflict("Assigned tasks have not been initialized")
    counts = benchmark.fetch_task_state_counts(session)

    return RunState(
        benchmark=benchmark,
        org=org,
        current=current,
        tasks=tasks,
        task_counts={status.value: count for status, count in counts.items()},
    )


def initialize_run_tasks(session: Session, dispatch_id: UUID, claimant_id: UUID, task_ids: Sequence[str]) -> RunState:
    benchmark, dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    if not current or benchmark.status != BenchmarkStatus.IN_PROGRESS:
        raise DispatchConflict("Executor run cannot initialize tasks after authority was revoked")
    _check_task_assignment(dispatch, task_ids)
    org = session.exec(select(Org).where(Org.id == benchmark.org_id)).one()
    ensure_task_rows(session, benchmark, org, task_ids, started_at=dispatch.created_at)
    state = _read_state(session, benchmark, org, task_ids, current=current)
    if any(as_utc(task.started_at) > as_utc(dispatch.created_at) for _, task in state.tasks):
        raise DispatchConflict("Assigned task attempts were superseded by a newer dispatch")

    return state


def read_run_state(session: Session, dispatch_id: UUID, claimant_id: UUID, task_ids: Sequence[str]) -> RunState:
    benchmark, dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    _check_task_assignment(dispatch, task_ids)
    org = session.exec(select(Org).where(Org.id == benchmark.org_id)).one()

    return _read_state(session, benchmark, org, task_ids, current=current)
