"""Snapshot-fenced run finalization without holding transactions during scoring."""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

from pydantic import JsonValue
from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    ExecutorRunReceipt,
    FinalEvaluation,
    Org,
    TaskStatus,
)
from tracker.executor.dispatch_api import DispatchConflict, as_utc, lock_claimed_dispatch
from tracker.executor.dispatch_control import terminalize_active_dispatches
from tracker.executor.score_state import fetch_final_score_state

FinalizationOperation = Literal["complete", "fail", "stop"]
_RUNNABLE = (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


@dataclass(frozen=True)
class FinalizationState:
    benchmark_id: UUID
    current: bool
    status: BenchmarkStatus
    snapshot_digest: str | None = None
    operation: FinalizationOperation | None = None
    evaluation_results: dict[str, dict[str, Any] | None] = field(default_factory=dict)
    task_errors: dict[str, str] = field(default_factory=dict)


def _read_finalization(session: Session, benchmark: Benchmark, *, current: bool) -> FinalizationState:
    unavailable = FinalizationState(benchmark_id=benchmark.id, current=current, status=benchmark.status)
    if not current or benchmark.status not in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING):
        return unavailable
    org = session.exec(select(Org).where(Org.id == benchmark.org_id)).one()
    results, fingerprint = fetch_final_score_state(session, benchmark, org, for_update=True)
    if any(status in _RUNNABLE for _, _, status, _ in fingerprint):
        return unavailable

    # A newly admitted sibling may not have created its task rows yet.
    assignments = session.exec(
        select(ExecutorDispatch)
        .where(ExecutorDispatch.benchmark_id == benchmark.id)
        .where(col(ExecutorDispatch.status).in_((ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)))
        .order_by(col(ExecutorDispatch.id))
    ).all()
    if any(row.assigned_task_ids is None or not set(row.assigned_task_ids).issubset(results) for row in assignments):
        return unavailable
    task_errors = benchmark.fetch_tasks_with_errors(session) or {}
    stopped = any(status == TaskStatus.STOPPED for _, _, status, _ in fingerprint)
    operation: FinalizationOperation = (
        "complete" if any(result is not None for result in results.values()) else "stop" if stopped else "fail"
    )
    payload = {
        "benchmark_id": str(benchmark.id),
        "started_at": as_utc(benchmark.started_at).isoformat(),
        "status": benchmark.status.value,
        "tasks": [
            [str(task_id), as_utc(started_at).isoformat(), status.value, str(result_id) if result_id else None]
            for task_id, started_at, status, result_id in fingerprint
        ],
        "results": results,
        "errors": task_errors,
        "assignments": [[str(row.id), sorted(row.assigned_task_ids or [])] for row in assignments],
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()

    return FinalizationState(
        benchmark_id=benchmark.id,
        current=current,
        status=benchmark.status,
        snapshot_digest=digest,
        operation=operation,
        evaluation_results=results,
        task_errors=task_errors,
    )


def read_finalization(session: Session, dispatch_id: UUID, claimant_id: UUID) -> FinalizationState:
    benchmark, _dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)

    return _read_finalization(session, benchmark, current=current)


def finalize_run(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    command_id: UUID,
    request_digest: str,
    snapshot_digest: str,
    operation: FinalizationOperation,
    *,
    final_score: float | None = None,
    metadata: dict[str, JsonValue] | None = None,
    error_message: str | None = None,
) -> ExecutorRunReceipt:
    benchmark, _dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    previous = session.get(ExecutorRunReceipt, (dispatch_id, command_id))
    if previous is not None:
        if previous.request_digest != request_digest:
            raise DispatchConflict("Executor finalization command was already used for a different request")
        return previous
    state = _read_finalization(session, benchmark, current=current)
    if state.snapshot_digest is None or state.snapshot_digest != snapshot_digest:
        raise DispatchConflict("Run finalization snapshot is no longer current")
    if operation != state.operation:
        raise DispatchConflict("Run results do not permit this finalization operation")

    counts = benchmark.fetch_task_state_counts(session)
    stopped = counts.get(TaskStatus.STOPPED, 0) > 0
    evaluation_id: UUID | None = None
    if operation == "complete":
        if final_score is None:
            raise DispatchConflict("Run completion requires a final score")
        evaluation = FinalEvaluation(
            org_id=benchmark.org_id,
            benchmark=benchmark.id,
            final_score=final_score,
            properties=metadata or {},
        )
        if benchmark.final_evaluation is not None:
            session.delete(benchmark.final_evaluation)
            session.flush()
        benchmark.final_evaluation = evaluation
        session.add(evaluation)
        evaluation_id = evaluation.id
    elif operation == "fail" and not error_message:
        raise DispatchConflict("Run failure requires an error summary")

    if stopped:
        benchmark.status = BenchmarkStatus.STOPPED
    elif operation == "fail":
        benchmark.status = BenchmarkStatus.ERROR
    else:
        benchmark.status = BenchmarkStatus.FINISHED
    benchmark.error_message = error_message if operation == "fail" else None
    if operation == "fail" and benchmark.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark.docent_reading_status = DocentReadingStatus.ERROR
    terminalize_active_dispatches(
        session,
        benchmark.id,
        except_dispatch_id=dispatch_id if benchmark.status != BenchmarkStatus.STOPPED else None,
    )
    session.add(benchmark)
    receipt = ExecutorRunReceipt(
        dispatch_id=dispatch_id,
        command_id=command_id,
        request_digest=request_digest,
        status=benchmark.status.value,
        final_evaluation_id=evaluation_id,
    )
    session.add(receipt)
    session.flush()

    return receipt
