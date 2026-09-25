"""Current task results and attempt identity shared by run finalization paths."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlmodel import Session, col, desc, select

from tracker.database.models import Benchmark, EvaluationResult, Org, Task, TaskStatus

TaskFingerprint = tuple[tuple[UUID, datetime, TaskStatus, UUID | None], ...]


def fetch_final_score_state(
    session: Session, benchmark_row: Benchmark, org: Org, *, for_update: bool = False
) -> tuple[dict[str, dict[str, Any] | None], TaskFingerprint]:
    task_rows_query = (
        select(Task.id, Task.task_id, Task.started_at, Task.status)
        .where(col(Task.benchmark) == benchmark_row.id)
        .where(col(Task.org_id) == org.id)
        .order_by(col(Task.id))
    )
    if for_update:
        task_rows_query = task_rows_query.with_for_update()
    task_rows = cast(Sequence[tuple[UUID, str, datetime, TaskStatus]], session.exec(task_rows_query).all())
    task_row_ids = [task_row_id for task_row_id, _task_id, _started_at, _status in task_rows]
    result_rows = session.exec(
        select(col(EvaluationResult.task), col(EvaluationResult.id), col(EvaluationResult.result))
        .where(col(EvaluationResult.task).in_(task_row_ids))
        .where(col(EvaluationResult.org_id) == org.id)
        .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
    ).all()
    latest_results: dict[UUID, tuple[UUID, dict[str, Any]]] = {}
    for task_row_id, result_id, result in result_rows:
        latest_results.setdefault(task_row_id, (result_id, result))

    inputs = {
        task_id: latest_results[task_row_id][1]
        if status == TaskStatus.FINISHED and task_row_id in latest_results
        else None
        for task_row_id, task_id, _started_at, status in task_rows
    }
    fingerprint = tuple(
        (
            task_row_id,
            started_at,
            status,
            latest_results[task_row_id][0] if task_row_id in latest_results else None,
        )
        for task_row_id, _task_id, started_at, status in task_rows
    )

    return inputs, fingerprint
