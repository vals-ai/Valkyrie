"""Run-wide task attempt history."""

from __future__ import annotations

from typing import Annotated, Literal, assert_never
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import defer
from sqlmodel import Session, col, desc, func, select

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import task_log_attempt_id
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskAttempt as TaskAttemptRow,
    TaskStatus,
)
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.types import ErrorTaskAttempt, ExecutionTaskAttempt, TaskAttemptBase, summarize_attempt_error

router = APIRouter(prefix="/benchmarks")


class RunErrorTaskAttempt(ErrorTaskAttempt):
    task_id: str
    attempt_number: int
    status: Literal[TaskStatus.ERROR] = TaskStatus.ERROR


class RunEvaluationTaskAttempt(TaskAttemptBase):
    kind: Literal["evaluation"] = "evaluation"
    instance_id: str | None
    agent_caused_exit_reason: AgentCausedExitReason | None
    task_id: str
    attempt_number: int
    status: Literal[TaskStatus.FINISHED] = TaskStatus.FINISHED


class RunExecutionTaskAttempt(ExecutionTaskAttempt):
    task_id: str
    attempt_number: int


RunTaskAttempt = Annotated[
    RunErrorTaskAttempt | RunEvaluationTaskAttempt | RunExecutionTaskAttempt,
    Field(discriminator="kind"),
]


class RunAttemptsResponse(BaseModel):
    attempts: list[RunTaskAttempt]
    total_count: int


@router.get("/{benchmark_id}/attempts", response_model=RunAttemptsResponse)
def get_run_attempts(
    benchmark_id: UUID,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RunAttemptsResponse:
    """Return every persisted task outcome in a run, newest first."""
    get_scoped(Benchmark, benchmark_id, session, org)
    page_end = offset + limit
    task_filters = (
        col(Task.benchmark) == benchmark_id,
        col(Task.org_id) == org.id,
    )
    evaluation_filters = (*task_filters, col(EvaluationResult.org_id) == org.id)
    error_filters = (*task_filters, col(ErrorResult.org_id) == org.id)
    evaluation_exists = (
        select(EvaluationResult.id)
        .where(col(EvaluationResult.task) == col(TaskAttemptRow.task))
        .where(col(EvaluationResult.attempt_id) == col(TaskAttemptRow.attempt_id))
        .where(col(EvaluationResult.org_id) == org.id)
        .exists()
    )
    error_exists = (
        select(ErrorResult.id)
        .where(col(ErrorResult.task) == col(TaskAttemptRow.task))
        .where(col(ErrorResult.attempt_id) == col(TaskAttemptRow.attempt_id))
        .where(col(ErrorResult.org_id) == org.id)
        .exists()
    )
    execution_filters = (
        *task_filters,
        col(TaskAttemptRow.org_id) == org.id,
        ~evaluation_exists,
        ~error_exists,
    )
    evaluation_rows = session.exec(
        select(EvaluationResult, col(Task.task_id))
        .join(Task, col(Task.id) == col(EvaluationResult.task))
        .where(*evaluation_filters)
        .order_by(desc(EvaluationResult.created_at), desc(EvaluationResult.id))
        .options(defer(EvaluationResult.result))  # pyright: ignore[reportArgumentType]
        .limit(page_end)
    ).all()
    error_rows = session.exec(
        select(ErrorResult, col(Task.task_id))
        .join(Task, col(Task.id) == col(ErrorResult.task))
        .where(*error_filters)
        .order_by(desc(ErrorResult.created_at), desc(ErrorResult.id))
        .limit(page_end)
    ).all()
    execution_rows = session.exec(
        select(
            TaskAttemptRow,
            col(Task.task_id),
            col(Task.started_at),
            col(Task.status),
        )
        .join(Task, col(Task.id) == col(TaskAttemptRow.task))
        .where(*execution_filters)
        .order_by(desc(TaskAttemptRow.started_at), desc(TaskAttemptRow.id))
        .limit(page_end)
    ).all()

    attempts_with_tasks: list[tuple[str, ErrorResult | EvaluationResult | TaskAttemptRow, TaskStatus | None]] = [
        (task_id, row, None) for row, task_id in evaluation_rows
    ]
    attempts_with_tasks.extend((task_id, row, None) for row, task_id in error_rows)
    attempts_with_tasks.extend(
        (
            task_id,
            row,
            task_status if row.attempt_id == task_log_attempt_id(task_started_at) else TaskStatus.STOPPED,
        )
        for row, task_id, task_started_at, task_status in execution_rows
    )
    attempts_with_tasks.sort(
        key=lambda item: (
            (item[1].started_at if isinstance(item[1], TaskAttemptRow) else item[1].created_at),
            item[1].id.int,
            isinstance(item[1], EvaluationResult),
        ),
        reverse=True,
    )

    attempt_counts: dict[str, int] = {}
    for task_id, count in session.exec(
        select(col(Task.task_id), func.count(col(EvaluationResult.id)))
        .join(EvaluationResult, col(EvaluationResult.task) == col(Task.id))
        .where(*evaluation_filters)
        .group_by(col(Task.task_id))
    ).all():
        attempt_counts[task_id] = count
    for task_id, count in session.exec(
        select(col(Task.task_id), func.count(col(ErrorResult.id)))
        .join(ErrorResult, col(ErrorResult.task) == col(Task.id))
        .where(*error_filters)
        .group_by(col(Task.task_id))
    ).all():
        attempt_counts[task_id] = attempt_counts.get(task_id, 0) + count
    for task_id, count in session.exec(
        select(col(Task.task_id), func.count(col(TaskAttemptRow.id)))
        .join(TaskAttemptRow, col(TaskAttemptRow.task) == col(Task.id))
        .where(*execution_filters)
        .group_by(col(Task.task_id))
    ).all():
        attempt_counts[task_id] = attempt_counts.get(task_id, 0) + count

    attempts: list[RunTaskAttempt] = []
    seen_by_task: dict[str, int] = {}
    for index, (task_id, row, execution_status) in enumerate(attempts_with_tasks):
        attempt_number = attempt_counts[task_id] - seen_by_task.get(task_id, 0)
        seen_by_task[task_id] = seen_by_task.get(task_id, 0) + 1
        if index < offset or index >= page_end:
            continue

        match row:
            case ErrorResult():
                message, truncated, fingerprint = summarize_attempt_error(row.error_message)
                attempts.append(
                    RunErrorTaskAttempt(
                        id=row.id,
                        attempt_id=row.attempt_id,
                        created_at=row.created_at,
                        task_id=task_id,
                        attempt_number=attempt_number,
                        error_message=message,
                        error_message_truncated=truncated,
                        error_fingerprint=fingerprint,
                    )
                )
            case EvaluationResult():
                attempts.append(
                    RunEvaluationTaskAttempt(
                        id=row.id,
                        attempt_id=row.attempt_id,
                        created_at=row.created_at,
                        task_id=task_id,
                        attempt_number=attempt_number,
                        instance_id=row.instance_id,
                        agent_caused_exit_reason=row.agent_caused_exit_reason,
                    )
                )
            case TaskAttemptRow():
                assert execution_status is not None
                attempts.append(
                    RunExecutionTaskAttempt(
                        id=row.id,
                        attempt_id=row.attempt_id,
                        created_at=row.started_at,
                        task_id=task_id,
                        attempt_number=attempt_number,
                        status=execution_status,
                        instance_id=row.sandbox_instance_id,
                    )
                )
            case _:
                assert_never(row)

    return RunAttemptsResponse(attempts=attempts, total_count=sum(attempt_counts.values()))
