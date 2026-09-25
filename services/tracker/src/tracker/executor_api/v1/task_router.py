"""Version-one task command adapters for transactional executor writes."""

import hashlib
import json
from collections.abc import Callable
from functools import partial
from typing import assert_never
from uuid import UUID

from fastapi import APIRouter
from sqlmodel import Session

from tracker.database.models import AgentCausedExitReason, Task, TaskStatus
from tracker.executor.dispatch_api import as_utc
from tracker.executor.task_api import (
    begin_evaluation,
    claim_task,
    complete_task,
    record_task_error,
    save_checkpoint,
    set_task_status,
    task_authority,
    write_task,
)
from tracker.executor_api.v1.dependencies import DispatchSession
from tracker.executor_api.v1.schemas import AuthorityResponse
from tracker.executor_api.v1.task_schemas import (
    BuildTask,
    RunTask,
    EvaluateTask,
    SaveCheckpoint,
    CompleteTask,
    FailTask,
    RetryTask,
    PendingTask,
    StopTask,
    Mutation,
    TaskAuthorityRequest,
    TaskAttemptRequest,
    TaskWriteRequest,
    TaskWriteResponse,
)

router = APIRouter()


@router.post("/{dispatch_id}/tasks/{task_id}/authority", response_model_exclude_none=True)
def authority(
    dispatch_id: UUID, task_id: UUID, request: TaskAuthorityRequest, session: DispatchSession
) -> AuthorityResponse:
    return AuthorityResponse(
        current=task_authority(session, dispatch_id, request.claimant_id, task_id, request.expected_started_at)
    )


def task_command_digest(task_id: UUID, operation: str, request: TaskAttemptRequest) -> str:
    normalized = request.model_copy(update={"expected_started_at": as_utc(request.expected_started_at)})
    payload = {
        "api": "v1",
        "task_id": str(task_id),
        "operation": operation,
        "request": normalized.model_dump(mode="json"),
    }

    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _mutation(mutation: Mutation) -> Callable[[Session, Task], None]:
    if isinstance(mutation, BuildTask):
        return partial(set_task_status, status=TaskStatus.BUILDING, expected=(TaskStatus.PENDING,))
    if isinstance(mutation, RunTask):
        return partial(set_task_status, status=TaskStatus.IN_PROGRESS, expected=(TaskStatus.BUILDING,))
    if isinstance(mutation, EvaluateTask):
        return partial(
            begin_evaluation,
            sandbox_build_duration=mutation.sandbox_build_duration,
            agent_run_duration=mutation.agent_run_duration,
        )
    if isinstance(mutation, SaveCheckpoint):
        return partial(save_checkpoint, checkpoint=mutation.checkpoint)
    if isinstance(mutation, CompleteTask):
        return partial(
            complete_task,
            result=mutation.result,
            instance_id=mutation.instance_id,
            exit_reason=AgentCausedExitReason(mutation.exit_reason) if mutation.exit_reason is not None else None,
            evaluation_run_duration=mutation.evaluation_run_duration,
            sandbox_run_duration=mutation.sandbox_run_duration,
        )
    if isinstance(mutation, (FailTask, RetryTask)):
        return partial(
            record_task_error,
            error_message=mutation.error_message,
            producer=mutation.producer,
            operation=mutation.operation_name,
            error_type=mutation.error_type,
            cause_code=mutation.cause_code,
            retry_scheduled=isinstance(mutation, RetryTask),
            failed_attempt_number=mutation.failed_attempt_number if isinstance(mutation, RetryTask) else None,
        )
    if isinstance(mutation, PendingTask):
        return partial(
            set_task_status,
            status=TaskStatus.PENDING,
            expected=(TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING),
        )
    match mutation:
        case StopTask():
            return partial(
                set_task_status,
                status=TaskStatus.STOPPED,
                expected=(TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING),
            )
        case _:
            assert_never(mutation)


@router.post("/{dispatch_id}/tasks/{task_id}/claim")
def task_claim(
    dispatch_id: UUID, task_id: UUID, request: TaskAttemptRequest, session: DispatchSession
) -> TaskWriteResponse:
    receipt = claim_task(
        session,
        dispatch_id,
        request.claimant_id,
        task_id,
        request.expected_started_at,
        request.command_id,
        task_command_digest(task_id, "claim", request),
    )
    response = TaskWriteResponse(command_id=receipt.command_id, task_id=receipt.task_id, revision=receipt.revision)
    session.commit()

    return response


@router.post("/{dispatch_id}/tasks/{task_id}/write")
def task_write(
    dispatch_id: UUID, task_id: UUID, request: TaskWriteRequest, session: DispatchSession
) -> TaskWriteResponse:
    receipt = write_task(
        session,
        dispatch_id,
        request.claimant_id,
        task_id,
        request.expected_started_at,
        request.command_id,
        task_command_digest(task_id, "write", request),
        request.expected_revision,
        _mutation(request.mutation),
        allow_stopping=isinstance(request.mutation, (SaveCheckpoint, CompleteTask, FailTask, StopTask)),
    )
    response = TaskWriteResponse(command_id=receipt.command_id, task_id=receipt.task_id, revision=receipt.revision)
    session.commit()

    return response
