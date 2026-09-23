"""Version-one adapters for persistent sandbox-creation reservations."""

from uuid import UUID

from fastapi import APIRouter

from tracker.executor.queue_api import release_pool, reserve_pool
from tracker.executor_api.v1.dependencies import DispatchSession
from tracker.executor_api.v1.queue_schemas import (
    ReleasePoolRequest,
    ReleasePoolResponse,
    ReservePoolRequest,
    ReservePoolResponse,
)
from tracker.executor_api.v1.task_router import task_command_digest

router = APIRouter()


@router.post("/{dispatch_id}/tasks/{task_id}/queue/reserve")
def queue_reserve(
    dispatch_id: UUID, task_id: UUID, request: ReservePoolRequest, session: DispatchSession
) -> ReservePoolResponse:
    receipt = reserve_pool(
        session,
        dispatch_id,
        request.claimant_id,
        task_id,
        request.expected_started_at,
        request.command_id,
        task_command_digest(task_id, "queue/reserve", request),
        request.expected_revision,
    )
    response = ReservePoolResponse(
        command_id=request.command_id,
        reserved=receipt is not None,
        reservation_id=receipt.command_id if receipt is not None else None,
        revision=receipt.revision if receipt is not None else None,
    )
    session.commit()

    return response


@router.post("/{dispatch_id}/tasks/{task_id}/queue/release")
def queue_release(
    dispatch_id: UUID, task_id: UUID, request: ReleasePoolRequest, session: DispatchSession
) -> ReleasePoolResponse:
    release_pool(
        session,
        dispatch_id,
        request.claimant_id,
        task_id,
        request.expected_started_at,
        request.command_id,
        task_command_digest(task_id, "queue/release", request),
        request.reservation_id,
    )
    session.commit()

    return ReleasePoolResponse(command_id=request.command_id, reservation_id=request.reservation_id)
