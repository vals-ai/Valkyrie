"""Version-one adapters for final score snapshots and atomic run completion."""

import hashlib
import json
from uuid import UUID

from fastapi import APIRouter

from tracker.executor.finalization_api import finalize_run, read_finalization
from tracker.executor_api.v1.dependencies import DispatchSession
from tracker.executor_api.v1.schemas import DispatchRequest, RunStatus
from tracker.executor_api.v1.finalization_schemas import (
    CompleteRun,
    FailRun,
    FinalizationResponse,
    FinalizeRequest,
    FinalizeResponse,
)

router = APIRouter()


@router.post("/{dispatch_id}/run/finalization")
def run_finalization(dispatch_id: UUID, request: DispatchRequest, session: DispatchSession) -> FinalizationResponse:
    state = read_finalization(session, dispatch_id, request.claimant_id)

    return FinalizationResponse(
        benchmark_id=state.benchmark_id,
        current=state.current,
        status=RunStatus(state.status.value),
        snapshot_digest=state.snapshot_digest,
        operation=state.operation,
        evaluation_results=state.evaluation_results,
        task_errors=state.task_errors,
    )


@router.post("/{dispatch_id}/run/finalize")
def run_finalize(dispatch_id: UUID, request: FinalizeRequest, session: DispatchSession) -> FinalizeResponse:
    payload = {"api": "v1", "dispatch_id": str(dispatch_id), "request": request.model_dump(mode="json")}
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    finalization = request.finalization
    receipt = finalize_run(
        session,
        dispatch_id,
        request.claimant_id,
        request.command_id,
        digest,
        request.snapshot_digest,
        finalization.operation,
        final_score=finalization.final_score if isinstance(finalization, CompleteRun) else None,
        metadata=finalization.metadata if isinstance(finalization, CompleteRun) else None,
        error_message=finalization.error_message if isinstance(finalization, FailRun) else None,
    )
    state = read_finalization(session, dispatch_id, request.claimant_id)
    response = FinalizeResponse.model_validate(
        {
            "command_id": receipt.command_id,
            "benchmark_id": state.benchmark_id,
            "status": receipt.status,
            "final_evaluation_id": receipt.final_evaluation_id,
        }
    )
    session.commit()

    return response
