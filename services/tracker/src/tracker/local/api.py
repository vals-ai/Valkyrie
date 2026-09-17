"""Internal claimant-authenticated local execution secret handoff."""

import os
from datetime import UTC, datetime
from secrets import compare_digest
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatch, ExecutorDispatchStatus
from tracker.database.session import get_session
from tracker.exceptions import SecretsError
from tracker.local.handoff import pending_execution_secrets

router = APIRouter(prefix="/internal/local-execution-secrets", include_in_schema=False)


def _require_claim(dispatch_id: UUID, request: Request, session: Session) -> str:
    if os.environ.get("VALKYRIE_RUNTIME") != "local":
        raise HTTPException(status_code=404, detail="Not found")
    claim_token = request.headers.get("x-executor-claim", "")
    dispatch = session.get(ExecutorDispatch, dispatch_id)
    now = datetime.now(UTC).replace(tzinfo=None)
    if (
        dispatch is None
        or not dispatch.claim_token
        or not claim_token
        or not compare_digest(dispatch.claim_token, claim_token)
        or dispatch.status != ExecutorDispatchStatus.RUNNING
        or dispatch.lease_expires_at is None
        or dispatch.lease_expires_at.replace(tzinfo=None) <= now
    ):
        raise HTTPException(status_code=403, detail="Current executor claim required")
    benchmark = session.get(Benchmark, dispatch.benchmark_id)
    if benchmark is None or benchmark.status != BenchmarkStatus.IN_PROGRESS:
        pending_execution_secrets.discard(dispatch_id)
        raise HTTPException(status_code=409, detail="Execution is no longer active")
    return claim_token


@router.post("/{dispatch_id}/receive")
def receive_execution_secrets(
    dispatch_id: UUID, request: Request, response: Response, session: Session = Depends(get_session)
) -> dict[str, str]:
    claim_token = _require_claim(dispatch_id, request, session)
    response.headers["Cache-Control"] = "no-store"
    try:
        return pending_execution_secrets.receive(dispatch_id, claim_token)
    except SecretsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/{dispatch_id}/acknowledge", status_code=204)
def acknowledge_execution_secrets(
    dispatch_id: UUID, request: Request, session: Session = Depends(get_session)
) -> Response:
    claim_token = _require_claim(dispatch_id, request, session)
    try:
        pending_execution_secrets.acknowledge(dispatch_id, claim_token)
    except SecretsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return Response(status_code=204, headers={"Cache-Control": "no-store"})
