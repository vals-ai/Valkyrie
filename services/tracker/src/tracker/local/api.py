"""Internal installation-authenticated local execution secret handoff."""

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
from tracker.local.secret_pipe import local_handoff_token

router = APIRouter(prefix="/internal/local-execution-secrets", include_in_schema=False)


def _authenticate(request: Request) -> None:
    if os.environ.get("VALKYRIE_RUNTIME") != "local":
        raise HTTPException(status_code=404, detail="Not found")
    token = request.headers.get("x-local-handoff-token", "")
    if not compare_digest(local_handoff_token().encode(), token.encode()):
        raise HTTPException(status_code=403, detail="Local installation token required")


def _require_claim(dispatch_id: UUID, session: Session) -> None:
    dispatch = session.get(ExecutorDispatch, dispatch_id)
    now = datetime.now(UTC).replace(tzinfo=None)
    if (
        dispatch is None
        or dispatch.status != ExecutorDispatchStatus.RUNNING
        or dispatch.lease_expires_at is None
        or dispatch.lease_expires_at.replace(tzinfo=None) <= now
    ):
        raise HTTPException(status_code=403, detail="Current executor claim required")
    benchmark = session.get(Benchmark, dispatch.benchmark_id)
    if benchmark is None or benchmark.status != BenchmarkStatus.IN_PROGRESS:
        pending_execution_secrets.discard(dispatch_id)
        raise HTTPException(status_code=409, detail="Execution is no longer active")


@router.post("/{dispatch_id}/receive")
def receive_execution_secrets(
    dispatch_id: UUID, request: Request, response: Response, session: Session = Depends(get_session)
) -> dict[str, str]:
    _authenticate(request)
    _require_claim(dispatch_id, session)
    response.headers["Cache-Control"] = "no-store"
    try:
        return pending_execution_secrets.receive(dispatch_id)
    except SecretsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/{dispatch_id}/acknowledge", status_code=204)
def acknowledge_execution_secrets(dispatch_id: UUID, request: Request) -> Response:
    _authenticate(request)
    pending_execution_secrets.discard(dispatch_id)
    return Response(status_code=204, headers={"Cache-Control": "no-store"})
