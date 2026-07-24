"""Durable, org-scoped receipts for browser-triggered mutations."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
import json
from typing import Literal, TypeAlias, assert_never
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import HTTPException
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, select

from tracker.database.models import MutationOperation, MutationOperationKind, MutationOperationState, Org
from tracker.types import ERROR_EXCERPT_MAX_LENGTH

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, JSONValue]


@dataclass(frozen=True)
class ExecuteMutation:
    outcome: Literal["execute"] = "execute"


@dataclass(frozen=True)
class ProcessingMutation:
    kind: MutationOperationKind
    outcome: Literal["processing"] = "processing"


@dataclass(frozen=True)
class SucceededMutation:
    kind: MutationOperationKind
    response: JSONObject
    outcome: Literal["succeeded"] = "succeeded"


@dataclass(frozen=True)
class FailedMutation:
    kind: MutationOperationKind
    status_code: int
    detail: str
    outcome: Literal["failed"] = "failed"


@dataclass(frozen=True)
class UncertainMutation:
    kind: MutationOperationKind
    outcome: Literal["uncertain"] = "uncertain"


MutationStatus = ProcessingMutation | SucceededMutation | FailedMutation | UncertainMutation
MutationClaim = ExecuteMutation | MutationStatus
STALE_PROCESSING_AFTER = timedelta(minutes=15)


def mutation_fingerprint(kind: MutationOperationKind, request: JSONObject) -> str:
    """Fingerprint a caller-sanitized request object. Auth and secrets must be excluded by the caller."""
    canonical_request = json.dumps(
        {"kind": kind, "request": request},
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(canonical_request.encode()).hexdigest()


def claim_mutation(
    session: Session,
    org: Org,
    operation_id: UUID,
    kind: MutationOperationKind,
    request_fingerprint: str,
) -> MutationClaim:
    values = {
        "org_id": org.id,
        "operation_id": operation_id,
        "kind": kind,
        "request_fingerprint": request_fingerprint,
        "state": MutationOperationState.PROCESSING,
        "response": None,
        "failure_status_code": None,
        "failure_detail": None,
    }
    match session.get_bind().dialect.name:
        case "postgresql":
            statement = postgresql_insert(MutationOperation)
        case "sqlite":
            statement = sqlite_insert(MutationOperation)
        case dialect:
            raise AssertionError(f"Unsupported mutation receipt database: {dialect}")

    result = session.exec(statement.values(**values).on_conflict_do_nothing(index_elements=["org_id", "operation_id"]))
    session.commit()
    if result.rowcount == 1:
        return ExecuteMutation()

    operation = _find_scoped_operation(session, org, operation_id, for_update=True)
    assert operation is not None
    if operation.kind != kind or operation.request_fingerprint != request_fingerprint:
        session.commit()
        raise HTTPException(status_code=409, detail="Operation ID was already used for a different request")
    return _locked_status(session, operation)


def complete_mutation(
    session: Session,
    org: Org,
    operation_id: UUID,
    response: JSONObject,
) -> SucceededMutation:
    operation = _find_scoped_operation(session, org, operation_id, for_update=True)
    assert operation is not None
    match operation.state:
        case MutationOperationState.PROCESSING | MutationOperationState.UNCERTAIN:
            operation.state = MutationOperationState.SUCCEEDED
            operation.response = response
            operation.updated_at = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
            session.add(operation)
        case MutationOperationState.SUCCEEDED:
            pass
        case MutationOperationState.FAILED:
            raise AssertionError("Failed mutations cannot later succeed")

    status = _status(operation)
    session.commit()
    assert isinstance(status, SucceededMutation)
    return status


def mark_mutation_failed(
    session: Session,
    org: Org,
    operation_id: UUID,
    status_code: int,
    detail: str,
) -> FailedMutation:
    detail = detail[:ERROR_EXCERPT_MAX_LENGTH]
    operation = _find_scoped_operation(session, org, operation_id, for_update=True)
    assert operation is not None
    assert 400 <= status_code <= 599
    match operation.state:
        case MutationOperationState.PROCESSING:
            operation.state = MutationOperationState.FAILED
            operation.failure_status_code = status_code
            operation.failure_detail = detail
            operation.updated_at = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
            session.add(operation)
        case MutationOperationState.FAILED:
            assert operation.failure_status_code == status_code
            assert operation.failure_detail == detail
        case MutationOperationState.SUCCEEDED | MutationOperationState.UNCERTAIN:
            raise AssertionError(f"Cannot fail a {operation.state.value} mutation")
    status = _status(operation)
    session.commit()
    assert isinstance(status, FailedMutation)
    return status


def mark_mutation_uncertain(session: Session, org: Org, operation_id: UUID) -> MutationStatus:
    operation = _find_scoped_operation(session, org, operation_id, for_update=True)
    assert operation is not None
    match operation.state:
        case MutationOperationState.PROCESSING:
            operation.state = MutationOperationState.UNCERTAIN
            operation.updated_at = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
            session.add(operation)
        case MutationOperationState.SUCCEEDED | MutationOperationState.FAILED | MutationOperationState.UNCERTAIN:
            pass
    status = _status(operation)
    session.commit()
    return status


def get_mutation_status(session: Session, org: Org, operation_id: UUID) -> MutationStatus | None:
    operation = _find_scoped_operation(session, org, operation_id, for_update=True)
    if operation is None:
        session.commit()
        return None
    return _locked_status(session, operation)


def _find_scoped_operation(
    session: Session,
    org: Org,
    operation_id: UUID,
    *,
    for_update: bool = False,
) -> MutationOperation | None:
    statement = select(MutationOperation).where(
        MutationOperation.org_id == org.id,
        MutationOperation.operation_id == operation_id,
    )
    if for_update:
        statement = statement.with_for_update()
    return session.exec(statement).one_or_none()


def _locked_status(session: Session, operation: MutationOperation) -> MutationStatus:
    stale_before = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None) - STALE_PROCESSING_AFTER
    if operation.state == MutationOperationState.PROCESSING and operation.updated_at <= stale_before:
        operation.state = MutationOperationState.UNCERTAIN
        operation.updated_at = datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)
        session.add(operation)
    status = _status(operation)
    session.commit()
    return status


def _status(operation: MutationOperation) -> MutationStatus:
    match operation.state:
        case MutationOperationState.PROCESSING:
            return ProcessingMutation(kind=operation.kind)
        case MutationOperationState.SUCCEEDED:
            assert operation.response is not None
            return SucceededMutation(kind=operation.kind, response=operation.response)
        case MutationOperationState.FAILED:
            assert operation.failure_status_code is not None
            assert operation.failure_detail is not None
            return FailedMutation(
                kind=operation.kind,
                status_code=operation.failure_status_code,
                detail=operation.failure_detail,
            )
        case MutationOperationState.UNCERTAIN:
            return UncertainMutation(kind=operation.kind)
    assert_never(operation.state)
