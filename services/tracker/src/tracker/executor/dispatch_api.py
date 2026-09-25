"""Database transactions shared by the versioned executor dispatch endpoints."""

import hashlib
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func
from sqlmodel import Session, select

from executor_protocol import DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchAccess,
    ExecutorDispatchStatus,
)
from tracker.executor.dispatch_control import active_dispatch_exists, record_dispatch_failure


class DispatchAccessDenied(PermissionError):
    """The request does not carry this dispatch's API credential."""


class DispatchConflict(Exception):
    """The dispatch cannot accept this operation from this claimant."""


@dataclass(frozen=True)
class DispatchIdentity:
    benchmark_id: UUID
    release_id: str
    artifact_uri: str
    artifact_digest: str
    protocol_version: str


@dataclass(frozen=True)
class DispatchAuthorityState:
    current: bool
    lease_expires_at: datetime | None
    server_time: datetime


def create_dispatch_access(session: Session, dispatch: ExecutorDispatch) -> str:
    """Issue a credential in the admission transaction; enqueue it only after commit.

    The plaintext is returned once and must never appear in logs or run metadata.
    Existing dispatches deliberately receive no API access during migration.
    """
    token = secrets.token_urlsafe(32)
    session.add(
        ExecutorDispatchAccess(
            dispatch_id=dispatch.id,
            token_digest=hashlib.sha256(token.encode()).hexdigest(),
        )
    )
    session.flush()

    return token


def authenticate_dispatch(session: Session, dispatch_id: UUID, token: str) -> ExecutorDispatchAccess:
    access = session.get(ExecutorDispatchAccess, dispatch_id)
    digest = hashlib.sha256(token.encode()).hexdigest()
    expected = access.token_digest if access is not None else "0" * 64
    if not secrets.compare_digest(digest, expected) or access is None:
        raise DispatchAccessDenied("Invalid executor credential")

    return access


def _lock_dispatch(session: Session, dispatch_id: UUID) -> tuple[Benchmark, ExecutorDispatch, ExecutorDispatchAccess]:
    benchmark_id = session.exec(
        select(ExecutorDispatch.benchmark_id).where(ExecutorDispatch.id == dispatch_id)
    ).one_or_none()
    if benchmark_id is None:
        raise DispatchConflict("Executor dispatch no longer exists")

    # Match cancellation and reconciliation: benchmark first, then dispatch.
    benchmark = session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()
    dispatch = session.exec(
        select(ExecutorDispatch)
        .where(ExecutorDispatch.id == dispatch_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()
    access = session.exec(
        select(ExecutorDispatchAccess)
        .where(ExecutorDispatchAccess.dispatch_id == dispatch_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()

    return benchmark, dispatch, access


def database_now(session: Session) -> datetime:
    # Read after acquiring locks; transaction-start time can precede a long lock wait.
    return as_utc(session.exec(select(func.clock_timestamp())).one()).replace(tzinfo=None)


def _unexpired(expires_at: datetime | None, now: datetime) -> bool:
    return expires_at is not None and expires_at.replace(tzinfo=None) > now


def _is_current(
    benchmark: Benchmark,
    dispatch: ExecutorDispatch,
    access: ExecutorDispatchAccess,
    claimant_id: UUID,
    now: datetime,
) -> bool:
    return (
        access.claimant_id == claimant_id
        and dispatch.status == ExecutorDispatchStatus.RUNNING
        and benchmark.status != BenchmarkStatus.STOPPED
        and _unexpired(dispatch.lease_expires_at, now)
    )


def claim_dispatch(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    identity: DispatchIdentity,
) -> ExecutorDispatch:
    benchmark, dispatch, access = _lock_dispatch(session, dispatch_id)
    now = database_now(session)
    if identity != DispatchIdentity(
        benchmark_id=dispatch.benchmark_id,
        release_id=dispatch.executor_release_id,
        artifact_uri=dispatch.executor_artifact_uri,
        artifact_digest=dispatch.executor_artifact_digest,
        protocol_version=dispatch.executor_protocol_version,
    ):
        raise DispatchConflict("Executor release identity does not match the dispatch")

    # Only this process may replay its lost claim response; redelivery uses a new id.
    if _is_current(benchmark, dispatch, access, claimant_id, now):
        return dispatch
    if (
        dispatch.status != ExecutorDispatchStatus.QUEUED
        or benchmark.status != BenchmarkStatus.IN_PROGRESS
        or access.claimant_id is not None
        or not _unexpired(dispatch.claim_deadline_at, now)
    ):
        raise DispatchConflict("Executor dispatch cannot be claimed")

    access.claimant_id = claimant_id
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = now
    dispatch.heartbeat_at = now
    dispatch.lease_expires_at = now + timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS)
    session.add_all([access, dispatch])
    session.flush()

    return dispatch


def dispatch_authority(session: Session, dispatch_id: UUID, claimant_id: UUID) -> DispatchAuthorityState:
    benchmark, dispatch, access = _lock_dispatch(session, dispatch_id)
    now = database_now(session)
    current = _is_current(benchmark, dispatch, access, claimant_id, now)

    return DispatchAuthorityState(
        current=current,
        lease_expires_at=as_utc(dispatch.lease_expires_at) if current and dispatch.lease_expires_at else None,
        server_time=as_utc(now),
    )


def lock_claimed_dispatch(
    session: Session, dispatch_id: UUID, claimant_id: UUID
) -> tuple[Benchmark, ExecutorDispatch, bool]:
    """Read run state for the exact claimant, including a revoked claim's final state."""
    benchmark, dispatch, access = _lock_dispatch(session, dispatch_id)
    if access.claimant_id != claimant_id:
        raise DispatchConflict("Executor dispatch belongs to another claimant")

    return benchmark, dispatch, _is_current(benchmark, dispatch, access, claimant_id, database_now(session))


def heartbeat_dispatch(session: Session, dispatch_id: UUID, claimant_id: UUID) -> ExecutorDispatch:
    benchmark, dispatch, access = _lock_dispatch(session, dispatch_id)
    now = database_now(session)
    if not _is_current(benchmark, dispatch, access, claimant_id, now):
        raise DispatchConflict("Executor dispatch authority was revoked")

    dispatch.heartbeat_at = now
    dispatch.lease_expires_at = now + timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS)
    session.add(dispatch)
    session.flush()

    return dispatch


def complete_dispatch(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    *,
    error_message: str | None = None,
) -> ExecutorDispatch:
    benchmark, dispatch, access = _lock_dispatch(session, dispatch_id)
    operation = "fail" if error_message is not None else "finish"
    request_digest = hashlib.sha256(f"{operation}\n{error_message or ''}".encode()).hexdigest()
    if access.claimant_id != claimant_id:
        raise DispatchConflict("Executor dispatch belongs to another claimant")

    if access.terminal_operation is not None:
        if access.terminal_operation != operation or access.terminal_request_digest != request_digest:
            raise DispatchConflict("Executor terminal request differs from the committed request")
        return dispatch

    now = database_now(session)
    if not _is_current(benchmark, dispatch, access, claimant_id, now):
        raise DispatchConflict("Executor dispatch authority was revoked")

    if error_message is not None:
        if dispatch.assigned_task_ids is None:
            raise DispatchConflict("Executor dispatch has no persisted task assignment")
        record_dispatch_failure(
            session,
            benchmark=benchmark,
            dispatch_id=dispatch_id,
            task_ids=dispatch.assigned_task_ids,
            error_message=error_message,
            producer="executor",
            operation="process_benchmark",
            error_type="ExecutorFailed",
            failure_reason="EXECUTOR_FAILED",
        )
    else:
        if benchmark.status == BenchmarkStatus.STOPPING:
            raise DispatchConflict("Executor benchmark is stopping")
        dispatch.status = ExecutorDispatchStatus.FINISHED
        dispatch.finished_at = now
        session.add(dispatch)
        session.flush()
        if benchmark.status == BenchmarkStatus.IN_PROGRESS and not active_dispatch_exists(session, benchmark.id):
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = "Executor exited without finalizing benchmark"
            session.add(benchmark)

    access.terminal_operation = operation
    access.terminal_request_digest = request_digest
    session.add(access)
    session.flush()

    return dispatch


def as_utc(value: datetime) -> datetime:
    """PostgreSQL stores these timestamps without a zone; the API always emits UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
