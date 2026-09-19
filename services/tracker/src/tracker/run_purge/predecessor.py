"""Explicit replacement of one planned, completed relocation."""

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, acquire_hold
from tracker.run_purge.contracts import AbandonedDeletion, PurgeRun, ReleasedRelocation


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _abandoned_deletion(record: RunLifecycle) -> bool:
    return record.purpose == "deletion" and record.phase == "abandoned" and record.released_at is not None


def _previous_identity(record: RunLifecycle) -> OperationIdentity:
    try:
        return OperationIdentity.model_validate_json(record.identity_json)
    except ValidationError as error:
        raise LifecycleConflict("Invalid lifecycle predecessor identity") from error


def _validated_previous(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> OperationIdentity:
    previous_identity = _previous_identity(record)
    try:
        previous_scope = RunScope.model_validate_json(record.scope_json)
    except ValidationError as error:
        raise LifecycleConflict("Invalid lifecycle predecessor scope") from error

    if (
        record.run_id != run_id
        or previous_scope.run_id != run_id
        or run_id not in previous_identity.run_ids
        or previous_identity.org_id != identity.org_id
        or previous_identity.github_owner_id != identity.github_owner_id
        or previous_identity.database_target != identity.database_target
    ):
        raise LifecycleConflict("Lifecycle predecessor is outside the deletion owner or database scope")

    return previous_identity


def _snapshot(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> ReleasedRelocation:
    if record.purpose != "relocation" or record.released_at is None or record.phase != "released":
        raise LifecycleConflict("A predecessor must be a released relocation or an abandoned deletion")

    previous_identity = _validated_previous(record, identity, run_id)

    return ReleasedRelocation(
        operation_id=previous_identity.operation_id,
        identity_sha256=hashlib.sha256(record.identity_json.encode()).hexdigest(),
        scope_sha256=hashlib.sha256(record.scope_json.encode()).hexdigest(),
        acquired_at=_utc(record.acquired_at),
        released_at=_utc(record.released_at),
    )


def _abandoned_snapshot(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> AbandonedDeletion:
    if not _abandoned_deletion(record) or record.released_at is None:
        raise LifecycleConflict("A predecessor must be a released relocation or an abandoned deletion")

    previous_identity = _validated_previous(record, identity, run_id)

    return AbandonedDeletion(
        operation_id=previous_identity.operation_id,
        identity_sha256=hashlib.sha256(record.identity_json.encode()).hexdigest(),
        scope_sha256=hashlib.sha256(record.scope_json.encode()).hexdigest(),
        acquired_at=_utc(record.acquired_at),
        released_at=_utc(record.released_at),
    )


Predecessor = tuple[ReleasedRelocation | None, AbandonedDeletion | None]


def _observed_predecessor(record: RunLifecycle | None, identity: OperationIdentity, run_id: UUID) -> Predecessor:
    if record is None:
        return None, None

    if _abandoned_deletion(record):
        return None, _abandoned_snapshot(record, identity, run_id)

    return _snapshot(record, identity, run_id), None


def capture_predecessor(session: Session, identity: OperationIdentity, run_id: UUID) -> Predecessor:
    record = session.exec(
        select(RunLifecycle).where(col(RunLifecycle.run_id) == run_id).execution_options(populate_existing=True)
    ).one_or_none()

    return _observed_predecessor(record, identity, run_id)


def acquire_deletion_hold(session: Session, identity: OperationIdentity, run: PurgeRun) -> RunLifecycle:
    # Keep the shared run-before-control lock order through validation and replacement.
    session.exec(
        select(Benchmark)
        .where(col(Benchmark.id) == run.scope.run_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    previous = session.exec(
        select(RunLifecycle)
        .where(col(RunLifecycle.run_id) == run.scope.run_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()

    if previous is not None and previous.identity_json == identity.model_dump_json() and previous.purpose == "deletion":
        return acquire_hold(session, identity=identity, scope=run.scope, purpose="deletion")

    relocation, abandoned = _observed_predecessor(previous, identity, run.scope.run_id)
    if relocation != run.released_relocation or abandoned != run.abandoned_deletion:
        raise LifecycleConflict("Planned lifecycle predecessor changed")

    planned = relocation or abandoned

    return acquire_hold(
        session,
        identity=identity,
        scope=run.scope,
        purpose="deletion",
        replace_released_operation_id=None if planned is None else planned.operation_id,
    )
