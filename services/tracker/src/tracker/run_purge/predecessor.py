"""Explicit replacement of one planned, completed relocation."""

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, acquire_hold
from tracker.run_purge.contracts import PurgeRun, ReleasedRelocation


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _abandoned_deletion(record: RunLifecycle) -> bool:
    return record.purpose == "deletion" and record.phase == "abandoned" and record.released_at is not None


def _previous_identity(record: RunLifecycle) -> OperationIdentity:
    try:
        return OperationIdentity.model_validate_json(record.identity_json)
    except ValidationError as error:
        raise LifecycleConflict("Invalid lifecycle predecessor identity") from error


def _snapshot(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> ReleasedRelocation:
    if record.purpose != "relocation" or record.released_at is None or record.phase != "released":
        raise LifecycleConflict("A predecessor must be a released relocation")

    previous_identity = _previous_identity(record)
    try:
        previous_scope = RunScope.model_validate_json(record.scope_json)
    except ValidationError as error:
        raise LifecycleConflict("Invalid relocation predecessor identity or scope") from error

    if (
        record.run_id != run_id
        or previous_scope.run_id != run_id
        or run_id not in previous_identity.run_ids
        or previous_identity.org_id != identity.org_id
        or previous_identity.github_owner_id != identity.github_owner_id
        or previous_identity.database_target != identity.database_target
    ):
        raise LifecycleConflict("Relocation predecessor is outside the deletion owner or database scope")

    return ReleasedRelocation(
        operation_id=previous_identity.operation_id,
        identity_sha256=hashlib.sha256(record.identity_json.encode()).hexdigest(),
        scope_sha256=hashlib.sha256(record.scope_json.encode()).hexdigest(),
        acquired_at=_utc(record.acquired_at),
        released_at=_utc(record.released_at),
    )


def capture_predecessor(session: Session, identity: OperationIdentity, run_id: UUID) -> ReleasedRelocation | None:
    record = session.exec(
        select(RunLifecycle).where(col(RunLifecycle.run_id) == run_id).execution_options(populate_existing=True)
    ).one_or_none()
    if record is None or _abandoned_deletion(record):
        return None

    return _snapshot(record, identity, run_id)


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

    if previous is not None and _abandoned_deletion(previous):
        if run.released_relocation is not None:
            raise LifecycleConflict("Planned relocation predecessor changed")

        return acquire_hold(
            session,
            identity=identity,
            scope=run.scope,
            purpose="deletion",
            replace_released_operation_id=_previous_identity(previous).operation_id,
        )

    observed = None if previous is None else _snapshot(previous, identity, run.scope.run_id)
    if observed != run.released_relocation:
        raise LifecycleConflict("Planned relocation predecessor changed")

    return acquire_hold(
        session,
        identity=identity,
        scope=run.scope,
        purpose="deletion",
        replace_released_operation_id=None if observed is None else observed.operation_id,
    )
