"""Explicit replacement of one planned, completed relocation."""

import hashlib
from uuid import UUID

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, acquire_hold, as_utc
from tracker.lifecycle_completion import (
    RelocationPredecessor,
    canonical_digest,
)
from tracker.lifecycle_completion import (
    capture_predecessor as capture_relocation,
)
from tracker.run_purge.contracts import AbandonedDeletion, PurgeRun, ReleasedRelocation
from tracker.run_transfer.contracts import TransferCheckpoint


def _abandoned_deletion(record: RunLifecycle) -> bool:
    return record.purpose == "deletion" and record.phase == "abandoned" and record.released_at is not None


def _completed_history(record: RunLifecycle) -> bool:
    return record.released_at is None and record.phase in {"relocated_history_only", "transferred_history_only"}


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
        acquired_at=as_utc(record.acquired_at),
        released_at=as_utc(record.released_at),
    )


def _abandoned_snapshot(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> AbandonedDeletion:
    if not _abandoned_deletion(record) or record.released_at is None:
        raise LifecycleConflict("A predecessor must be a released relocation or an abandoned deletion")

    previous_identity = _validated_previous(record, identity, run_id)

    return AbandonedDeletion(
        operation_id=previous_identity.operation_id,
        identity_sha256=hashlib.sha256(record.identity_json.encode()).hexdigest(),
        scope_sha256=hashlib.sha256(record.scope_json.encode()).hexdigest(),
        acquired_at=as_utc(record.acquired_at),
        released_at=as_utc(record.released_at),
    )


Predecessor = tuple[ReleasedRelocation | None, AbandonedDeletion | None, RelocationPredecessor | None]


def _observed_predecessor(record: RunLifecycle | None, identity: OperationIdentity, scope: RunScope) -> Predecessor:
    if record is None:
        return None, None, None

    if _completed_history(record):
        return None, None, capture_completed_history(record, identity, scope)

    if _abandoned_deletion(record):
        return None, _abandoned_snapshot(record, identity, scope.run_id), None

    return _snapshot(record, identity, scope.run_id), None, None


def capture_predecessor(session: Session, identity: OperationIdentity, scope: RunScope) -> Predecessor:
    record = session.exec(
        select(RunLifecycle).where(col(RunLifecycle.run_id) == scope.run_id).execution_options(populate_existing=True)
    ).one_or_none()

    return _observed_predecessor(record, identity, scope)


def acquire_deletion_hold(session: Session, identity: OperationIdentity, run: PurgeRun) -> RunLifecycle:
    # Keep the shared run-before-control lock order through validation and replacement.
    benchmark = session.exec(
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

    if run.completed_history is not None:
        if previous is None or capture_completed_history(previous, identity, run.scope) != run.completed_history:
            raise LifecycleConflict("Planned completed history predecessor changed")

        if (
            benchmark is None
            or benchmark.org_id != identity.org_id
            or benchmark.arguments.properties != run.scope.original_resources
        ):
            raise LifecycleConflict("Current saved run differs from completed history successor")

        session.delete(previous)
        session.flush()

        return acquire_hold(session, identity=identity, scope=run.scope, purpose="deletion")

    relocation, abandoned, history = _observed_predecessor(previous, identity, run.scope)
    if relocation != run.released_relocation or abandoned != run.abandoned_deletion or history is not None:
        raise LifecycleConflict("Planned lifecycle predecessor changed")

    planned = relocation or abandoned

    return acquire_hold(
        session,
        identity=identity,
        scope=run.scope,
        purpose="deletion",
        replace_released_operation_id=None if planned is None else planned.operation_id,
    )


def capture_completed_history(
    record: RunLifecycle, identity: OperationIdentity, scope: RunScope
) -> RelocationPredecessor:
    """Admit completed local history, including a prior foreign-source import."""
    old = OperationIdentity.model_validate_json(record.identity_json)
    old_scope = RunScope.model_validate_json(record.scope_json)
    if (
        record.purpose != "relocation"
        or record.released_at is not None
        or record.run_id != scope.run_id
        or old_scope.run_id != scope.run_id
        or scope.run_id not in old.run_ids
        or old.operation_id == identity.operation_id
        or old.destination_aws_account_id != identity.source_aws_account_id
        or any(
            getattr(old, name) != getattr(identity, name)
            for name in ("github_owner_id", "org_id", "database_target", "region", "environment")
        )
    ):
        raise LifecycleConflict("Completed history local authority differs")

    if record.phase == "relocated_history_only":
        return capture_relocation(record, old, scope)

    if record.phase != "transferred_history_only":
        raise LifecycleConflict("Predecessor is not completed history")

    checkpoint = TransferCheckpoint.model_validate_json(record.checkpoint_json or "null")
    if (
        checkpoint.phase != record.phase
        or checkpoint.execution_policy != "history_only"
        or old_scope != scope
        or checkpoint.identity_sha256 != canonical_digest(old.model_dump(mode="json"))
        or checkpoint.scope_sha256 != canonical_digest(old_scope.model_dump(mode="json"))
        or checkpoint.archive is None
        or checkpoint.archive.reference.run_id != scope.run_id
        or checkpoint.archive.reference.operation_id != old.operation_id
        or checkpoint.archive.reference.parent_plan_sha256 != old.parent_plan_sha256
    ):
        raise LifecycleConflict("Completed transfer history checkpoint differs")

    return RelocationPredecessor(
        kind="completed_history_only",
        operation_id=old.operation_id,
        identity_sha256=canonical_digest(old.model_dump(mode="json")),
        scope_sha256=canonical_digest(old_scope.model_dump(mode="json")),
        completion_sha256=canonical_digest(checkpoint.model_dump(mode="json")),
    )
