"""Explicit replacement of one planned, completed relocation."""

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from pydantic import ValidationError
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, acquire_hold
from tracker.lifecycle_completion import (
    RelocationPredecessor,
    canonical_digest,
)
from tracker.lifecycle_completion import (
    capture_predecessor as capture_relocation,
)
from tracker.run_purge.contracts import PurgeRun, ReleasedRelocation
from tracker.run_transfer.contracts import TransferCheckpoint


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _snapshot(record: RunLifecycle, identity: OperationIdentity, run_id: UUID) -> ReleasedRelocation:
    if record.purpose != "relocation" or record.released_at is None or record.phase != "released":
        raise LifecycleConflict("A predecessor must be a released relocation")

    try:
        previous_identity = OperationIdentity.model_validate_json(record.identity_json)
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
    return None if record is None else _snapshot(record, identity, run_id)


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
