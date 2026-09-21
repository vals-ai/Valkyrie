"""Exact completed relocation predecessor replacement without an admission gap."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import model_validator
from sqlmodel import Session, col, select

from tracker.aws.runtime import AWSResources
from tracker.database.models import Benchmark, RunLifecycle
from tracker.lifecycle import (
    ContractModel,
    Digest,
    LifecycleConflict,
    OperationIdentity,
    Purpose,
    RunScope,
    acquire_hold,
)
from tracker.storage_migration_exchange import CopiedObject, DestinationVersion, RunObservation


def canonical_digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class RelocationPredecessor(ContractModel):
    kind: Literal["released_relocation", "completed_history_only"]
    operation_id: UUID
    identity_sha256: Digest
    scope_sha256: Digest
    completion_sha256: Digest | None = None


class RelocationCheckpoint(ContractModel):
    schema_version: Literal[1] = 1
    identity_sha256: Digest
    scope_sha256: Digest
    child_plan_sha256: Digest
    execution_arguments_sha256: Digest
    execution_policy: Literal["portable", "history_only"]
    destination_resources: AWSResources
    dispatch_ids: tuple[UUID, ...]
    copied_objects_sha256: Digest | None = None
    destination_versions_sha256: Digest | None = None
    copied_objects: tuple[CopiedObject, ...] = ()
    destination_versions: tuple[DestinationVersion, ...] = ()
    parent_completion_sha256: Digest | None = None
    phase: Literal["held", "prepared", "relocated", "released", "relocated_history_only"] = "held"
    receipt: RunObservation | None = None

    @model_validator(mode="after")
    def completed_proof(self) -> "RelocationCheckpoint":
        if self.dispatch_ids != tuple(sorted(set(self.dispatch_ids), key=str)):
            raise ValueError("Dispatch scope must be sorted and unique")

        if self.phase in {"relocated", "released", "relocated_history_only"} and (
            self.copied_objects_sha256 is None or self.destination_versions_sha256 is None
        ):
            raise ValueError("Relocation requires complete copy proof")

        if self.copied_objects_sha256 is not None and (
            self.copied_objects_sha256
            != canonical_digest([item.model_dump(mode="json") for item in self.copied_objects])
            or self.destination_versions_sha256
            != canonical_digest([item.model_dump(mode="json") for item in self.destination_versions])
        ):
            raise ValueError("Durable object evidence digest does not match")

        if self.phase in {"released", "relocated_history_only"} and (
            self.parent_completion_sha256 is None or (self.phase == "released") != (self.execution_policy == "portable")
        ):
            raise ValueError("Completed relocation requires matching policy and parent completion")
        return self


def completion_digest(checkpoint: RelocationCheckpoint) -> str:
    """The completion proof covers the relocation evidence, never the replayed operator receipt."""
    return canonical_digest(checkpoint.model_dump(mode="json", exclude={"receipt"}))


def capture_predecessor(record: RunLifecycle, identity: OperationIdentity, scope: RunScope) -> RelocationPredecessor:
    previous_identity = OperationIdentity.model_validate_json(record.identity_json)
    previous_scope = RunScope.model_validate_json(record.scope_json)
    if (
        record.purpose != "relocation"
        or previous_scope.run_id != scope.run_id
        or record.run_id != scope.run_id
        or scope.run_id not in previous_identity.run_ids
        or any(
            getattr(previous_identity, field) != getattr(identity, field)
            for field in (
                "github_owner_id",
                "org_id",
                "source_aws_account_id",
                "destination_aws_account_id",
                "region",
                "environment",
                "database_target",
            )
        )
    ):
        raise LifecycleConflict("Predecessor is outside current operation scope")

    completion = None
    if record.released_at is not None and record.phase == "released":
        kind = "released_relocation"
    elif record.released_at is None and record.phase == "relocated_history_only":
        checkpoint = RelocationCheckpoint.model_validate_json(record.checkpoint_json or "null")
        if (
            checkpoint.phase != record.phase
            or checkpoint.identity_sha256 != canonical_digest(previous_identity.model_dump(mode="json"))
            or checkpoint.scope_sha256 != canonical_digest(previous_scope.model_dump(mode="json"))
            or checkpoint.destination_resources != scope.original_resources
        ):
            raise LifecycleConflict("Completed history checkpoint or current resources do not match")
        kind = "completed_history_only"
        completion = completion_digest(checkpoint)
    else:
        raise LifecycleConflict("Predecessor is not a completed relocation")

    return RelocationPredecessor(
        kind=kind,
        operation_id=previous_identity.operation_id,
        identity_sha256=canonical_digest(previous_identity.model_dump(mode="json")),
        scope_sha256=canonical_digest(previous_scope.model_dump(mode="json")),
        completion_sha256=completion,
    )


def acquire_successor_hold(
    session: Session,
    *,
    identity: OperationIdentity,
    scope: RunScope,
    purpose: Purpose,
    predecessor: RelocationPredecessor | None,
) -> RunLifecycle:
    """Replace only an exact reviewed completed record under refreshed row locks."""
    if scope.run_id not in identity.run_ids or scope.original_resources.region != identity.region:
        raise LifecycleConflict("Successor run scope is outside the operation identity")

    with session.no_autoflush:
        benchmark = session.exec(
            select(Benchmark)
            .where(col(Benchmark.id) == scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        previous = session.exec(
            select(RunLifecycle)
            .where(col(RunLifecycle.run_id) == scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        if previous is not None and previous.identity_json == identity.model_dump_json():
            return acquire_hold(session, identity=identity, scope=scope, purpose=purpose)

        observed = None if previous is None else capture_predecessor(previous, identity, scope)
        if predecessor != observed:
            raise LifecycleConflict("Exact planned predecessor changed")

        if (
            benchmark is None
            or benchmark.org_id != identity.org_id
            or benchmark.arguments.properties != scope.original_resources
        ):
            raise LifecycleConflict("Current saved run scope does not match successor")

        if observed is None or observed.kind == "released_relocation":
            return acquire_hold(
                session,
                identity=identity,
                scope=scope,
                purpose=purpose,
                replace_released_operation_id=None if observed is None else observed.operation_id,
            )

        if identity.operation_id == observed.operation_id:
            raise LifecycleConflict("Successor requires a new operation identity")
        assert previous is not None
        session.delete(previous)
        session.flush()
        record = RunLifecycle(
            run_id=scope.run_id,
            identity_json=identity.model_dump_json(),
            scope_json=scope.model_dump_json(),
            purpose=purpose,
            acquired_at=datetime.now(UTC),
            phase="held",
        )
        session.add(record)
        session.flush()
        return record
