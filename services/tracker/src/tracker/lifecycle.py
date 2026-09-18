"""Shared, tracker-local operation identity and run admission holds.

Callers own transactions. Admission locks must precede the run lock. No provider
checks or destructive operations belong here.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func
from sqlmodel import Session, col, select

from tracker.aws.runtime import AWSResources
from tracker.database.models import Benchmark, RunLifecycle
from tracker.exceptions import TrackerServiceError

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
AccountId = Annotated[str, Field(pattern=r"^[0-9]{12}$")]
SafeIdentity = Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$")]
Purpose = Literal["relocation", "deletion"]


class LifecycleConflict(TrackerServiceError):
    """The run scope or durable operation ownership does not match."""


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class OperationIdentity(ContractModel):
    schema_version: Literal[1] = 1
    operation_id: UUID
    parent_plan_sha256: Digest
    github_owner_id: Annotated[int, Field(gt=0, strict=True)]
    org_id: UUID
    source_aws_account_id: AccountId
    destination_aws_account_id: AccountId
    region: SafeIdentity
    environment: SafeIdentity
    database_target: SafeIdentity
    run_ids: tuple[UUID, ...]

    @field_validator("run_ids")
    @classmethod
    def validate_run_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if not value or value != tuple(sorted(set(value), key=str)):
            raise ValueError("run_ids must be nonempty, sorted and unique")
        return value


class RunScope(ContractModel):
    run_id: UUID
    original_resources: AWSResources

    object_prefix: str = ""
    log_group: str = ""

    @model_validator(mode="after")
    def validate_locations(self) -> "RunScope":
        prefix = f"benchmarks/{self.run_id}/"
        group = f"{self.original_resources.log_group}/{self.run_id}"
        if self.object_prefix not in ("", prefix) or self.log_group not in ("", group):
            raise ValueError("Run prefix and log group must match the original saved scope")
        object.__setattr__(self, "object_prefix", prefix)
        object.__setattr__(self, "log_group", group)
        return self


def active_hold(session: Session, run_id: UUID) -> RunLifecycle | None:
    return session.exec(
        select(RunLifecycle)
        .where(col(RunLifecycle.run_id) == run_id, col(RunLifecycle.released_at).is_(None))
        .execution_options(populate_existing=True)
    ).one_or_none()


def require_unheld(session: Session, run_id: UUID) -> None:
    if active_hold(session, run_id) is not None:
        raise LifecycleConflict("Run has an active lifecycle hold")


def _lock_run(session: Session, run_id: UUID) -> Benchmark | None:
    return session.exec(
        select(Benchmark).where(col(Benchmark.id) == run_id).execution_options(populate_existing=True).with_for_update()
    ).one_or_none()


def _encoded_scope(scope: RunScope) -> str:
    return scope.model_dump_json()


def require_owned_hold(
    session: Session, *, identity: OperationIdentity, scope: RunScope, purpose: Purpose
) -> RunLifecycle:
    """Match every immutable field, including original resources after run removal."""
    record = session.exec(
        select(RunLifecycle)
        .where(col(RunLifecycle.run_id) == scope.run_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one_or_none()
    if (
        record is None
        or record.identity_json != identity.model_dump_json()
        or record.scope_json != _encoded_scope(scope)
        or record.purpose != purpose
    ):
        raise LifecycleConflict("Lifecycle operation or original scope does not match")
    return record


def acquire_hold(
    session: Session,
    *,
    identity: OperationIdentity,
    scope: RunScope,
    purpose: Purpose,
    replace_released_operation_id: UUID | None = None,
) -> RunLifecycle:
    """Acquire or resume exact ownership; retain deletion records after run removal."""
    if scope.run_id not in identity.run_ids or scope.original_resources.region != identity.region:
        raise LifecycleConflict("Run scope is outside the operation identity")
    with session.no_autoflush:
        benchmark = _lock_run(session, scope.run_id)
        previous = session.exec(
            select(RunLifecycle)
            .where(col(RunLifecycle.run_id) == scope.run_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one_or_none()
        if previous is not None:
            previous_identity = OperationIdentity.model_validate_json(previous.identity_json)
            if previous_identity.operation_id == identity.operation_id:
                record = require_owned_hold(session, identity=identity, scope=scope, purpose=purpose)
                if record.released_at is not None:
                    raise LifecycleConflict("A released operation cannot acquire a new hold")
                return record
            if (
                previous.purpose != "relocation"
                or previous.released_at is None
                or replace_released_operation_id != previous_identity.operation_id
            ):
                raise LifecycleConflict("Another lifecycle operation owns this run")

        if (
            benchmark is None
            or benchmark.org_id != identity.org_id
            or benchmark.arguments.properties != scope.original_resources
        ):
            raise LifecycleConflict("Saved run resources or org do not match")
        if previous is not None:
            session.delete(previous)
            session.flush()
        record = RunLifecycle(
            run_id=scope.run_id,
            identity_json=identity.model_dump_json(),
            scope_json=_encoded_scope(scope),
            purpose=purpose,
            acquired_at=datetime.now(UTC),
            phase="held",
        )
        session.add(record)
        session.flush()
        return record


def release_relocation_hold(
    session: Session,
    *,
    identity: OperationIdentity,
    scope: RunScope,
    verify_completion: Callable[[Session, RunLifecycle], None],
) -> None:
    """Explicit release phase after the operator rechecks completion under the run lock.

    The callback must read current source/destination and database state and raise
    on any incomplete phase. A saved receipt or caller boolean is not sufficient.
    """
    if _lock_run(session, scope.run_id) is None:
        raise LifecycleConflict("Cannot release a relocation hold for an absent run")
    record = require_owned_hold(session, identity=identity, scope=scope, purpose="relocation")
    verify_completion(session, record)
    if record.released_at is None:
        record.released_at = session.exec(select(func.current_timestamp())).one()
        record.phase = "released"
        session.add(record)
        session.flush()
