"""Typed observations for lifecycle tools; receipts never replace fresh checks."""

import hashlib
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, field_validator
from sqlmodel import Session, col, select

from tracker.aws.runtime import AWSResources
from tracker.database.models import ExecutorDispatch, ExecutorDispatchStatus
from tracker.lifecycle import (
    ContractModel,
    Digest,
    LifecycleConflict,
    OperationIdentity,
    Purpose,
    RunScope,
    SafeIdentity,
    require_owned_hold,
)

HOST_CONTRACT = "stable-host-lifecycle-v1"


class HostContractObservation(ContractModel):
    """Built by the operator after inspecting the complete deployed host inventory."""

    contract: Literal["stable-host-lifecycle-v1"]
    deployment_sha256: Digest
    host_inventory: tuple[SafeIdentity, ...]
    observed_at: AwareDatetime
    acknowledgement_required_since: AwareDatetime
    verifier: SafeIdentity
    legacy_dispatch_ids: tuple[UUID, ...] = ()

    @field_validator("host_inventory")
    @classmethod
    def validate_inventory(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(set(value))):
            raise ValueError("Host inventory must be complete, sorted and unique")
        return value


class ExternalHostDrain(ContractModel):
    provenance: Literal["externally_confirmed_host_drain"]
    identity: OperationIdentity
    run_id: UUID
    hold_acquired_at: AwareDatetime
    dispatch_ids: tuple[UUID, ...]
    host_inventory: tuple[SafeIdentity, ...]
    deployed_host_contract: SafeIdentity
    observed_at: AwareDatetime
    verifier: SafeIdentity
    evidence_sha256: Digest
    confirmation: Literal["all_inventory_hosts_terminated_and_old_claims_disabled"]


class DispatchDrain(ContractModel):
    dispatch_id: UUID
    provenance: Literal[
        "host_process_exit",
        "verified_finished_contract",
        "held_unclaimed",
        "externally_confirmed_host_drain",
        "pending",
    ]
    observed_exit_at: datetime | None = None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def classify_dispatch(dispatch: ExecutorDispatch, *, host_contract: HostContractObservation | None) -> DispatchDrain:
    """Classify a freshly read dispatch while its run is held by the caller."""
    provenance: Literal["host_process_exit", "verified_finished_contract", "held_unclaimed", "pending"] = "pending"
    if dispatch.process_exited_at is not None:
        provenance = "host_process_exit"
    elif host_contract is not None:
        if dispatch.status == ExecutorDispatchStatus.FINISHED and dispatch.started_at is not None:
            provenance = "verified_finished_contract"
        elif dispatch.started_at is None and dispatch.status != ExecutorDispatchStatus.RUNNING:
            provenance = "held_unclaimed"
    return DispatchDrain(dispatch_id=dispatch.id, provenance=provenance, observed_exit_at=dispatch.process_exited_at)


def verify_drain(
    session: Session,
    *,
    identity: OperationIdentity,
    scope: RunScope,
    purpose: Purpose,
    host_contract: HostContractObservation | None,
    external: ExternalHostDrain | None = None,
    external_evidence: bytes | None = None,
) -> tuple[DispatchDrain, ...]:
    """Read current dispatch evidence under exact active ownership; return pending gaps.

    Host observations must come from the caller's current deployment inspection.
    External evidence is an operator attestation, never a tool process-exit check.
    """
    record = require_owned_hold(session, identity=identity, scope=scope, purpose=purpose)
    if record.released_at is not None:
        raise LifecycleConflict("Drain requires an active hold")
    dispatches = session.exec(
        select(ExecutorDispatch)
        .where(col(ExecutorDispatch.benchmark_id) == scope.run_id)
        .order_by(col(ExecutorDispatch.id))
        .execution_options(populate_existing=True)
        .with_for_update()
    ).all()
    results = tuple(classify_dispatch(dispatch, host_contract=host_contract) for dispatch in dispatches)
    if external is None:
        return results

    legacy_ids = tuple(
        dispatch.id
        for dispatch, result in zip(dispatches, results, strict=True)
        if result.provenance == "pending" and dispatch.started_at is not None
    )
    if (
        host_contract is None
        or external.identity != identity
        or external.run_id != scope.run_id
        or external.hold_acquired_at != _aware(record.acquired_at)
        or external.observed_at < _aware(record.acquired_at)
        or external.observed_at > datetime.now(UTC)
        or external.dispatch_ids != legacy_ids
        or not legacy_ids
        or external.host_inventory != host_contract.host_inventory
        or external.deployed_host_contract != host_contract.contract
        or external_evidence is None
        or hashlib.sha256(external_evidence).hexdigest() != external.evidence_sha256
        or not set(legacy_ids).issubset(host_contract.legacy_dispatch_ids)
    ):
        raise LifecycleConflict("External host-drain evidence does not match the exact legacy scope")
    for dispatch in dispatches:
        if dispatch.id in legacy_ids and (
            dispatch.started_at is None or _aware(dispatch.started_at) >= host_contract.acknowledgement_required_since
        ):
            raise LifecycleConflict("External legacy evidence cannot hide a current host acknowledgement failure")
    return tuple(
        result.model_copy(update={"provenance": "externally_confirmed_host_drain"})
        if result.dispatch_id in legacy_ids
        else result
        for result in results
    )


class CopiedObject(ContractModel):
    key: Annotated[str, Field(min_length=1)]
    source_version_id: Annotated[str, Field(min_length=1)]
    destination_version_id: Annotated[str, Field(min_length=1)]
    checksum_sha256: Digest | None
    state: Literal["version", "current_object", "current_delete_marker", "delete_marker"]


class RunReport(ContractModel):
    scope: RunScope
    phase: SafeIdentity
    destination_resources: AWSResources | None = None
    copied_objects: tuple[CopiedObject, ...] = ()
    dispatch_drain: tuple[DispatchDrain, ...] = ()
    external_host_drain: ExternalHostDrain | None = None


class LifecycleReport(ContractModel):
    identity: OperationIdentity
    purpose: Purpose
    observed_at: AwareDatetime
    host_contract: HostContractObservation | None = None
    runs: tuple[RunReport, ...]


def write_report(path: Path, report: LifecycleReport) -> None:
    """Atomically replace a private report without exposing partial receipts."""
    if tuple(run.scope.run_id for run in report.runs) != report.identity.run_ids:
        raise LifecycleConflict("Report run scope does not match the immutable identity")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(report.model_dump_json(indent=2))
            output.flush()
            os.fsync(output.fileno())
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)
