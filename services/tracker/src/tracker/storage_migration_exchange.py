"""Version 1 JSON exchange with the separately installed tracker operator."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


def canonical_digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
AccountId = Annotated[str, Field(pattern=r"^[0-9]{12}$")]
SafeIdentity = Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$")]
Action = Literal["inventory", "prepare", "inspect", "relocate", "release"]
ExecutionPolicy = Literal["portable", "history_only"]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AWSResources(ContractModel):
    region: SafeIdentity
    s3_bucket: Annotated[str, Field(min_length=3, max_length=63)]
    log_group: str
    log_retention_days: Annotated[int, Field(gt=0)]


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
    run_ids: tuple[UUID, ...] = Field(min_length=1, json_schema_extra={"uniqueItems": True})

    @field_validator("run_ids")
    @classmethod
    def sorted_runs(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if not value or value != tuple(sorted(set(value), key=str)):
            raise ValueError("run_ids must be nonempty, sorted and unique")
        return value


class RunScope(ContractModel):
    run_id: UUID
    original_resources: AWSResources
    object_prefix: str
    log_group: str

    @model_validator(mode="after")
    def exact_scope(self) -> RunScope:
        if (
            self.object_prefix != f"benchmarks/{self.run_id}/"
            or self.log_group != f"{self.original_resources.log_group}/{self.run_id}"
        ):
            raise ValueError("run prefix or log group does not match saved scope")
        return self


class RelocationPredecessor(ContractModel):
    kind: Literal["released_relocation", "completed_history_only"]
    operation_id: UUID
    identity_sha256: Digest
    scope_sha256: Digest
    completion_sha256: Digest | None = None

    @model_validator(mode="after")
    def completed_history(self) -> RelocationPredecessor:
        if self.kind == "completed_history_only" and self.completion_sha256 is None:
            raise ValueError("history-only predecessor requires its completed checkpoint digest")
        return self


class JsonLocatorEdit(ContractModel):
    pointer: str
    original: str
    replacement: str

    @field_validator("pointer")
    @classmethod
    def canonical_pointer(cls, value: str) -> str:
        if not value.startswith("/") or re.search(r"~(?![01])", value):
            raise ValueError("invalid JSON pointer")
        return value


class ObjectTransformation(ContractModel):
    source_bucket: str
    key: str
    source_version_id: str
    original_size: int = Field(ge=0)
    original_sha256: Digest
    rewritten_size: int = Field(ge=0)
    rewritten_sha256: Digest
    edits: tuple[JsonLocatorEdit, ...]

    @model_validator(mode="after")
    def unique_edits(self) -> ObjectTransformation:
        if not self.edits or len({edit.pointer for edit in self.edits}) != len(self.edits):
            raise ValueError("transformation requires unique nonempty locator edits")
        return self


class RelocationRun(ContractModel):
    scope: RunScope
    destination_resources: AWSResources
    expected_label: str | None
    execution_policy: ExecutionPolicy
    execution_arguments_sha256: Digest
    predecessor: RelocationPredecessor | None = None
    transformations: tuple[ObjectTransformation, ...] = ()


class RelocationPlan(ContractModel):
    schema_version: Literal[1] = 1
    identity: OperationIdentity
    runs: tuple[RelocationRun, ...]

    @model_validator(mode="after")
    def same_account(self) -> RelocationPlan:
        if self.identity.source_aws_account_id != self.identity.destination_aws_account_id:
            raise ValueError("cross-account migration requires the production transfer envelope")
        if tuple(run.scope.run_id for run in self.runs) != self.identity.run_ids:
            raise ValueError("relocation runs do not match identity")
        for run in self.runs:
            old = run.scope.original_resources
            new = run.destination_resources
            if old.region != self.identity.region or old.model_copy(update={"s3_bucket": new.s3_bucket}) != new:
                raise ValueError("same-account relocation may change only the bucket")
            seen: set[tuple[str, str, str]] = set()
            for transformation in run.transformations:
                key = (
                    transformation.source_bucket,
                    transformation.key,
                    transformation.source_version_id,
                )
                if (
                    key in seen
                    or transformation.source_bucket not in {old.s3_bucket, new.s3_bucket}
                    or not transformation.key.startswith(run.scope.object_prefix)
                ):
                    raise ValueError("transformation scope is duplicated or outside its run")
                seen.add(key)
        return self

    @property
    def sha256(self) -> str:
        return canonical_digest(self.model_dump(mode="json"))


class CopiedObject(ContractModel):
    run_id: UUID
    key: str
    source_bucket: str
    source_version_id: str
    destination_bucket: str
    destination_version_id: str
    is_delete_marker: bool
    source_sha256: Digest | None
    destination_sha256: Digest | None
    source_size: Annotated[int, Field(ge=0)]
    destination_size: Annotated[int, Field(ge=0)]
    is_current: bool
    transformation_sha256: Digest | None = None

    @model_validator(mode="after")
    def exact_key(self) -> CopiedObject:
        if not self.key.startswith(f"benchmarks/{self.run_id}/"):
            raise ValueError("copied object is outside run scope")
        if self.is_delete_marker:
            if self.source_sha256 is not None or self.destination_sha256 is not None:
                raise ValueError("delete markers have no byte checksum")
        elif self.source_sha256 is None or self.destination_sha256 is None:
            raise ValueError("live versions require both byte checksums")
        return self


class HostContractObservation(ContractModel):
    contract: Literal["stable-host-lifecycle-v1"]
    deployment_sha256: Digest
    host_inventory: tuple[SafeIdentity, ...] = Field(min_length=1, json_schema_extra={"uniqueItems": True})
    observed_at: AwareDatetime
    acknowledgement_required_since: AwareDatetime
    verifier: SafeIdentity
    legacy_dispatch_ids: tuple[UUID, ...] = ()

    @field_validator("host_inventory")
    @classmethod
    def validate_inventory(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or value != tuple(sorted(set(value))):
            raise ValueError("host inventory must be complete, sorted and unique")
        return value

    def require_current(self) -> None:
        now = datetime.now(UTC)
        if (
            self.observed_at.utcoffset() != timedelta(0)
            or self.acknowledgement_required_since.utcoffset() != timedelta(0)
            or self.observed_at > now
            or now - self.observed_at > timedelta(minutes=15)
            or self.acknowledgement_required_since > self.observed_at
        ):
            raise ValueError("host observation is stale, future, non-UTC, or has a later cutoff")


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


class DestinationVersion(ContractModel):
    run_id: UUID
    bucket: str
    key: str
    version_id: str
    is_delete_marker: bool
    size: int = Field(ge=0)
    sha256: Digest | None
    is_current: bool
    provenance: Literal["existing", "copied", "restored"]
    restored_from_version_id: str | None = None
    transformation_sha256: Digest | None = None


class DispatchObservation(ContractModel):
    dispatch_id: UUID
    status: str
    started_at: datetime | None
    process_exited_at: datetime | None
    evidence: Literal[
        "process_exited",
        "finished_current_host",
        "unclaimed_held",
        "externally_confirmed_host_drain",
    ]
    external_evidence_sha256: Digest | None = None


class ExecutionReference(ContractModel):
    pointer: str
    value_sha256: Digest
    kind: Literal["retained_s3_object", "builtin_dataset", "retired_source", "unknown"]
    bucket: str | None = None
    key: str | None = None
    version_id: str | None = None
    sha256: Digest | None = None

    @model_validator(mode="after")
    def retained_object_identity(self) -> ExecutionReference:
        if self.kind == "retained_s3_object" and any(
            value is None for value in (self.bucket, self.key, self.version_id, self.sha256)
        ):
            raise ValueError("retained execution object requires exact version and checksum")
        return self


class RunObservation(ContractModel):
    run_id: UUID
    org_id: UUID
    label: str | None
    model_sha256: Digest | None = None
    dataset_sha256: Digest | None = None
    benchmark_name: str | None = None
    predecessor: RelocationPredecessor | None = None
    execution_arguments_sha256: Digest | None = None
    execution_references: tuple[ExecutionReference, ...] = ()
    resources: AWSResources
    status: Literal["FINISHED", "ERROR", "STOPPED", "STOPPING", "IN_PROGRESS"]
    hold_identity: OperationIdentity | None = None
    hold_scope: RunScope | None = None
    hold_phase: str | None = None
    hold_purpose: Literal["relocation", "deletion"] | None = None
    hold_released_at: datetime | None = None
    deployed_host_contract_sha256: Digest | None = None
    dispatches: tuple[DispatchObservation, ...] = ()
    sandbox_ids: tuple[str, ...] = ()
    pending_task_ids: tuple[UUID, ...] = ()
    observed_at: datetime


class TrackerRequest(ContractModel):
    schema_version: Literal[1] = 1
    action: Action
    nonce: UUID
    github_owner_id: Annotated[int, Field(gt=0, strict=True)]
    org_id: UUID
    source_aws_account_id: AccountId
    destination_aws_account_id: AccountId
    region: SafeIdentity
    environment: SafeIdentity
    database_target: SafeIdentity
    run_ids: tuple[UUID, ...]
    plan: RelocationPlan | None = None
    copied_objects: tuple[CopiedObject, ...] = ()
    completion_sha256: Digest | None = None
    destination_versions: tuple[DestinationVersion, ...] = ()
    host_contract: HostContractObservation | None = None
    external_host_drains: tuple[ExternalHostDrain, ...] = ()
    external_evidence_files: tuple[str, ...] = ()


class TrackerResponse(ContractModel):
    destination_versions_sha256: Digest | None = None
    schema_version: Literal[1] = 1
    nonce: UUID
    action: Action
    child_plan_sha256: Digest | None = None
    copied_objects_sha256: Digest | None = None
    completion_sha256: Digest | None = None
    runs: tuple[RunObservation, ...]
