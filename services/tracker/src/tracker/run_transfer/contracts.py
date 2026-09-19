"""Versioned paired transfer exchange. Payloads remain in the two databases."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from tracker.lifecycle import ContractModel, Digest, OperationIdentity, RunScope
from tracker.lifecycle_completion import RelocationPredecessor
from tracker.lifecycle_evidence import DispatchDrain, ExternalHostDrain, HostContractObservation
from tracker.run_purge.contracts import ProviderLocator
from tracker.runtime.log_history import ArchiveReport
from tracker.storage_migration_exchange import CopiedObject, DestinationVersion, ObjectTransformation, canonical_digest

Action = Literal["plan", "prepare", "inspect", "import", "cleanup", "finalize"]
Phase = Literal["held", "prepared", "transferred", "transferred_source_retired", "transferred_history_only", "released"]


class ReleaseMapping(ContractModel):
    source_id: str
    destination_id: str
    source_artifact_uri: str
    destination_artifact_uri: str
    artifact_digest: str
    protocol_version: str


class ReferenceEdit(ContractModel):
    pointer: Literal["/arguments/dataset", "/arguments/sandbox_provider_secret_name", "/webhook_secret_name"]
    original_sha256: Digest
    replacement: str = Field(min_length=1)


class TransferRun(ContractModel):
    source: RunScope
    destination: RunScope
    source_rows_sha256: Digest | None = None
    execution_policy: Literal["portable", "history_only"]
    predecessor: RelocationPredecessor | None = None
    reference_edits: tuple[ReferenceEdit, ...] = ()
    transformations: tuple[ObjectTransformation, ...] = ()
    unmasked_read_authorized: Literal[True]


class TransferPlan(ContractModel):
    schema_version: Literal[1] = 1
    source_identity: OperationIdentity
    destination_identity: OperationIdentity
    org_name: str
    releases: tuple[ReleaseMapping, ...] = ()
    runs: tuple[TransferRun, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def pair(self) -> "TransferPlan":
        source, destination = self.source_identity, self.destination_identity
        if source.model_dump(exclude={"database_target", "region"}) != destination.model_dump(
            exclude={"database_target", "region"}
        ):
            raise ValueError("Paired operation identities differ")
        if (
            source.database_target == destination.database_target
            or source.source_aws_account_id == source.destination_aws_account_id
        ):
            raise ValueError("Transfer requires distinct databases and accounts")
        if tuple(run.source.run_id for run in self.runs) != source.run_ids:
            raise ValueError("Exact sorted transfer run set differs")
        for run in self.runs:
            if len({edit.pointer for edit in run.reference_edits}) != len(run.reference_edits):
                raise ValueError("Duplicate reference edit")
            if (
                run.source.run_id != run.destination.run_id
                or run.source.original_resources.region != source.region
                or run.destination.original_resources.region != destination.region
            ):
                raise ValueError("Local resource scope differs")
            if run.source.original_resources.s3_bucket == run.destination.original_resources.s3_bucket:
                raise ValueError("Cross-account destination bucket must differ")
        if len({release.source_id for release in self.releases}) != len(self.releases):
            raise ValueError("Duplicate release mapping")
        return self

    @property
    def sha256(self) -> str:
        return canonical_digest(self.model_dump(mode="json"))


class ParentCompletion(ContractModel):
    operation_id: UUID
    parent_plan_sha256: Digest
    child_plan_sha256: Digest
    valsmith_commit_sha256: Digest
    object_completion_sha256: Digest
    destination_rows_sha256: Digest
    archives_sha256: Digest


class TransferRequest(ContractModel):
    schema_version: Literal[1] = 1
    action: Action = "plan"
    nonce: UUID
    plan: TransferPlan
    copied_objects: tuple[CopiedObject, ...] = ()
    destination_versions: tuple[DestinationVersion, ...] = ()
    source_host_contract: HostContractObservation | None = None
    external_host_drains: tuple[ExternalHostDrain, ...] = ()
    external_evidence_files: tuple[str, ...] = ()
    parent_completion: ParentCompletion | None = None


class TableProof(ContractModel):
    count: int = Field(ge=0)
    sha256: Digest


class StoredReference(ContractModel):
    table: str
    row_id: UUID
    pointer: str
    kind: Literal["secret_locator", "external_locator", "execution_input"]
    value_sha256: Digest


class TransferObservation(ContractModel):
    run_id: UUID
    source_rows_sha256: Digest | None
    destination_rows_sha256: Digest | None
    predecessor: RelocationPredecessor | None = None
    source_hold_identity: OperationIdentity | None = None
    source_hold_scope: RunScope | None = None
    destination_hold_identity: OperationIdentity | None = None
    destination_hold_scope: RunScope | None = None
    references: tuple[StoredReference, ...] = ()
    provider_absence: Literal["not_inspected", "verified_absent"] = "not_inspected"
    dispatches: tuple[DispatchDrain, ...] = ()
    drain_origin: Literal["not_inspected", "current_source", "retired_source_checkpoint"] = "not_inspected"
    host_observation_sha256: Digest | None = None
    source_tables: dict[str, TableProof]
    destination_tables: dict[str, TableProof]
    source_identity: OperationIdentity
    destination_identity: OperationIdentity
    source_scope: RunScope
    destination_scope: RunScope
    source_phase: Phase | Literal["relocated_history_only"] | None
    destination_phase: Phase | None
    destination_released_at: datetime | None
    source_checkpoint_sha256: Digest | None
    destination_checkpoint_sha256: Digest | None
    archive: ArchiveReport | None
    observed_at: datetime


class TransferResponse(ContractModel):
    copied_objects_sha256: Digest | None = None
    destination_versions_sha256: Digest | None = None
    parent_completion_sha256: Digest | None = None
    schema_version: Literal[1] = 1
    action: Action
    nonce: UUID
    child_plan_sha256: Digest
    runs: tuple[TransferObservation, ...]


class TransferCheckpoint(ContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["paired_transfer"] = "paired_transfer"
    provider: ProviderLocator
    dispatches: tuple[DispatchDrain, ...] = ()
    child_plan_sha256: Digest
    identity_sha256: Digest
    scope_sha256: Digest
    source_rows_sha256: Digest
    destination_rows_sha256: Digest | None = None
    execution_policy: Literal["portable", "history_only"]
    phase: Phase
    archive: ArchiveReport | None = None
    log_completeness_sha256: Digest | None = None
    copied_objects_sha256: Digest | None = None
    destination_versions_sha256: Digest | None = None
    parent_completion_sha256: Digest | None = None

    @model_validator(mode="after")
    def completed(self) -> "TransferCheckpoint":
        if self.phase in {"transferred", "transferred_source_retired", "transferred_history_only", "released"} and any(
            value is None
            for value in (
                self.destination_rows_sha256,
                self.archive,
                self.log_completeness_sha256,
                self.copied_objects_sha256,
                self.destination_versions_sha256,
            )
        ):
            raise ValueError("Transferred checkpoint needs complete row, object and archive proof")
        if (
            self.phase in {"transferred_source_retired", "transferred_history_only", "released"}
            and self.parent_completion_sha256 is None
        ):
            raise ValueError("Completed transfer requires parent proof")
        if self.phase == "released" and self.execution_policy != "portable":
            raise ValueError("History-only transfer cannot release")
        return self
