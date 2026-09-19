"""Version 1 private historical log format and scoped transfer input."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from tracker.runtime.log_history_reference import (
    ArchiveObject as ArchiveObject,
    LogHistoryReference as LogHistoryReference,
)

from tracker.lifecycle import AccountId, ContractModel, Digest, OperationIdentity, RunScope

Positive = Annotated[int, Field(gt=0, strict=True)]
Nonnegative = Annotated[int, Field(ge=0, strict=True)]
Nonempty = Annotated[str, Field(min_length=1, strict=True)]
Milliseconds = Annotated[int, Field(strict=True)]


class ArchiveError(Exception):
    """Archive verification failed; customer data must remain at the source."""


class ArchiveLimits(ContractModel):
    chunk_bytes: Annotated[int, Field(ge=256, le=4 * 1024 * 1024)] = 1024 * 1024
    manifest_bytes: Annotated[int, Field(ge=1024, le=16 * 1024 * 1024)] = 4 * 1024 * 1024
    max_streams: Annotated[int, Field(gt=0, le=100_000)] = 10_000
    max_chunks: Annotated[int, Field(gt=0, le=100_000)] = 10_000
    max_pages: Annotated[int, Field(gt=0, le=1_000_000)] = 100_000


class ArchiveLocation(ContractModel):
    run_id: UUID
    github_owner_id: Positive
    org_id: UUID
    environment: Nonempty
    account_id: AccountId
    region: Nonempty
    bucket: Nonempty


class FrozenLogScope(ContractModel):
    """Caller attests current operation-owned holds, terminal state and positive drain.

    This provider cannot establish database holds. Evidence must bind both local
    scopes and the parent plan. The transfer operator rechecks it before cleanup.
    """

    source_identity: OperationIdentity
    destination_identity: OperationIdentity
    source: RunScope
    destination: RunScope
    freeze_evidence_sha256: Digest
    unmasked_read_authorized: Literal[True]

    @model_validator(mode="after")
    def validate_pair(self) -> "FrozenLogScope":
        source = self.source_identity
        destination = self.destination_identity
        local_fields = {"region", "database_target"}
        if source.model_dump(exclude=local_fields) != destination.model_dump(exclude=local_fields):
            raise ValueError("paired operation identity mismatch")

        if source.database_target == destination.database_target:
            raise ValueError("distinct database targets required")

        if self.source.run_id != self.destination.run_id or self.source.run_id not in source.run_ids:
            raise ValueError("run scope mismatch")

        if source.region != self.source.original_resources.region:
            raise ValueError("source region mismatch")

        if destination.region != self.destination.original_resources.region:
            raise ValueError("destination region mismatch")

        return self

    @property
    def location(self) -> ArchiveLocation:
        return ArchiveLocation(
            run_id=self.destination.run_id,
            github_owner_id=self.destination_identity.github_owner_id,
            org_id=self.destination_identity.org_id,
            environment=self.destination_identity.environment,
            account_id=self.destination_identity.destination_aws_account_id,
            region=self.destination.original_resources.region,
            bucket=self.destination.original_resources.s3_bucket,
        )

    @property
    def prefix(self) -> str:
        return f"benchmarks/{self.source.run_id}/log-history/{self.source_identity.operation_id}/v1/"


class ArchivedLogEvent(ContractModel):
    ordinal: Nonnegative
    timestamp: Annotated[int, Field(strict=True)]
    ingestion_time: Annotated[int, Field(strict=True)]
    message: Annotated[str, Field(strict=True)]
    event_id: Nonempty
    stream_name: Nonempty


class ArchiveChunk(ContractModel):
    format_version: Literal[1] = 1
    run_id: UUID
    operation_id: UUID
    first_ordinal: Nonnegative
    events: tuple[ArchivedLogEvent, ...]


class ChunkReference(ContractModel):
    object: ArchiveObject
    first_ordinal: Nonnegative
    event_count: Positive


class ScanEvidence(ContractModel):
    group_absent: bool
    group_pages: Positive
    stream_pages: Nonnegative
    event_pages: Nonnegative
    stream_count: Nonnegative
    event_count: Nonnegative
    stream_sha256: Digest
    event_sha256: Digest
    newest_event_ms: Milliseconds | None = None
    newest_ingestion_ms: Milliseconds | None = None


class LogHistoryManifest(ContractModel):
    format_version: Literal[1] = 1
    run_id: UUID
    operation_id: UUID
    parent_plan_sha256: Digest
    scope_sha256: Digest
    source_account_id: AccountId
    source_region: Nonempty
    source_group: Nonempty
    source_principal_arn: Nonempty
    destination: ArchiveLocation
    freeze_evidence_sha256: Digest
    unmasked: Literal[True] = True
    encoding: Literal["utf-8-json"] = "utf-8-json"
    limits: ArchiveLimits
    stream_names: tuple[Nonempty, ...]
    chunks: tuple[ChunkReference, ...]
    first_scan: ScanEvidence
    second_scan: ScanEvidence


class ArchiveReport(ContractModel):
    reference: LogHistoryReference
    event_count: Nonnegative
    stream_count: Nonnegative
    chunk_count: Nonnegative
    event_sha256: Digest
