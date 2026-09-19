"""Minimal immutable scope and durable purge phase evidence."""

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from executor_protocol import ExecutorDispatchStatus
from tracker.lifecycle import ContractModel, Digest, OperationIdentity, RunScope, SafeIdentity
from tracker.lifecycle_evidence import DispatchDrain, ExternalHostDrain, LifecycleReport


class ProviderLocator(ContractModel):
    kind: SafeIdentity
    secret_name: Annotated[str, Field(min_length=1, max_length=2048)]


class ReleasedRelocation(ContractModel):
    operation_id: UUID
    identity_sha256: Digest
    scope_sha256: Digest
    acquired_at: AwareDatetime
    released_at: AwareDatetime


class AbandonedDeletion(ContractModel):
    operation_id: UUID
    identity_sha256: Digest
    scope_sha256: Digest
    acquired_at: AwareDatetime
    released_at: AwareDatetime


class PurgeRun(ContractModel):
    scope: RunScope
    provider: ProviderLocator
    released_relocation: ReleasedRelocation | None = None
    abandoned_deletion: AbandonedDeletion | None = None


class PurgePlan(ContractModel):
    identity: OperationIdentity
    runs: tuple[PurgeRun, ...]

    @model_validator(mode="after")
    def validate_scope(self) -> "PurgePlan":
        if tuple(run.scope.run_id for run in self.runs) != self.identity.run_ids:
            raise ValueError("Plan run scope does not match identity")
        if self.identity.source_aws_account_id != self.identity.destination_aws_account_id:
            raise ValueError("Deletion must use one source account")
        if any(run.scope.original_resources.region != self.identity.region for run in self.runs):
            raise ValueError("Plan regions do not match identity")
        if any(
            run.released_relocation is not None and run.released_relocation.operation_id == self.identity.operation_id
            for run in self.runs
        ):
            raise ValueError("Deletion requires a new operation after relocation")
        if any(
            run.abandoned_deletion is not None
            and (
                run.released_relocation is not None or run.abandoned_deletion.operation_id == self.identity.operation_id
            )
            for run in self.runs
        ):
            raise ValueError("A run has one predecessor and deletion requires a new operation")
        return self

    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json", exclude_none=True), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class DispatchSnapshot(ContractModel):
    dispatch_id: UUID
    status: ExecutorDispatchStatus
    started_at: datetime | None
    process_exited_at: datetime | None


class RowScope(ContractModel):
    table: Literal[
        "benchmark", "task", "evaluationresult", "errorresult", "finalevaluation", "executordispatch", "taskbreakdown"
    ]
    ids: tuple[UUID, ...]


class PurgeCheckpoint(ContractModel):
    schema_version: Literal[1] = 1
    child_plan_sha256: Digest
    provider: ProviderLocator
    released_relocation: ReleasedRelocation | None = None
    original_dispatches: tuple[DispatchSnapshot, ...]
    dispatch_drain: tuple[DispatchDrain, ...] = ()
    external_host_drain: ExternalHostDrain | None = None
    rows: tuple[RowScope, ...] = ()
    fence_policy_sha256: Digest | None = None
    phase: Literal["held", "prepared", "objects_removed", "logs_removed", "rows_removed", "complete"] = "held"

    @model_validator(mode="after")
    def validate_proof(self) -> "PurgeCheckpoint":
        dispatch_ids = tuple(item.dispatch_id for item in self.original_dispatches)
        if dispatch_ids != tuple(sorted(set(dispatch_ids), key=str)):
            raise ValueError("Original dispatch scope must be sorted and unique")

        if self.phase != "held":
            if tuple(item.dispatch_id for item in self.dispatch_drain) != dispatch_ids:
                raise ValueError("Drain proof must match every original dispatch")
            for original, drain in zip(self.original_dispatches, self.dispatch_drain, strict=True):
                if drain.provenance == "pending":
                    raise ValueError("Prepared checkpoint cannot contain pending drain")
                if drain.provenance == "host_process_exit" and drain.observed_exit_at is None:
                    raise ValueError("Positive exit timestamp is missing")
                if drain.provenance == "held_unclaimed" and (
                    original.started_at is not None or original.status == "RUNNING"
                ):
                    raise ValueError("Started dispatch cannot use unclaimed proof")
                if drain.provenance == "verified_finished_contract" and (
                    original.started_at is None or original.status != "FINISHED"
                ):
                    raise ValueError("Original dispatch lacks normal finished proof")
                if drain.provenance == "externally_confirmed_host_drain" and (
                    self.external_host_drain is None or drain.dispatch_id not in self.external_host_drain.dispatch_ids
                ):
                    raise ValueError("External drain proof is missing")

        if self.rows or self.phase in {"objects_removed", "logs_removed", "rows_removed", "complete"}:
            tables = tuple(row.table for row in self.rows)
            if len(tables) != 7 or set(tables) != {
                "benchmark",
                "task",
                "evaluationresult",
                "errorresult",
                "finalevaluation",
                "executordispatch",
                "taskbreakdown",
            }:
                raise ValueError("Complete row scope is missing")
            for row in self.rows:
                if row.ids != tuple(sorted(set(row.ids), key=str)):
                    raise ValueError("Row identities must be sorted and unique")
            if self.fence_policy_sha256 is None:
                raise ValueError("Row deletion requires a verified fence identity")
        return self


class PurgeReport(LifecycleReport):
    child_plan_sha256: Digest
    outcome: Literal["checked", "incomplete"]
