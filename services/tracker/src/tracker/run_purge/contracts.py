"""Minimal immutable scope and durable purge phase evidence."""

import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from executor_protocol import ExecutorDispatchStatus
from tracker.lifecycle import ContractModel, Digest, OperationIdentity, RunScope, SafeIdentity, UTCDatetime
from tracker.lifecycle_completion import RelocationPredecessor
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
    expected_run_label: str | None = None
    scope: RunScope
    provider: ProviderLocator
    released_relocation: ReleasedRelocation | None = None
    abandoned_deletion: AbandonedDeletion | None = None
    completed_history: RelocationPredecessor | None = Field(default=None, exclude_if=lambda value: value is None)


class PurgePlan(ContractModel):
    identity: OperationIdentity
    runs: Annotated[tuple[PurgeRun, ...], Field(min_length=1, json_schema_extra={"uniqueItems": True})]

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
                run.released_relocation is not None
                or run.completed_history is not None
                or run.abandoned_deletion.operation_id == self.identity.operation_id
            )
            for run in self.runs
        ):
            raise ValueError("A run has one predecessor and deletion requires a new operation")

        for run in self.runs:
            if run.completed_history is not None and (
                run.released_relocation is not None
                or run.completed_history.kind != "completed_history_only"
                or run.completed_history.completion_sha256 is None
                or run.completed_history.operation_id == self.identity.operation_id
            ):
                raise ValueError("Deletion requires one exact completed history predecessor and a new operation")

        return self

    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json", exclude_none=True), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class DispatchSnapshot(ContractModel):
    dispatch_id: UUID
    status: ExecutorDispatchStatus
    started_at: UTCDatetime | None
    process_exited_at: UTCDatetime | None


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
    completed_history: RelocationPredecessor | None = Field(default=None, exclude_if=lambda value: value is None)
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


class InspectionCheckpoint(ContractModel):
    phase: Literal["held", "prepared", "objects_removed", "logs_removed", "rows_removed", "complete"]
    checkpoint_sha256: Digest
    child_plan_sha256: Digest


class InspectionRun(ContractModel):
    scope: RunScope
    provider: ProviderLocator
    expected_run_label: str | None


class PresentUnheldInspection(InspectionRun):
    state: Literal["present_unheld"] = "present_unheld"
    current_label: str | None
    released_relocation: ReleasedRelocation | None


class PresentHistoryHeldInspection(InspectionRun):
    state: Literal["present_history_held"] = "present_history_held"
    current_label: str | None
    completed_history: RelocationPredecessor

    @model_validator(mode="after")
    def completed_proof(self) -> "PresentHistoryHeldInspection":
        if self.completed_history.kind != "completed_history_only" or self.completed_history.completion_sha256 is None:
            raise ValueError("History observation requires exact completed predecessor")

        return self


class PresentHeldInspection(InspectionRun):
    state: Literal["present_held"] = "present_held"
    current_label: str | None
    checkpoint: InspectionCheckpoint


class RemovedInspection(InspectionRun):
    state: Literal["removed"] = "removed"
    checkpoint: InspectionCheckpoint
    fence_policy_sha256: Digest
    absence: Literal["rows_sandboxes_objects_logs"] = "rows_sandboxes_objects_logs"

    @model_validator(mode="after")
    def validate_removed(self) -> "RemovedInspection":
        if self.checkpoint.phase not in {"rows_removed", "complete"}:
            raise ValueError("Removed observation requires a durable row-removal checkpoint")

        return self


PurgeInspectionRun = Annotated[
    PresentUnheldInspection | PresentHistoryHeldInspection | PresentHeldInspection | RemovedInspection,
    Field(discriminator="state"),
]


class PurgeInspection(ContractModel):
    schema_version: Literal[1] = 1
    action: Literal["inspect"] = "inspect"
    request_nonce: UUID
    identity: OperationIdentity
    child_plan_sha256: Digest
    observed_at: AwareDatetime
    runs: tuple[PurgeInspectionRun, ...]

    @model_validator(mode="after")
    def validate_runs(self) -> "PurgeInspection":
        if tuple(run.scope.run_id for run in self.runs) != self.identity.run_ids:
            raise ValueError("Inspection run scope does not match identity")

        for run in self.runs:
            if isinstance(run, (PresentHeldInspection, RemovedInspection)):
                if run.checkpoint.child_plan_sha256 != self.child_plan_sha256:
                    raise ValueError("Inspection checkpoint differs from child plan")

            if isinstance(run, (PresentHeldInspection, PresentUnheldInspection, PresentHistoryHeldInspection)):
                if run.expected_run_label is not None and run.current_label != run.expected_run_label:
                    raise ValueError("Observed label differs from bound label")

        return self
