"""Internal provenance contract and canonical persistence for terminal failures."""

from typing import TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlmodel import Session

from tracker.database.models import (
    FailureCategory,
    FailureClassificationState,
    FailureRecord,
    FailureTerminalEffect,
)

SafeDetailValue: TypeAlias = str | int | float | bool | None
_SAFE_DETAIL_KEYS = frozenset({"http_status", "last_message_age_seconds"})


class FailureEvidence(BaseModel):
    """Validated direct evidence for one terminal failure."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1, le=1)
    category: FailureCategory
    producer: str
    operation: str
    error_type: str
    error_message: str
    classification_state: FailureClassificationState
    cause_code: str | None = None
    safe_details: dict[str, SafeDetailValue] | None = None

    @model_validator(mode="after")
    def validate_classification(self) -> "FailureEvidence":
        classified = self.classification_state == FailureClassificationState.CLASSIFIED
        if classified != (self.cause_code is not None):
            raise ValueError("classified failure evidence requires a direct cause code")
        if self.safe_details is not None:
            unsupported = self.safe_details.keys() - _SAFE_DETAIL_KEYS
            if unsupported:
                raise ValueError(f"unsupported safe detail keys: {', '.join(sorted(unsupported))}")
        return self


def record_failure(
    session: Session,
    *,
    org_id: UUID,
    benchmark_id: UUID,
    evidence: FailureEvidence,
    terminal_effect: FailureTerminalEffect,
    task_id: UUID | None = None,
    task_attempt_id: UUID | None = None,
    dispatch_id: UUID | None = None,
    retry_sequence: int | None = None,
) -> FailureRecord:
    """Add one canonical failure row to the caller-owned transaction."""
    failure = FailureRecord(
        schema_version=evidence.schema_version,
        org_id=org_id,
        benchmark_id=benchmark_id,
        task=task_id,
        task_attempt_id=task_attempt_id,
        dispatch_id=dispatch_id,
        retry_sequence=retry_sequence,
        category=evidence.category,
        producer=evidence.producer,
        operation=evidence.operation,
        error_type=evidence.error_type,
        error_message=evidence.error_message,
        classification_state=evidence.classification_state,
        cause_code=evidence.cause_code,
        terminal_effect=terminal_effect,
        safe_details=evidence.safe_details,
    )
    session.add(failure)
    return failure


def record_terminal_failure(
    session: Session,
    *,
    org_id: UUID,
    benchmark_id: UUID,
    evidence: FailureEvidence,
    task_id: UUID | None = None,
    task_attempt_id: UUID | None = None,
    dispatch_id: UUID | None = None,
) -> FailureRecord:
    """Add the canonical terminal failure row to the caller-owned transaction."""
    return record_failure(
        session,
        org_id=org_id,
        benchmark_id=benchmark_id,
        evidence=evidence,
        terminal_effect=FailureTerminalEffect.TERMINAL,
        task_id=task_id,
        task_attempt_id=task_attempt_id,
        dispatch_id=dispatch_id,
    )
