"""Internal provenance contract and canonical persistence for failures."""

from typing import TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator
from sqlmodel import Session

from tracker.database.models import FailureRecord

SafeDetailValue: TypeAlias = str | int | float | bool | None
_SAFE_DETAIL_KEYS = frozenset({"http_status", "last_message_age_seconds"})


class FailureEvidence(BaseModel):
    """Validated direct evidence for one failure event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    producer: str
    operation: str
    error_type: str
    message: str
    cause_code: str | None = None
    safe_details: dict[str, SafeDetailValue] | None = None

    @model_validator(mode="after")
    def validate_safe_details(self) -> "FailureEvidence":
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
    retry_scheduled: bool,
    task_id: UUID | None = None,
    task_attempt_id: UUID | None = None,
    dispatch_id: UUID | None = None,
) -> FailureRecord:
    """Add one canonical failure row to the caller-owned transaction."""
    failure = FailureRecord(
        org_id=org_id,
        benchmark_id=benchmark_id,
        task=task_id,
        task_attempt_id=task_attempt_id,
        dispatch_id=dispatch_id,
        producer=evidence.producer,
        operation=evidence.operation,
        error_type=evidence.error_type,
        message=evidence.message,
        cause_code=evidence.cause_code,
        retry_scheduled=retry_scheduled,
        safe_details=evidence.safe_details,
    )
    session.add(failure)
    return failure
