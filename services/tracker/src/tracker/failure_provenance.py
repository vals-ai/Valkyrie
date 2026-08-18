"""Internal provenance contract and canonical persistence for failures."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlmodel import Session

from tracker.database.models import FailureRecord


class FailureEvidence(BaseModel):
    """Validated direct evidence for one failure event."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    producer: str
    operation: str
    error_type: str
    message: str
    cause_code: str | None = None


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
    )
    session.add(failure)
    return failure
