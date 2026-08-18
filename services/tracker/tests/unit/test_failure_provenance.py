"""Unit tests for direct failure provenance validation and persistence.

Run: uv run pytest tests/unit/test_failure_provenance.py
"""

from unittest.mock import Mock
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlmodel import Session

from tracker.failure_provenance import FailureEvidence, record_failure


def _evidence(**overrides: object) -> FailureEvidence:
    values: dict[str, object] = {
        "producer": "benchmark_service",
        "operation": "setup_task",
        "error_type": "BenchmarkServiceError",
        "message": "benchmark service request failed",
    }
    values.update(overrides)
    return FailureEvidence(**values)


@pytest.mark.parametrize(
    "safe_details",
    [
        {"not_approved": "value"},
        {"http_status": [500]},
    ],
)
def test_failure_evidence_rejects_unapproved_or_non_scalar_details(safe_details: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _evidence(safe_details=safe_details)


def test_record_failure_preserves_factual_evidence_and_retry_decision() -> None:
    session = Mock(spec=Session)
    org_id = uuid4()
    benchmark_id = uuid4()
    task_id = uuid4()
    task_attempt_id = uuid4()
    dispatch_id = uuid4()
    evidence = _evidence(
        cause_code="sandbox_setup_failed",
        safe_details={"http_status": 503},
    )

    failure = record_failure(
        session,
        org_id=org_id,
        benchmark_id=benchmark_id,
        evidence=evidence,
        retry_scheduled=True,
        task_id=task_id,
        task_attempt_id=task_attempt_id,
        dispatch_id=dispatch_id,
    )

    assert failure.org_id == org_id
    assert failure.benchmark_id == benchmark_id
    assert failure.task == task_id
    assert failure.task_attempt_id == task_attempt_id
    assert failure.dispatch_id == dispatch_id
    assert failure.producer == evidence.producer
    assert failure.operation == evidence.operation
    assert failure.error_type == evidence.error_type
    assert failure.message == evidence.message
    assert failure.cause_code == evidence.cause_code
    assert failure.retry_scheduled is True
    assert failure.safe_details == evidence.safe_details
    session.add.assert_called_once_with(failure)
