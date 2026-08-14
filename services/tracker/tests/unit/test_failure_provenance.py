"""Unit tests for direct failure provenance validation and persistence.

Run: uv run pytest tests/unit/test_failure_provenance.py
"""

from unittest.mock import Mock
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlmodel import Session

from tracker.database.models import FailureCategory, FailureClassificationState, FailureTerminalEffect
from tracker.failure_provenance import FailureEvidence, record_failure


def _evidence(**overrides: object) -> FailureEvidence:
    values: dict[str, object] = {
        "category": FailureCategory.HARNESS,
        "producer": "benchmark_service",
        "operation": "setup_task",
        "error_type": "BenchmarkServiceError",
        "error_message": "message text is not a classifier",
        "classification_state": FailureClassificationState.UNCLASSIFIED,
    }
    values.update(overrides)
    return FailureEvidence(**values)


@pytest.mark.parametrize(
    ("classification_state", "cause_code"),
    [
        (FailureClassificationState.CLASSIFIED, "sandbox_setup_failed"),
        (FailureClassificationState.UNCLASSIFIED, None),
        (FailureClassificationState.DETAILS_UNAVAILABLE, None),
    ],
)
def test_failure_evidence_accepts_exact_direct_states(
    classification_state: FailureClassificationState,
    cause_code: str | None,
) -> None:
    evidence = _evidence(classification_state=classification_state, cause_code=cause_code)

    assert evidence.classification_state is classification_state
    assert evidence.cause_code == cause_code


def test_failure_evidence_rejects_classified_without_cause_code() -> None:
    with pytest.raises(ValidationError, match="direct cause code"):
        _evidence(classification_state=FailureClassificationState.CLASSIFIED)


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


def test_record_failure_preserves_recovered_effect_and_retry_sequence() -> None:
    session = Mock(spec=Session)
    failure = record_failure(
        session,
        org_id=uuid4(),
        benchmark_id=uuid4(),
        evidence=_evidence(),
        terminal_effect=FailureTerminalEffect.RECOVERED,
        retry_sequence=2,
    )

    assert failure.terminal_effect is FailureTerminalEffect.RECOVERED
    assert failure.retry_sequence == 2
    session.add.assert_called_once_with(failure)


def test_failure_evidence_uses_explicit_state_not_message_text() -> None:
    unclassified = _evidence(error_message="classified: sandbox_setup_failed")
    classified = _evidence(
        error_message="unrelated text",
        classification_state=FailureClassificationState.CLASSIFIED,
        cause_code="sandbox_setup_failed",
    )

    assert unclassified.classification_state is FailureClassificationState.UNCLASSIFIED
    assert unclassified.cause_code is None
    assert classified.classification_state is FailureClassificationState.CLASSIFIED
    assert classified.cause_code == "sandbox_setup_failed"
