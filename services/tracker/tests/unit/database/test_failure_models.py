"""Unit tests for task-attempt and failure-history model contracts.

Run: uv run pytest tests/unit/database/test_failure_models.py
"""

from uuid import uuid4

from tracker.database.models import (
    EvaluationResult,
    FailureCategory,
    FailureClassificationState,
    FailureRecord,
    FailureTerminalEffect,
    TaskAttempt,
    TaskAttemptAdmissionReason,
    TaskAttemptOutcome,
)


def test_failure_enums_use_lowercase_values_and_model_defaults() -> None:
    assert [member.value for member in FailureCategory] == [
        "valkyrie",
        "daytona",
        "harness",
        "model",
        "model_gateway",
        "unknown",
    ]
    assert [member.value for member in FailureClassificationState] == [
        "classified",
        "unclassified",
        "details_unavailable",
        "legacy_unclassified",
    ]
    assert [member.value for member in FailureTerminalEffect] == ["recovered", "secondary", "terminal"]
    assert [member.value for member in TaskAttemptAdmissionReason] == [
        "initial",
        "manual_retry",
        "resume",
        "rollout_claim",
    ]
    assert [member.value for member in TaskAttemptOutcome] == [
        "pending",
        "finished",
        "error",
        "stopped",
    ]

    failure = FailureRecord(org_id=uuid4(), benchmark_id=uuid4(), error_message="failure")
    assert failure.schema_version == 1
    assert failure.category is FailureCategory.UNKNOWN
    assert failure.classification_state is FailureClassificationState.UNCLASSIFIED
    assert failure.terminal_effect is FailureTerminalEffect.TERMINAL
    assert failure.task is None
    assert failure.task_attempt_id is None
    assert failure.dispatch_id is None

    attempt = TaskAttempt(org_id=uuid4(), task=uuid4())
    assert attempt.admission_reason is TaskAttemptAdmissionReason.INITIAL
    assert attempt.outcome is TaskAttemptOutcome.PENDING
    assert attempt.dispatch_id is None
    assert attempt.previous_attempt_id is None
    assert attempt.reason_failure_id is None


def test_evaluation_result_attempt_is_nullable() -> None:
    result = EvaluationResult(org_id=uuid4(), task=uuid4(), result={})

    assert result.task_attempt_id is None
