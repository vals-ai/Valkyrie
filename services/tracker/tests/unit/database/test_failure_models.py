"""Unit tests for task-attempt and failure-history model contracts.

Run: uv run pytest tests/unit/database/test_failure_models.py
"""

from uuid import uuid4

from tracker.database.models import EvaluationResult, FailureRecord, TaskAttempt, TaskAttemptOutcome


def test_factual_failure_and_task_attempt_defaults() -> None:
    assert [member.value for member in TaskAttemptOutcome] == [
        "pending",
        "finished",
        "error",
        "stopped",
    ]

    failure = FailureRecord(
        org_id=uuid4(),
        benchmark_id=uuid4(),
        message="failure",
        retry_scheduled=False,
    )
    assert failure.task is None
    assert failure.task_attempt_id is None
    assert failure.dispatch_id is None
    assert failure.producer is None
    assert failure.operation is None
    assert failure.error_type is None
    assert failure.cause_code is None
    assert failure.safe_details is None

    attempt = TaskAttempt(org_id=uuid4(), task=uuid4())
    assert attempt.outcome is TaskAttemptOutcome.PENDING
    assert attempt.dispatch_id is None


def test_evaluation_result_attempt_is_nullable() -> None:
    result = EvaluationResult(org_id=uuid4(), task=uuid4(), result={})

    assert result.task_attempt_id is None
