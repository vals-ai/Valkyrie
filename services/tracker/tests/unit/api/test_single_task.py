"""Run with `uv run pytest tests/unit/api/test_single_task.py`.

Cover task details and artifact-link behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import ANY, AsyncMock, Mock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.single_task as single_task_module
from main import app
from tests.factories import make_evaluation_result, make_task
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    FailureRecord,
    Org,
    TaskAttempt,
    TaskAttemptOutcome,
    TaskStatus,
)

_client = TestClient(app)


def test_single_task_returns_current_failure_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Task detail must expose the active attempt's current failure without leaking another organization.

    Test cases:
    - Finished, error, and pending tasks return status-appropriate result fields.
    - A scheduled retry remains in history but cannot replace the current failure.
    - A newer failure from a stale attempt cannot replace the active attempt's failure.
    - A benchmark from another organization returns 404.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()

    finished_task = make_task(
        benchmark,
        "finished-task",
        status=TaskStatus.FINISHED,
        finished_at=now,
    )
    error_task = make_task(
        benchmark,
        "error-task",
        status=TaskStatus.ERROR,
        finished_at=now,
    )
    pending_task = make_task(benchmark, "pending-task")
    database_session.add_all([finished_task, error_task, pending_task])
    database_session.flush()

    previous_attempt = TaskAttempt(
        org_id=error_task.org_id,
        task=error_task.id,
        started_at=now - timedelta(minutes=4),
        finished_at=now - timedelta(minutes=3),
        outcome=TaskAttemptOutcome.ERROR,
    )
    database_session.add(previous_attempt)
    database_session.flush()
    active_attempt = TaskAttempt(
        org_id=error_task.org_id,
        task=error_task.id,
        started_at=now - timedelta(minutes=2),
        finished_at=now,
        outcome=TaskAttemptOutcome.ERROR,
    )
    database_session.add(active_attempt)
    database_session.flush()
    error_task.active_attempt_id = active_attempt.id
    database_session.add(error_task)

    database_session.add_all(
        [
            make_evaluation_result(
                finished_task,
                "old-attempt",
                {"score": 0.0},
                now - timedelta(minutes=1),
            ),
            make_evaluation_result(
                finished_task,
                "new-attempt",
                {"score": 1.0},
                now,
                exit_reason=AgentCausedExitReason.TIMEOUT,
            ),
            FailureRecord(
                org_id=error_task.org_id,
                benchmark_id=benchmark.id,
                task=error_task.id,
                task_attempt_id=active_attempt.id,
                producer="sandbox_provider",
                operation="setup",
                error_type="SandboxSetupError",
                message="automatic retry was scheduled",
                retry_scheduled=True,
                occurred_at=now - timedelta(seconds=90),
            ),
            FailureRecord(
                org_id=error_task.org_id,
                benchmark_id=benchmark.id,
                task=error_task.id,
                task_attempt_id=active_attempt.id,
                producer="tracker",
                operation="process_task",
                error_type="RuntimeError",
                message="active attempt failure",
                cause_code="terminal_failure",
                retry_scheduled=False,
                safe_details={"http_status": 500},
                occurred_at=now - timedelta(minutes=1),
            ),
            FailureRecord(
                org_id=error_task.org_id,
                benchmark_id=benchmark.id,
                task=error_task.id,
                task_attempt_id=previous_attempt.id,
                message="newer stale failure",
                retry_scheduled=False,
                occurred_at=now,
            ),
        ]
    )

    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(
        org_id=other_org.id,
        name=benchmark.name,
        arguments=benchmark.arguments,
    )
    database_session.add_all([other_org, other_benchmark])
    database_session.commit()

    finished_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}")
    error_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}",
        params={"failure_history_limit": 3},
    )
    pending_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{pending_task.task_id}")
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}/tasks/unknown")

    assert finished_response.status_code == 200
    assert finished_response.json()["evaluation_result"] == {"score": 1.0}
    assert finished_response.json()["agent_caused_exit_reason"] == "TIMEOUT"
    assert finished_response.json()["error_message"] is None
    assert error_response.status_code == 200
    error_body = error_response.json()
    assert error_body["task_id"] == error_task.task_id
    assert error_body["error_message"] == "active attempt failure"
    assert error_body["evaluation_result"] is None
    assert error_body["failure"]["task_row_id"] == str(error_task.id)
    assert error_body["failure"]["task_attempt_id"] == str(active_attempt.id)
    assert error_body["failure"]["dispatch_id"] is None
    assert error_body["failure"]["message"] == "active attempt failure"
    assert error_body["failure"]["cause_code"] == "terminal_failure"
    assert error_body["failure"]["retry_scheduled"] is False
    assert error_body["failure"]["safe_details"] == {"http_status": 500}
    assert [failure["message"] for failure in error_body["failure_history"]] == [
        "newer stale failure",
        "active attempt failure",
        "automatic retry was scheduled",
    ]
    assert [failure["retry_scheduled"] for failure in error_body["failure_history"]] == [False, False, True]
    assert error_body["failure_history_truncated"] is False
    assert pending_response.status_code == 200
    assert pending_response.json()["error_message"] is None
    assert pending_response.json()["evaluation_result"] is None
    assert other_org_response.status_code == 404


def test_task_artifacts_only_presign_existing_output(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Artifact detail must return useful links without signing a missing output archive.

    Test cases:
    - Existing output receives a five-minute presigned URL and CloudWatch link.
    - Missing output returns no S3 URL and does not call the signer again.
    """
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task-with-output")
    database_session.add_all([benchmark, task])
    database_session.commit()

    object_exists = AsyncMock(return_value=True)
    create_presigned_url = AsyncMock(return_value="https://example.test/presigned")
    get_log_url = Mock(return_value="https://example.test/cloudwatch")
    monkeypatch.setattr(single_task_module, "s3_object_exists", object_exists)
    monkeypatch.setattr(single_task_module, "create_presigned_url", create_presigned_url)
    monkeypatch.setattr(single_task_module, "get_benchmark_log_url", get_log_url)

    found_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")
    object_exists.return_value = False
    missing_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")

    expected_key = f"benchmarks/{benchmark.id}/{task.task_id}/agent_output.tar.gz"
    assert found_response.status_code == 200
    assert found_response.json() == {
        "cloudwatch_url": "https://example.test/cloudwatch",
        "agent_output_url": "https://example.test/presigned",
        "agent_output_expires_in": 300,
    }
    object_exists.assert_awaited_with(
        expected_key,
        aws=ANY,
        s3_bucket="test-bucket",
    )
    create_presigned_url.assert_awaited_once_with(
        s3_key=expected_key,
        aws=ANY,
        s3_bucket="test-bucket",
        expiration=300,
    )
    assert missing_response.status_code == 200
    assert missing_response.json()["agent_output_url"] is None
    assert missing_response.json()["agent_output_expires_in"] is None
    assert create_presigned_url.await_count == 1
