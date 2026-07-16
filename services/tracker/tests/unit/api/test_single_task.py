"""Behavior tests for the single-task API."""

from datetime import datetime, timedelta
from unittest.mock import ANY, AsyncMock, Mock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.single_task as single_task_module
from main import app
from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)

client = TestClient(app)


def test_single_task_returns_latest_terminal_result_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Task detail must expose the latest terminal result without leaking another organization.

    Test cases:
    - Finished, error, and pending tasks return status-appropriate result fields.
    - The newest result row wins when a task has retry history.
    - A benchmark from another organization returns 404.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()

    finished_task = Task(
        org_id=TEST_ORG_ID,
        benchmark=benchmark.id,
        task_id="finished-task",
        status=TaskStatus.FINISHED,
        finished_at=now,
    )
    error_task = Task(
        org_id=TEST_ORG_ID,
        benchmark=benchmark.id,
        task_id="error-task",
        status=TaskStatus.ERROR,
        finished_at=now,
    )
    pending_task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="pending-task")
    database_session.add_all([finished_task, error_task, pending_task])
    database_session.flush()
    database_session.add_all(
        [
            EvaluationResult(
                org_id=TEST_ORG_ID,
                task=finished_task.id,
                instance_id="old-attempt",
                result={"score": 0.0},
                created_at=now - timedelta(minutes=1),
            ),
            EvaluationResult(
                org_id=TEST_ORG_ID,
                task=finished_task.id,
                instance_id="new-attempt",
                result={"score": 1.0},
                agent_caused_exit_reason=AgentCausedExitReason.TIMEOUT,
                created_at=now,
            ),
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=error_task.id,
                error_message="old failure",
                created_at=now - timedelta(minutes=1),
            ),
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=error_task.id,
                error_message="latest failure",
                created_at=now,
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

    finished = client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}")
    error = client.get(f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}")
    pending = client.get(f"/benchmarks/{benchmark.id}/tasks/{pending_task.task_id}")
    other_org_response = client.get(f"/benchmarks/{other_benchmark.id}/tasks/unknown")

    assert finished.status_code == 200
    assert finished.json()["evaluation_result"] == {"score": 1.0}
    assert finished.json()["agent_caused_exit_reason"] == "TIMEOUT"
    assert finished.json()["error_message"] is None
    assert error.status_code == 200
    assert error.json()["error_message"] == "latest failure"
    assert error.json()["evaluation_result"] is None
    assert pending.status_code == 200
    assert pending.json()["error_message"] is None
    assert pending.json()["evaluation_result"] is None
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
    task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="task-with-output")
    database_session.add_all([benchmark, task])
    database_session.commit()

    object_exists = AsyncMock(return_value=True)
    create_presigned_url = AsyncMock(return_value="https://example.test/presigned")
    get_log_url = Mock(return_value="https://example.test/cloudwatch")
    monkeypatch.setattr(single_task_module, "s3_object_exists", object_exists)
    monkeypatch.setattr(single_task_module, "create_presigned_url", create_presigned_url)
    monkeypatch.setattr(single_task_module, "get_benchmark_log_url", get_log_url)

    found = client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")
    object_exists.return_value = False
    missing = client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts")

    expected_key = f"benchmarks/{benchmark.id}/{task.task_id}/agent_output.tar.gz"
    assert found.status_code == 200
    assert found.json() == {
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
    assert missing.status_code == 200
    assert missing.json()["agent_output_url"] is None
    assert missing.json()["agent_output_expires_in"] is None
    assert create_presigned_url.await_count == 1
