"""Run with `uv run pytest tests/unit/api/test_single_task.py`.

Cover task details and artifact-link behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.parse import quote
from unittest.mock import ANY, AsyncMock, Mock, call
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.single_task as single_task_module
from main import app
from tests.factories import make_error_result, make_evaluation_result, make_task
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    Org,
    TaskAttempt,
    TaskStatus,
)
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.aws.cloudwatch_logs import task_log_attempt_id
from tracker.types import ERROR_EXCERPT_MAX_LENGTH
from tracker.utils.harness_config import try_fetch_harness_config

_client = TestClient(app)


def test_task_routes_preserve_slash_ids(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "suite/task-1")
    database_session.add_all([benchmark, task])
    database_session.commit()
    task_path = quote(task.task_id, safe="")

    detail = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task_path}")
    attempts = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task_path}/attempts",
    )

    assert detail.status_code == 200
    assert detail.json()["task_id"] == task.task_id
    assert attempts.status_code == 200
    assert attempts.json() == {"attempts": [], "total_count": 0}


def test_task_attempts_include_execution_without_outcome(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    started_at = datetime(2026, 7, 23, tzinfo=ZoneInfo("UTC"))
    task = make_task(
        benchmark,
        "active-task",
        status=TaskStatus.IN_PROGRESS,
        started_at=started_at,
    )
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        attempt_id=task_log_attempt_id(started_at),
        started_at=started_at,
        sandbox_provider="daytona",
        sandbox_instance_id="sandbox-live",
    )
    database_session.add_all([benchmark, task, attempt])
    database_session.commit()

    response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts",
    )

    assert response.status_code == 200
    assert response.json() == {
        "attempts": [
            {
                "kind": "execution",
                "id": str(attempt.id),
                "attempt_id": attempt.attempt_id,
                "created_at": "2026-07-23T00:00:00+00:00",
                "status": "IN_PROGRESS",
                "instance_id": "sandbox-live",
            }
        ],
        "total_count": 1,
    }


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
            make_error_result(error_task, "old failure", now - timedelta(minutes=1)),
            make_error_result(
                error_task,
                "latest failure" + "!" * ERROR_EXCERPT_MAX_LENGTH,
                now,
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
    error_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}")
    pending_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{pending_task.task_id}")
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}/tasks/unknown")

    assert finished_response.status_code == 200
    assert finished_response.json()["evaluation_result"] == {"score": 1.0}
    assert finished_response.json()["agent_caused_exit_reason"] == "TIMEOUT"
    assert finished_response.json()["error_message"] is None
    assert error_response.status_code == 200
    assert (
        error_response.json()["error_message"]
        == ("latest failure" + "!" * ERROR_EXCERPT_MAX_LENGTH)[:ERROR_EXCERPT_MAX_LENGTH]
    )
    assert error_response.json()["evaluation_result"] is None
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
    object_exists.assert_awaited_with(expected_key, ANY)
    create_presigned_url.assert_awaited_once_with(
        s3_key=expected_key,
        runtime=ANY,
        expiration=300,
    )
    assert missing_response.status_code == 200
    assert missing_response.json()["agent_output_url"] is None
    assert missing_response.json()["agent_output_expires_in"] is None
    assert create_presigned_url.await_count == 1


def test_task_logs_page_exact_retry_streams_and_tail_current_attempt(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    task = make_task(benchmark, "provider:model*fast", status=TaskStatus.IN_PROGRESS, started_at=started_at)
    database_session.add_all([benchmark, task])
    database_session.commit()

    current_attempt_id = task_log_attempt_id(started_at)
    cloudwatch = Mock()
    cloudwatch.describe_log_streams.return_value = {
        "logStreams": [
            {
                "logStreamName": "provider%3Amodel%2Afast_deadbeef",
                "creationTime": 1,
                "lastEventTimestamp": 2,
            },
            {
                "logStreamName": f"provider%3Amodel%2Afast_{current_attempt_id}",
                "creationTime": 3,
                "firstEventTimestamp": 4,
                "lastEventTimestamp": 5,
                "lastIngestionTime": 6,
            },
        ],
        "nextToken": "attempt-page-2",
    }
    cloudwatch.get_log_events.return_value = {
        "events": [{"timestamp": 10, "ingestionTime": 11, "message": "still running"}],
        "nextForwardToken": "events-page-2",
        "nextBackwardToken": "events-page-0",
    }
    monkeypatch.setattr(
        ExplicitCredentialsAWSClientProvider,
        "cloudwatch_logs_client",
        Mock(return_value=cloudwatch),
    )

    attempts_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts",
        params={"limit": 2},
    )
    events_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts/{current_attempt_id}/events",
        params={"direction": "forward", "limit": 100, "cursor": "events-page-1"},
    )

    assert attempts_response.status_code == 200, attempts_response.text
    assert attempts_response.json() == {
        "attempts": [
            {
                "id": current_attempt_id,
                "started_at": "2026-07-22T12:30:00Z",
                "is_current": True,
                "creation_time_ms": 3,
                "first_event_time_ms": 4,
                "last_event_time_ms": 5,
                "last_ingestion_time_ms": 6,
            },
            {
                "id": "deadbeef",
                "started_at": "1970-01-01T01:02:15.928559Z",
                "is_current": False,
                "creation_time_ms": 1,
                "first_event_time_ms": None,
                "last_event_time_ms": 2,
                "last_ingestion_time_ms": None,
            },
        ],
        "current_attempt_id": current_attempt_id,
        "next_cursor": "attempt-page-2",
    }
    assert events_response.status_code == 200, events_response.text
    assert events_response.json() == {
        "attempt_id": current_attempt_id,
        "is_current": True,
        "is_active": True,
        "task_status": "IN_PROGRESS",
        "direction": "forward",
        "events": [{"timestamp_ms": 10, "ingestion_time_ms": 11, "message": "still running"}],
        "older_cursor": "events-page-0",
        "newer_cursor": "events-page-2",
    }
    assert cloudwatch.describe_log_streams.call_args == call(
        logGroupName=f"test-log-group/{benchmark.id}",
        logStreamNamePrefix="provider%3Amodel%2Afast_",
        orderBy="LogStreamName",
        descending=True,
        limit=2,
    )
    assert cloudwatch.get_log_events.call_args == call(
        logGroupName=f"test-log-group/{benchmark.id}",
        logStreamName=f"provider%3Amodel%2Afast_{current_attempt_id}",
        limit=100,
        startFromHead=True,
        nextToken="events-page-1",
    )


def test_task_logs_accept_legacy_harness_headers_and_reject_untrusted_names(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()

    cloudwatch = Mock()
    cloudwatch.describe_log_streams.return_value = {"logStreams": []}
    monkeypatch.setattr(
        ExplicitCredentialsAWSClientProvider,
        "cloudwatch_logs_client",
        Mock(return_value=cloudwatch),
    )
    monkeypatch.delitem(app.dependency_overrides, try_fetch_harness_config)
    headers = {
        "x-harness-aws-access-key-id": "legacy-key",
        "x-harness-aws-secret-access-key": "legacy-secret",
        "x-harness-aws-default-region": "us-east-1",
        "x-harness-s3-bucket": "legacy-bucket",
        "x-harness-log-group": "legacy-log-group",
        "x-harness-log-retention-policy": "30",
        "x-harness-sandbox-provider-secret-name": "legacy-provider-secret",
    }

    legacy_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts",
        headers=headers,
    )
    invalid_attempt_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts/not_a_stream/events",
        headers=headers,
    )
    unexpected_query_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts",
        headers=headers,
        params={"log_group": "caller-controlled"},
    )

    assert legacy_response.status_code == 200, legacy_response.text
    assert legacy_response.json()["attempts"] == []
    assert cloudwatch.describe_log_streams.call_args == call(
        logGroupName=f"legacy-log-group/{benchmark.id}",
        logStreamNamePrefix="task_",
        orderBy="LogStreamName",
        descending=True,
        limit=20,
    )
    assert invalid_attempt_response.status_code == 422
    assert unexpected_query_response.status_code == 422
    cloudwatch.get_log_events.assert_not_called()


def test_current_task_log_attempt_is_empty_until_cloudwatch_stream_exists(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "pending-task", status=TaskStatus.BUILDING)
    database_session.add_all([benchmark, task])
    database_session.commit()

    cloudwatch = Mock()
    cloudwatch.describe_log_streams.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "DescribeLogStreams",
    )
    cloudwatch.get_log_events.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
        "GetLogEvents",
    )
    monkeypatch.setattr(
        ExplicitCredentialsAWSClientProvider,
        "cloudwatch_logs_client",
        Mock(return_value=cloudwatch),
    )
    current_attempt_id = task_log_attempt_id(task.started_at)

    attempts_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts",
    )
    current_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts/{current_attempt_id}/events",
    )
    unknown_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/log-attempts/deadbeef/events",
    )

    assert attempts_response.status_code == 200, attempts_response.text
    assert attempts_response.json() == {
        "attempts": [],
        "current_attempt_id": current_attempt_id,
        "next_cursor": None,
    }
    assert current_response.status_code == 200, current_response.text
    assert current_response.json() == {
        "attempt_id": current_attempt_id,
        "is_current": True,
        "is_active": True,
        "task_status": "BUILDING",
        "direction": "forward",
        "events": [],
        "older_cursor": None,
        "newer_cursor": None,
    }
    assert unknown_response.status_code == 404
