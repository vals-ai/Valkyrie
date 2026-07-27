"""Run with `uv run pytest tests/unit/api/test_single_task.py`.

Cover task details and artifact-link behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from hashlib import sha256
from urllib.parse import quote
from unittest.mock import ANY, AsyncMock, Mock
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.single_task as single_task_module
from main import app
from tests.factories import make_error_result, make_evaluation_result, make_task
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    Org,
    TaskStatus,
)

_client = TestClient(app)


def test_benchmark_attempts_page_retry_history_across_tasks(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    first_task = make_task(benchmark, "first-task", status=TaskStatus.ERROR)
    second_task = make_task(benchmark, "second-task", status=TaskStatus.FINISHED)
    database_session.add_all([benchmark, first_task, second_task])
    database_session.flush()

    repeated_failure = "identical failure\n" * 500
    newest_evaluation = make_evaluation_result(
        second_task,
        "evaluation-new",
        {"large": "x" * 5_000},
        datetime(2026, 6, 24, 16, tzinfo=ZoneInfo("UTC")),
    )
    repeated_error_new = make_error_result(
        first_task,
        repeated_failure,
        datetime(2026, 6, 24, 15, tzinfo=ZoneInfo("UTC")),
    )
    repeated_error_old = make_error_result(
        second_task,
        repeated_failure,
        datetime(2026, 6, 24, 15, tzinfo=ZoneInfo("UTC")),
    )
    older_evaluation = make_evaluation_result(
        first_task,
        "evaluation-old",
        {"score": 0.25},
        datetime(2026, 6, 24, 14, tzinfo=ZoneInfo("UTC")),
        exit_reason=AgentCausedExitReason.TIMEOUT,
    )
    newest_evaluation.id = UUID("00000000-0000-0000-0000-000000000004")
    repeated_error_new.id = UUID("00000000-0000-0000-0000-000000000003")
    repeated_error_old.id = UUID("00000000-0000-0000-0000-000000000002")
    older_evaluation.id = UUID("00000000-0000-0000-0000-000000000001")
    database_session.add_all(
        [
            newest_evaluation,
            repeated_error_new,
            repeated_error_old,
            older_evaluation,
        ]
    )
    database_session.commit()

    first_response = _client.get(f"/benchmarks/{benchmark.id}/attempts?limit=2")
    second_response = _client.get(f"/benchmarks/{benchmark.id}/attempts?limit=2&offset=2")

    fingerprint = sha256(repeated_failure.encode()).hexdigest()
    assert first_response.status_code == 200, first_response.text
    assert first_response.json() == {
        "attempts": [
            {
                "kind": "evaluation",
                "id": str(newest_evaluation.id),
                "task_id": second_task.task_id,
                "created_at": "2026-06-24T16:00:00+00:00",
                "instance_id": "evaluation-new",
                "agent_caused_exit_reason": None,
            },
            {
                "kind": "error",
                "id": str(repeated_error_new.id),
                "task_id": first_task.task_id,
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": repeated_failure[:4_000],
                "error_message_truncated": True,
                "error_fingerprint": fingerprint,
            },
        ],
        "total_count": 4,
    }
    assert second_response.status_code == 200, second_response.text
    assert second_response.json() == {
        "attempts": [
            {
                "kind": "error",
                "id": str(repeated_error_old.id),
                "task_id": second_task.task_id,
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": repeated_failure[:4_000],
                "error_message_truncated": True,
                "error_fingerprint": fingerprint,
            },
            {
                "kind": "evaluation",
                "id": str(older_evaluation.id),
                "task_id": first_task.task_id,
                "created_at": "2026-06-24T14:00:00+00:00",
                "instance_id": "evaluation-old",
                "agent_caused_exit_reason": "TIMEOUT",
            },
        ],
        "total_count": 4,
    }


def test_benchmark_attempts_enforce_org_scope_and_page_limit(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "target", status=TaskStatus.ERROR)
    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(
        org_id=other_org.id,
        name=benchmark.name,
        arguments=benchmark.arguments,
    )
    database_session.add_all([benchmark, task, other_org, other_benchmark])
    database_session.flush()

    target_error = make_error_result(task, "target failure", datetime.now(ZoneInfo("UTC")))
    foreign_error = make_error_result(task, "foreign failure", datetime.now(ZoneInfo("UTC")))
    foreign_error.org_id = other_org.id
    database_session.add_all([target_error, foreign_error])
    database_session.commit()

    response = _client.get(f"/benchmarks/{benchmark.id}/attempts")
    foreign_response = _client.get(f"/benchmarks/{other_benchmark.id}/attempts")
    oversized_response = _client.get(f"/benchmarks/{benchmark.id}/attempts?limit=101")
    distant_response = _client.get(f"/benchmarks/{benchmark.id}/attempts?offset=10001")

    assert response.status_code == 200
    assert [attempt["error_message"] for attempt in response.json()["attempts"]] == ["target failure"]
    assert foreign_response.status_code == 404
    assert oversized_response.status_code == 422
    assert distant_response.status_code == 422


def test_task_attempts_page_retry_history_newest_first(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(
        benchmark,
        "suite/task",
        status=TaskStatus.ERROR,
        finished_at=datetime(2026, 6, 24, 16, tzinfo=ZoneInfo("UTC")),
    )
    database_session.add_all([benchmark, task])
    database_session.flush()

    newest_evaluation = make_evaluation_result(
        task,
        "evaluation-new",
        {"score": 1.0},
        datetime(2026, 6, 24, 16, tzinfo=ZoneInfo("UTC")),
    )
    repeated_error_new = make_error_result(
        task,
        "identical failure",
        datetime(2026, 6, 24, 15, tzinfo=ZoneInfo("UTC")),
    )
    repeated_error_old = make_error_result(
        task,
        "identical failure",
        datetime(2026, 6, 24, 15, tzinfo=ZoneInfo("UTC")),
    )
    older_evaluation = make_evaluation_result(
        task,
        "evaluation-old",
        {"score": 0.25},
        datetime(2026, 6, 24, 14, tzinfo=ZoneInfo("UTC")),
        exit_reason=AgentCausedExitReason.TIMEOUT,
    )
    newest_evaluation.id = UUID("00000000-0000-0000-0000-000000000004")
    repeated_error_new.id = UUID("00000000-0000-0000-0000-000000000003")
    repeated_error_old.id = UUID("00000000-0000-0000-0000-000000000002")
    older_evaluation.id = UUID("00000000-0000-0000-0000-000000000001")
    database_session.add_all(
        [
            newest_evaluation,
            repeated_error_new,
            repeated_error_old,
            older_evaluation,
        ]
    )
    database_session.commit()

    task_path = quote(task.task_id, safe="")
    first_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task_path}/attempts?limit=2")
    second_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task_path}/attempts?limit=2&offset=2")

    fingerprint = sha256(b"identical failure").hexdigest()
    assert first_response.status_code == 200, first_response.text
    assert first_response.json() == {
        "attempts": [
            {
                "kind": "evaluation",
                "id": str(newest_evaluation.id),
                "created_at": "2026-06-24T16:00:00+00:00",
                "instance_id": "evaluation-new",
                "agent_caused_exit_reason": None,
            },
            {
                "kind": "error",
                "id": str(repeated_error_new.id),
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": "identical failure",
                "error_message_truncated": False,
                "error_fingerprint": fingerprint,
            },
        ],
        "total_count": 4,
    }
    assert second_response.status_code == 200, second_response.text
    assert second_response.json() == {
        "attempts": [
            {
                "kind": "error",
                "id": str(repeated_error_old.id),
                "created_at": "2026-06-24T15:00:00+00:00",
                "error_message": "identical failure",
                "error_message_truncated": False,
                "error_fingerprint": fingerprint,
            },
            {
                "kind": "evaluation",
                "id": str(older_evaluation.id),
                "created_at": "2026-06-24T14:00:00+00:00",
                "instance_id": "evaluation-old",
                "agent_caused_exit_reason": "TIMEOUT",
            },
        ],
        "total_count": 4,
    }


def test_task_attempts_enforce_org_scope_and_page_limit(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "target", status=TaskStatus.ERROR)
    other_org = Org(id=uuid4(), name="other-org")
    other_benchmark = Benchmark(
        org_id=other_org.id,
        name=benchmark.name,
        arguments=benchmark.arguments,
    )
    other_task = make_task(other_benchmark, task.task_id, status=TaskStatus.ERROR)
    database_session.add_all([benchmark, task, other_org, other_benchmark, other_task])
    database_session.flush()

    target_error = make_error_result(task, "target failure", datetime.now(ZoneInfo("UTC")))
    foreign_error = make_error_result(task, "foreign failure", datetime.now(ZoneInfo("UTC")))
    foreign_error.org_id = other_org.id
    database_session.add_all([target_error, foreign_error])
    database_session.commit()

    response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts")
    foreign_response = _client.get(f"/benchmarks/{other_benchmark.id}/tasks/{other_task.task_id}/attempts")
    oversized_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts?limit=101")
    distant_response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts?offset=10001")

    assert response.status_code == 200
    assert [attempt["error_message"] for attempt in response.json()["attempts"]] == ["target failure"]
    assert foreign_response.status_code == 404
    assert oversized_response.status_code == 422
    assert distant_response.status_code == 422


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
        "owner/repo/finished-task",
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
    ambiguous_task = make_task(benchmark, "owner/artifacts")
    database_session.add_all([finished_task, error_task, pending_task, ambiguous_task])
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
            make_error_result(error_task, "latest failure", now),
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
    ambiguous_response = _client.get(
        f"/benchmarks/{benchmark.id}/task",
        params={"task_id": ambiguous_task.task_id},
    )
    other_org_response = _client.get(f"/benchmarks/{other_benchmark.id}/tasks/unknown")

    assert finished_response.status_code == 200
    assert finished_response.json()["task_id"] == "owner/repo/finished-task"
    assert finished_response.json()["evaluation_result"] == {"score": 1.0}
    assert finished_response.json()["agent_caused_exit_reason"] == "TIMEOUT"
    assert finished_response.json()["error_message"] is None
    assert error_response.status_code == 200
    assert error_response.json()["error_message"] == "latest failure"
    assert error_response.json()["evaluation_result"] is None
    assert pending_response.status_code == 200
    assert pending_response.json()["error_message"] is None
    assert pending_response.json()["evaluation_result"] is None
    assert ambiguous_response.status_code == 200
    assert ambiguous_response.json()["task_id"] == "owner/artifacts"
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
    task = make_task(benchmark, "owner/repo/task-with-output")
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
