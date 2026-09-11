"""Run with `uv run pytest tests/unit/api/test_single_task.py`.

Cover task details and artifact-link behavior.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import ANY, AsyncMock
from uuid import uuid4
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


def test_single_task_returns_latest_terminal_result_and_enforces_org_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """Task detail must expose the latest terminal result without leaking another organization.

    Test cases:
    - Finished, error, and pending tasks return status-appropriate result fields.
    - The newest terminal result wins when a newer scheduled-retry row exists.
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
            make_error_result(error_task, "latest failure", now),
            make_error_result(
                error_task,
                "scheduled retry",
                now + timedelta(minutes=1),
                producer="sandbox_provider",
                operation="setup",
                error_type="SandboxSetupError",
                retry_scheduled=True,
                failed_attempt_number=1,
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
    assert error_response.json()["error_message"] == "latest failure"
    assert error_response.json()["evaluation_result"] is None
    assert pending_response.status_code == 200
    assert pending_response.json()["error_message"] is None
    assert pending_response.json()["evaluation_result"] is None
    assert other_org_response.status_code == 404


@pytest.mark.parametrize("task_id", ["task-with-output", "task:with-output", "task*with-output", "task%with-output"])
def test_task_artifacts_only_presign_existing_output(
    task_id: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
) -> None:
    """Artifact detail must return useful links without signing a missing output archive.

    Test cases:
    - Existing output receives a five-minute presigned URL and CloudWatch link.
    - Missing output returns no S3 URL and does not call the signer again.
    - Renamed task streams link to the run instead of guessing the historical encoding.
    """
    benchmark = example_benchmark_object
    task = make_task(benchmark, task_id)
    database_session.add_all([benchmark, task])
    database_session.commit()

    object_exists = AsyncMock(return_value=True)
    create_presigned_url = AsyncMock(return_value="https://example.test/presigned")
    monkeypatch.setattr(single_task_module, "s3_object_exists", object_exists)
    monkeypatch.setattr(single_task_module, "create_presigned_url", create_presigned_url)

    found_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts",
        headers=harness_headers,
    )
    object_exists.return_value = False
    missing_response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts",
        headers=harness_headers,
    )

    expected_key = f"benchmarks/{benchmark.id}/{task.task_id}/agent_output.tar.gz"
    assert found_response.status_code == 200
    assert found_response.json()["agent_output_url"] == "https://example.test/presigned"
    assert found_response.json()["agent_output_expires_in"] == 300
    cloudwatch_url = found_response.json()["cloudwatch_url"]
    assert "logsV2:log-groups/log-group/" in cloudwatch_url
    assert str(benchmark.id) in cloudwatch_url
    assert ("/log-events/" in cloudwatch_url) is (task_id == "task-with-output")
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


def test_run_artifacts_are_scoped_and_storage_errors_are_mapped(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
) -> None:
    from botocore.exceptions import ClientError
    from tracker.aws.clients import ExplicitCredentialsAWSClientProvider

    benchmark = example_benchmark_object
    other_org = Org(id=uuid4(), name="other-artifacts")
    other = Benchmark(org_id=other_org.id, name="other", arguments=benchmark.arguments)
    database_session.add_all([benchmark, other_org, other])
    database_session.commit()
    root = f"benchmarks/{benchmark.id}/"
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.list_objects_v2.return_value = {
        "Contents": [{"Key": root + "task/result.json", "Size": 2}, {"Key": root + "task-other/file", "Size": 1}],
        "NextContinuationToken": "next",
    }
    client.head_object.return_value = {"ContentLength": 2}
    client.generate_presigned_url.return_value = "https://download.test/file"
    monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", lambda _: client)
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts",
        params={"prefix": "task", "cursor": "previous", "limit": 2},
        headers=harness_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "artifacts": [{"path": "task/result.json", "size": 2, "last_modified": None}],
        "next_cursor": "next",
    }
    client.list_objects_v2.assert_awaited_once_with(
        Bucket="test-bucket", Prefix=root + "task", MaxKeys=2, ContinuationToken="previous"
    )
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts/download-url",
        params={"path": "task/result.json"},
        headers=harness_headers,
    )
    assert response.status_code == 200
    assert response.json()["download_url"] == "https://download.test/file"
    client.generate_presigned_url.assert_awaited_once_with(
        "get_object", Params={"Bucket": "test-bucket", "Key": root + "task/result.json"}, ExpiresIn=300
    )
    for endpoint, params in (("artifacts", {}), ("artifacts/download-url", {"path": "file"})):
        assert (
            _client.get(f"/benchmarks/{other.id}/{endpoint}", params=params, headers=harness_headers).status_code == 404
        )
    for path in ("../other", "/outside", "task/../file", "a\\b", "a:b"):
        assert (
            _client.get(
                f"/benchmarks/{benchmark.id}/artifacts/download-url", params={"path": path}, headers=harness_headers
            ).status_code
            == 400
        )
    for code, status in (("NoSuchKey", 404), ("AccessDenied", 403), ("InternalError", 502)):
        client.head_object.side_effect = ClientError({"Error": {"Code": code}}, "HeadObject")
        assert (
            _client.get(
                f"/benchmarks/{benchmark.id}/artifacts/download-url", params={"path": "file"}, headers=harness_headers
            ).status_code
            == status
        )
