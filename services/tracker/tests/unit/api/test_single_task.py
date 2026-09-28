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
from sqlmodel import Session, select

import tracker.api.single_task as single_task_module
from main import app
from tests.factories import make_error_result, make_evaluation_result, make_task
from tracker.aws.s3 import PrefixRestore
from tracker.database.models import (
    AgentCausedExitReason,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskBreakdown,
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


@pytest.mark.parametrize("task_id", ["task", "task:one"])
def test_run_artifacts_are_scoped_and_storage_errors_are_mapped(
    task_id: str,
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
        "Contents": [
            {"Key": root + f"{task_id}/result.json", "Size": 2},
            {"Key": root + f"{task_id}-other/file", "Size": 1},
        ],
        "NextContinuationToken": "next",
    }
    client.head_object.return_value = {"ContentLength": 2}
    client.generate_presigned_url.return_value = "https://download.test/file"
    monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", lambda _: client)
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts",
        params={"prefix": task_id, "cursor": "previous", "limit": 2},
        headers=harness_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "artifacts": [{"path": f"{task_id}/result.json", "size": 2, "last_modified": None}],
        "next_cursor": "next",
    }
    client.list_objects_v2.assert_awaited_once_with(
        Bucket="test-bucket", Prefix=root + task_id, MaxKeys=2, ContinuationToken="previous"
    )
    response = _client.get(
        f"/benchmarks/{benchmark.id}/artifacts/download-url",
        params={"path": f"{task_id}/result.json"},
        headers=harness_headers,
    )
    assert response.status_code == 200
    assert response.json()["download_url"] == "https://download.test/file"
    client.generate_presigned_url.assert_awaited_once_with(
        "get_object", Params={"Bucket": "test-bucket", "Key": root + f"{task_id}/result.json"}, ExpiresIn=300
    )
    for endpoint, params in (("artifacts", {}), ("artifacts/download-url", {"path": "file"})):
        assert (
            _client.get(f"/benchmarks/{other.id}/{endpoint}", params=params, headers=harness_headers).status_code == 404
        )
    for path in ("../other", "/outside", "task/../file", "a\\b"):
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


def test_task_results_list_history_newest_first_and_mark_current(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """History must list every evaluation attempt, flagging only a finished task's newest row as current."""
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    finished_task = make_task(benchmark, "finished-task", status=TaskStatus.FINISHED, finished_at=now)
    error_task = make_task(benchmark, "error-task", status=TaskStatus.ERROR, finished_at=now)
    breakdown = TaskBreakdown(agent_run_duration=12.0)
    database_session.add(breakdown)
    database_session.flush()
    breakdown_id = breakdown.id
    finished_task.task_breakdown = breakdown_id
    database_session.add_all([finished_task, error_task])
    database_session.flush()
    old = make_evaluation_result(finished_task, "old", {"score": 0.0}, now - timedelta(minutes=1))
    new = make_evaluation_result(finished_task, "new", {"score": 1.0}, now)
    errored_old = make_evaluation_result(error_task, "errored-old", {"score": 0.5}, now - timedelta(minutes=1))
    database_session.add_all([old, new, errored_old, make_error_result(error_task, "boom", now)])
    database_session.commit()

    finished = _client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/results").json()
    errored = _client.get(f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}/results").json()

    assert [(entry["id"], entry["current"]) for entry in finished["results"]] == [
        (str(new.id), True),
        (str(old.id), False),
    ]
    assert finished["results"][1]["result"] == {"score": 0.0}
    assert [(entry["id"], entry["current"]) for entry in errored["results"]] == [(str(errored_old.id), False)]
    assert errored["status"] == "ERROR"


def test_rollback_task_restores_previous_evaluation_and_artifacts(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
) -> None:
    """Rolling back must make the chosen attempt current, revert artifacts to that time, and drop the final score.

    Test cases:
    - Default target is the attempt before the current one; task detail then shows its result.
    - Artifacts are restored for the task prefix using the target attempt's timestamp.
    - The stale FinalEvaluation row and the published final view object are deleted.
    - The task's timing breakdown (which describes the rerun, not the restored attempt) is dropped.
    - An errored task rolls back to its last good evaluation and becomes FINISHED.
    - Explicit result_id from another task is 404; the current result is 409.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    benchmark.status = BenchmarkStatus.FINISHED
    benchmark.finished_at = now
    database_session.add(benchmark)
    database_session.flush()
    database_session.add(FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=1.0))
    finished_task = make_task(benchmark, "finished-task", status=TaskStatus.FINISHED, finished_at=now)
    error_task = make_task(benchmark, "error-task", status=TaskStatus.ERROR, finished_at=now)
    breakdown = TaskBreakdown(agent_run_duration=12.0)
    database_session.add(breakdown)
    database_session.flush()
    breakdown_id = breakdown.id
    finished_task.task_breakdown = breakdown_id
    database_session.add_all([finished_task, error_task])
    database_session.flush()
    old = make_evaluation_result(
        finished_task, "old", {"score": 0.0}, now - timedelta(minutes=5), exit_reason=AgentCausedExitReason.TIMEOUT
    )
    new = make_evaluation_result(finished_task, "new", {"score": 1.0}, now)
    errored_old = make_evaluation_result(error_task, "errored-old", {"score": 0.5}, now - timedelta(minutes=5))
    database_session.add_all([old, new, errored_old, make_error_result(error_task, "boom", now)])
    database_session.commit()

    restore = AsyncMock(return_value=PrefixRestore(restored=["agent_output.tar.gz"], removed=["extra.txt"]))
    monkeypatch.setattr(single_task_module, "restore_prefix_versions_before", restore)
    delete_final_view = AsyncMock()
    monkeypatch.setattr(single_task_module, "delete_from_s3", delete_final_view)

    response = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/rollback", headers=harness_headers, json={}
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["restored_from_result_id"] == str(old.id)
    assert body["artifacts_versioned"] is True
    assert body["restored_artifacts"] == ["agent_output.tar.gz"]
    assert body["removed_artifacts"] == ["extra.txt"]
    restore.assert_awaited_once()
    assert restore.await_args is not None
    assert restore.await_args.args[0] == f"benchmarks/{benchmark.id}/{finished_task.task_id}/"
    assert restore.await_args.args[1] == old.created_at
    assert delete_final_view.await_args is not None
    assert delete_final_view.await_args.args[0] == f"benchmarks/{benchmark.id}/{benchmark.name}.json"

    detail = _client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}").json()
    assert detail["evaluation_result"] == {"score": 0.0}
    assert detail["agent_caused_exit_reason"] == "TIMEOUT"
    history = _client.get(f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/results").json()
    assert [entry["id"] for entry in history["results"]] == [body["result_id"], str(new.id), str(old.id)]
    database_session.expire_all()
    assert database_session.exec(select(FinalEvaluation)).first() is None
    rolled_back = database_session.get(Task, finished_task.id)
    assert rolled_back is not None and rolled_back.task_breakdown is None
    assert database_session.get(TaskBreakdown, breakdown_id) is None

    errored = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}/rollback", headers=harness_headers, json={}
    )
    assert errored.status_code == 200, errored.text
    assert errored.json()["restored_from_result_id"] == str(errored_old.id)
    assert errored.json()["status"] == "FINISHED"
    assert _client.get(f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}").json()["evaluation_result"] == {
        "score": 0.5
    }

    wrong_task = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/rollback",
        headers=harness_headers,
        json={"result_id": str(errored_old.id)},
    )
    assert wrong_task.status_code == 404
    already_current = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/rollback",
        headers=harness_headers,
        json={"result_id": body["result_id"]},
    )
    assert already_current.status_code == 409
    assert restore.await_count == 2


def test_rollback_task_rejects_active_runs_unsettled_tasks_and_missing_history(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
) -> None:
    """Rollback must not touch state while a run is active, for unsettled tasks, or with nothing to restore."""
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    database_session.add(benchmark)
    database_session.flush()
    finished_task = make_task(benchmark, "finished-task", status=TaskStatus.FINISHED, finished_at=now)
    pending_task = make_task(benchmark, "pending-task")
    database_session.add_all([finished_task, pending_task])
    database_session.flush()
    database_session.add(make_evaluation_result(finished_task, "only", {"score": 1.0}, now))
    database_session.commit()
    restore = AsyncMock()
    monkeypatch.setattr(single_task_module, "restore_prefix_versions_before", restore)
    monkeypatch.setattr(single_task_module, "delete_from_s3", AsyncMock())

    active = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/rollback", headers=harness_headers, json={}
    )
    assert active.status_code == 409

    benchmark.status = BenchmarkStatus.FINISHED
    benchmark.finished_at = now
    database_session.add(benchmark)
    database_session.commit()

    unsettled = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{pending_task.task_id}/rollback", headers=harness_headers, json={}
    )
    no_history = _client.post(
        f"/benchmarks/{benchmark.id}/tasks/{finished_task.task_id}/rollback", headers=harness_headers, json={}
    )

    assert unsettled.status_code == 409
    assert no_history.status_code == 404
    restore.assert_not_awaited()


def test_rollback_task_excludes_nested_sibling_tasks_and_detects_concurrent_changes(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
    harness_headers: dict[str, str],
) -> None:
    """Restoring `foo` must not touch `foo/bar`'s artifacts, and a task that changes mid-rollback is not committed.

    Test cases:
    - The S3 restore receives the nested sibling's prefix as an exclusion.
    - If a new evaluation lands while S3 is being restored, the rollback is refused with 409 and nothing is written.
    """
    now = datetime.now(ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    benchmark.status = BenchmarkStatus.FINISHED
    benchmark.finished_at = now
    database_session.add(benchmark)
    database_session.flush()
    parent = make_task(benchmark, "foo", status=TaskStatus.FINISHED, finished_at=now)
    nested = make_task(benchmark, "foo/bar", status=TaskStatus.FINISHED, finished_at=now)
    unrelated = make_task(benchmark, "foobar", status=TaskStatus.FINISHED, finished_at=now)
    database_session.add_all([parent, nested, unrelated])
    database_session.flush()
    database_session.add_all(
        [
            make_evaluation_result(parent, "p-old", {"score": 0.0}, now - timedelta(minutes=5)),
            make_evaluation_result(parent, "p-new", {"score": 1.0}, now),
        ]
    )
    database_session.commit()
    restore = AsyncMock(return_value=PrefixRestore(restored=[], removed=[]))
    monkeypatch.setattr(single_task_module, "restore_prefix_versions_before", restore)
    monkeypatch.setattr(single_task_module, "delete_from_s3", AsyncMock())

    response = _client.post(f"/benchmarks/{benchmark.id}/tasks/foo/rollback", headers=harness_headers, json={})

    assert response.status_code == 200, response.text
    assert restore.await_args is not None
    assert restore.await_args.kwargs["exclude_prefixes"] == [f"benchmarks/{benchmark.id}/foo/bar/"]
    result_count = len(database_session.exec(select(EvaluationResult).where(EvaluationResult.task == parent.id)).all())
    assert result_count == 3

    async def evaluate_during_restore(*_args: object, **_kwargs: object) -> PrefixRestore:
        database_session.add(make_evaluation_result(parent, "raced", {"score": 0.5}, datetime.now(ZoneInfo("UTC"))))
        database_session.commit()
        return PrefixRestore(restored=[], removed=[])

    monkeypatch.setattr(single_task_module, "restore_prefix_versions_before", evaluate_during_restore)
    raced = _client.post(f"/benchmarks/{benchmark.id}/tasks/foo/rollback", headers=harness_headers, json={})

    assert raced.status_code == 409, raced.text
    database_session.expire_all()
    assert len(database_session.exec(select(EvaluationResult).where(EvaluationResult.task == parent.id)).all()) == 4
