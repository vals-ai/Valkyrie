"""Exercise saved history through real route dependency and AWS composition."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select

from main import app
from tests.factories import make_task
from tests.unit.aws.test_log_history_archive import (
    DESTINATION_ACCOUNT,
    RUN_ID,
    SOURCE_ACCOUNT,
    FakeLogs,
    FakeS3,
    FakeSession,
    scoped_input,
)
from tracker.aws import log_history_archive
from tracker.aws.clients import AWSClientProvider
from tracker.aws.cloudwatch_logs import task_log_stream_name
from tracker.database.models import Benchmark, BenchmarkStatus, Org


class DestinationLogs:
    def __init__(self) -> None:
        self.stream_name = "retry-stream"

    def filter_log_events(self, **request: Any) -> dict[str, Any]:
        if "logStreamNames" in request and self.stream_name not in request["logStreamNames"]:
            return {"events": []}

        return {"events": [{"timestamp": 2, "message": "retry", "eventId": "live", "logStreamName": self.stream_name}]}

    def get_log_events(self, **request: Any) -> dict[str, Any]:
        return {"events": [], "nextForwardToken": "end"}


def install_history(
    benchmark: Benchmark,
    database_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_logs: DestinationLogs | None = None,
) -> FakeS3:
    benchmark.id = RUN_ID
    scope = scoped_input(log_history_archive)
    scope = scope.model_copy(
        update={
            "source_identity": scope.source_identity.model_copy(update={"org_id": benchmark.org_id}),
            "destination_identity": scope.destination_identity.model_copy(update={"org_id": benchmark.org_id}),
        }
    )
    storage = FakeS3()
    tags = storage.get_bucket_tagging()["TagSet"]
    tags[2]["Value"] = str(benchmark.org_id)

    def bucket_tags(**kwargs: Any) -> dict[str, Any]:
        return {"TagSet": tags}

    monkeypatch.setattr(storage, "get_bucket_tagging", bucket_tags)
    report = log_history_archive.archive_logs(
        scope,
        source_session=FakeSession(SOURCE_ACCOUNT, FakeLogs()),
        destination_session=FakeSession(DESTINATION_ACCOUNT, storage),
        journal_directory=tmp_path,
    )
    benchmark.log_history = report.reference
    benchmark.arguments = benchmark.arguments.model_copy(update={"properties": scope.destination.original_resources})
    benchmark.status = BenchmarkStatus.FINISHED
    benchmark.finished_at = datetime.now(UTC)
    database_session.add(benchmark)
    database_session.commit()
    database_session.expire_all()
    saved = database_session.get(Benchmark, RUN_ID)
    assert saved is not None and saved.log_history == report.reference

    def archive_session(self: AWSClientProvider) -> FakeSession:
        return FakeSession(DESTINATION_ACCOUNT, storage)

    destination_logs = live_logs or DestinationLogs()

    def live_client(self: AWSClientProvider) -> DestinationLogs:
        return destination_logs

    def disallow_network(self: AWSClientProvider) -> Any:
        raise AssertionError("unexpected storage network access")

    monkeypatch.setattr(AWSClientProvider, "s3_client", disallow_network)
    monkeypatch.setattr(AWSClientProvider, "boto3_session", archive_session)
    monkeypatch.setattr(AWSClientProvider, "cloudwatch_logs_client", live_client)
    return storage


def test_real_routes_merge_saved_history_and_retry_logs(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    live_logs = DestinationLogs()
    install_history(benchmark, database_session, tmp_path, monkeypatch, live_logs)
    client = TestClient(app)
    first = client.get(f"/benchmarks/{RUN_ID}/logs", params={"limit": 1}, headers=harness_headers)
    assert first.status_code == 200
    assert first.json()["events"][0]["event_id"] == "first"
    second = client.get(
        f"/benchmarks/{RUN_ID}/logs", params={"cursor": first.json()["next_cursor"]}, headers=harness_headers
    )
    assert [event["event_id"] for event in second.json()["events"]] == ["second", "live"]
    task = make_task(benchmark, "new-task")
    database_session.add(task)
    database_session.commit()
    live_logs.stream_name = task_log_stream_name(task.task_id, task.started_at)
    retry = client.get(f"/benchmarks/{RUN_ID}/logs", params={"task_id": task.task_id}, headers=harness_headers)
    assert [(event["event_id"], event["task_id"]) for event in retry.json()["events"]] == [("live", "new-task")]
    follow = client.get(f"/benchmarks/{RUN_ID}/logs/stream", params={"task_id": task.task_id}, headers=harness_headers)
    assert "event: end" in follow.text
    assert "private old" not in follow.text


def test_real_route_fails_closed_on_missing_history(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = install_history(example_benchmark_object, database_session, tmp_path, monkeypatch)
    storage.objects.clear()
    response = TestClient(app).get(f"/benchmarks/{RUN_ID}/logs", headers=harness_headers)
    assert response.status_code == 502
    assert "archive" in response.json()["detail"]


def test_native_links_do_not_claim_archived_history(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_history(example_benchmark_object, database_session, tmp_path, monkeypatch)
    response = TestClient(app).get(f"/benchmarks/{RUN_ID}", headers=harness_headers)
    assert response.status_code == 200
    assert response.json()["cloudwatch_url"] is None


def test_archive_route_does_not_cross_org_or_run_task_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    install_history(benchmark, database_session, tmp_path, monkeypatch)
    other_org = Org(id=uuid4(), name="other")
    other_run = Benchmark(
        org_id=other_org.id, name="other", arguments=benchmark.arguments, log_history=benchmark.log_history
    )
    database_session.add_all([other_org, other_run])
    database_session.commit()
    client = TestClient(app)
    assert client.get(f"/benchmarks/{other_run.id}/logs", headers=harness_headers).status_code == 404
    missing_task = client.get(f"/benchmarks/{RUN_ID}/logs", params={"task_id": "not-this-run"}, headers=harness_headers)
    assert missing_task.status_code == 404


def test_archive_task_native_link_is_absent(
    database_session: Session,
    example_benchmark_object: Benchmark,
    harness_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    install_history(benchmark, database_session, tmp_path, monkeypatch)
    task = make_task(benchmark, "new-task")
    database_session.add(task)
    database_session.commit()
    monkeypatch.setattr("tracker.api.single_task.s3_object_exists", AsyncMock(return_value=False))
    response = TestClient(app).get(f"/benchmarks/{RUN_ID}/tasks/{task.task_id}/artifacts", headers=harness_headers)
    assert response.status_code == 200
    assert response.json()["cloudwatch_url"] is None


def test_ordinary_run_stores_sql_null_history(database_session: Session, example_benchmark_object: Benchmark) -> None:
    database_session.add(example_benchmark_object)
    database_session.commit()
    assert (
        database_session.exec(
            select(Benchmark).where(Benchmark.id == example_benchmark_object.id, col(Benchmark.log_history).is_(None))
        )
        .one()
        .id
        == example_benchmark_object.id
    )
