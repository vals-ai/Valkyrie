"""Run with `uv run pytest tests/unit/api/test_task_artifacts.py`."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import quote
from unittest.mock import ANY, AsyncMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.task_artifacts as api
from main import app
from tests.factories import make_task
from tracker.aws.cloudwatch_logs import task_log_attempt_id
from tracker.database.models import Benchmark, Org
from tracker.task_artifacts import (
    ArtifactContent,
    ArtifactFile,
    ArtifactIndex,
    ArtifactIndexNotFoundError,
)

_client = TestClient(app)


def _index() -> ArtifactIndex:
    return ArtifactIndex(
        generation="a" * 32,
        archive_available=True,
        pack_size_bytes=8,
        files=[
            ArtifactFile(path="agent_output/fix.patch", size_bytes=2, offset=0),
            ArtifactFile(
                path="agent_output/vals_format/turns.jsonl",
                size_bytes=5,
                offset=2,
            ),
            ArtifactFile(path="summary.txt", size_bytes=1, offset=7),
        ],
    )


def test_artifact_index_defaults_to_current_attempt_and_derives_storage_scope(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=ZoneInfo("UTC"))
    benchmark = example_benchmark_object
    task = make_task(benchmark, "suite/task", started_at=started_at)
    database_session.add_all([benchmark, task])
    database_session.commit()

    load = AsyncMock(return_value=_index())
    monkeypatch.setattr(api, "load_artifact_index", load)

    task_path = quote(task.task_id, safe="")
    response = _client.get(f"/benchmarks/{benchmark.id}/tasks/{task_path}/artifacts/index")

    attempt_id = task_log_attempt_id(started_at)
    assert response.status_code == 200
    assert response.json() == {
        "attempt_id": attempt_id,
        "is_current": True,
        "archive_available": True,
        "file_count": 3,
        "pack_size_bytes": 8,
        "trajectory_path": "agent_output/vals_format/turns.jsonl",
        "diff_path": "agent_output/fix.patch",
    }
    load.assert_awaited_once_with(str(benchmark.id), task.task_id, attempt_id, ANY)


def test_artifact_files_page_direct_children(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    monkeypatch.setattr(api, "load_artifact_index", AsyncMock(return_value=_index()))

    response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts/files",
        params={"prefix": "agent_output", "limit": 1},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"] == [
        {
            "kind": "directory",
            "path": "agent_output/vals_format",
        }
    ]
    assert body["next_cursor"] is not None


def test_artifact_content_reads_exact_attempt_and_returns_bytes(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    load = AsyncMock(return_value=_index())
    read = AsyncMock(
        return_value=ArtifactContent(
            data=b'{"turn":1}\n',
            next_cursor="next",
        )
    )
    monkeypatch.setattr(api, "load_artifact_index", load)
    monkeypatch.setattr(api, "read_artifact_content", read)

    response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts/content",
        params={
            "attempt_id": "deadbeef",
            "path": "agent_output/vals_format/turns.jsonl",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "attempt_id": "deadbeef",
        "path": "agent_output/vals_format/turns.jsonl",
        "size_bytes": 5,
        "next_cursor": "next",
        "content_base64": "eyJ0dXJuIjoxfQo=",
    }
    load.assert_awaited_once_with(str(benchmark.id), task.task_id, "deadbeef", ANY)
    read.assert_awaited_once_with(
        str(benchmark.id),
        task.task_id,
        "deadbeef",
        "a" * 32,
        _index().files[1],
        None,
        ANY,
    )


def test_artifact_archive_redirects_to_exact_attempt(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    database_session.add_all([benchmark, task])
    database_session.commit()
    monkeypatch.setattr(api, "load_artifact_index", AsyncMock(return_value=_index()))
    presign = AsyncMock(return_value="https://example.test/exact-archive")
    monkeypatch.setattr(api, "create_presigned_url", presign)

    response = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts/archive",
        params={"attempt_id": "deadbeef"},
        follow_redirects=False,
    )

    key = (
        f"benchmarks/{benchmark.id}/{task.task_id}/.valkyrie/artifacts/deadbeef"
        f"/generations/{'a' * 32}/agent_output.tar.gz"
    )
    assert response.status_code == 303
    assert response.headers["location"] == "https://example.test/exact-archive"
    presign.assert_awaited_once_with(key, ANY, expiration=300)


def test_missing_historical_index_is_explicit_404_and_org_scoped(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = example_benchmark_object
    task = make_task(benchmark, "task")
    other_org = Org(id=uuid4(), name="other")
    other_benchmark = Benchmark(
        org_id=other_org.id,
        name=benchmark.name,
        arguments=benchmark.arguments,
    )
    other_task = make_task(other_benchmark, "task")
    database_session.add_all(
        [
            benchmark,
            task,
            other_org,
            other_benchmark,
            other_task,
        ]
    )
    database_session.commit()
    load = AsyncMock(side_effect=ArtifactIndexNotFoundError)
    monkeypatch.setattr(api, "load_artifact_index", load)

    missing = _client.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/artifacts/index",
        params={"attempt_id": "deadbeef"},
    )
    forbidden = _client.get(
        f"/benchmarks/{other_benchmark.id}/tasks/{other_task.task_id}/artifacts/index",
    )

    assert missing.status_code == 404
    assert missing.json()["detail"] == "Artifact index not found for this attempt"
    assert forbidden.status_code == 404
    load.assert_awaited_once()
