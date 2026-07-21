"""Tests for trusted executor-release operator commands."""

import json
from datetime import datetime, timezone
from typing import Any

import pytest
from dateutil.parser import isoparse
from pytest import CaptureFixture, MonkeyPatch
from sqlmodel import Session

from tracker import release_cli
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
)


@pytest.fixture(autouse=True)
def active_executor_release(database_session: Session) -> None:
    """Provide the admission target reported by the status command."""
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version="1",
        status=ExecutorReleaseStatus.ACTIVE,
        readiness_verified=True,
    )
    database_session.add(release)
    database_session.add(ExecutorAdmission(release_id=release.id))
    database_session.commit()


def _release_status(
    database_session: Session,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> dict[str, Any]:
    monkeypatch.setattr(release_cli, "engine", database_session.get_bind())

    release_cli.main(["status"])

    return json.loads(capsys.readouterr().out)


def test_status_exposes_active_candidates_and_retirement_blockers(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    database_session.add(
        ExecutorRelease(
            id="candidate-release",
            artifact_uri="s3://artifacts/candidate.pex",
            artifact_digest="a" * 64,
            protocol_version="1",
            status=ExecutorReleaseStatus.CANDIDATE,
            readiness_verified=False,
        )
    )
    database_session.add(
        ExecutorRelease(
            id="draining-release",
            artifact_uri="s3://artifacts/draining.pex",
            artifact_digest="b" * 64,
            protocol_version="1",
            status=ExecutorReleaseStatus.DRAINING,
            readiness_verified=True,
        )
    )
    example_benchmark_object.executor_release_id = "draining-release"
    example_benchmark_object.current_execution_release_id = "draining-release"
    example_benchmark_object.executor_artifact_uri = "s3://artifacts/draining.pex"
    example_benchmark_object.executor_artifact_digest = "b" * 64
    example_benchmark_object.executor_protocol_version = "1"
    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(example_benchmark_object)
    database_session.commit()

    body = _release_status(database_session, monkeypatch, capsys)

    assert body["active_release_id"] == "test-release"
    entries = {entry["id"]: entry for entry in body["entries"]}
    assert entries["candidate-release"]["readiness_verified"] is False
    assert entries["draining-release"]["owned_active_runs"] == 1
    assert entries["draining-release"]["retirement_blocker"] == "1 active execution"
    assert entries["draining-release"]["blocking_dispatches"] == []
    blocking_executions = entries["draining-release"]["blocking_executions"]
    assert len(blocking_executions) == 1
    assert blocking_executions[0]["benchmark_id"] == str(example_benchmark_object.id)
    assert blocking_executions[0]["status"] == "IN_PROGRESS"
    assert blocking_executions[0]["current_execution_release_id"] == "draining-release"
    assert isoparse(blocking_executions[0]["started_at"]) == example_benchmark_object.started_at
    assert body["unattributed_active_execution_count"] == 0
    assert body["unattributed_active_executions"] == []


def test_status_counts_retry_dispatch_on_its_selected_release(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    release = ExecutorRelease(
        id="retry-release",
        artifact_uri="s3://artifacts/retry.pex",
        artifact_digest="b" * 64,
        protocol_version="1",
        status=ExecutorReleaseStatus.DRAINING,
        readiness_verified=True,
    )
    database_session.add(release)
    example_benchmark_object.current_execution_release_id = release.id
    database_session.add(example_benchmark_object)
    database_session.commit()
    database_session.add(
        ExecutorDispatch(
            benchmark_id=example_benchmark_object.id,
            kind=ExecutorDispatchKind.RETRY,
            status=ExecutorDispatchStatus.RUNNING,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
        )
    )
    database_session.commit()

    entries = {entry["id"]: entry for entry in _release_status(database_session, monkeypatch, capsys)["entries"]}

    assert entries["retry-release"]["owned_active_runs"] == 1
    assert entries["retry-release"]["retirement_blocker"] == "1 active execution"
    assert entries["retry-release"]["blocking_executions"] == []
    assert len(entries["retry-release"]["blocking_dispatches"]) == 1
    assert entries["retry-release"]["blocking_dispatches"][0]["benchmark_id"] == str(example_benchmark_object.id)
    assert entries["retry-release"]["blocking_dispatches"][0]["status"] == "RUNNING"


def test_status_exposes_null_owner_with_dispatch_history(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    release = ExecutorRelease(
        id="draining-release",
        artifact_uri="s3://artifacts/draining.pex",
        artifact_digest="b" * 64,
        protocol_version="1",
        status=ExecutorReleaseStatus.DRAINING,
        readiness_verified=True,
    )
    database_session.add(release)
    example_benchmark_object.executor_release_id = release.id
    example_benchmark_object.current_execution_release_id = None
    example_benchmark_object.executor_artifact_uri = release.artifact_uri
    example_benchmark_object.executor_artifact_digest = release.artifact_digest
    example_benchmark_object.executor_protocol_version = release.protocol_version
    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(example_benchmark_object)
    database_session.flush()
    database_session.add(
        ExecutorDispatch(
            benchmark_id=example_benchmark_object.id,
            kind=ExecutorDispatchKind.START,
            status=ExecutorDispatchStatus.FINISHED,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
        )
    )
    database_session.commit()

    body = _release_status(database_session, monkeypatch, capsys)

    assert body["unattributed_active_execution_count"] == 1
    unattributed = body["unattributed_active_executions"]
    assert len(unattributed) == 1
    assert unattributed[0]["benchmark_id"] == str(example_benchmark_object.id)
    assert unattributed[0]["status"] == "IN_PROGRESS"
    assert isoparse(unattributed[0]["started_at"]) == example_benchmark_object.started_at
    entry = next(entry for entry in body["entries"] if entry["id"] == release.id)
    assert entry["owned_active_runs"] == 0
    assert entry["blocking_dispatches"] == []
    assert entry["blocking_executions"] == []
    assert entry["retirement_blocker"] == "1 unattributed active execution"


def test_status_handles_naive_retention_timestamp(
    database_session: Session,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    retention_until = datetime(2100, 1, 1)
    database_session.add(
        ExecutorRelease(
            id="retired-release",
            artifact_uri="s3://artifacts/retired.pex",
            artifact_digest="c" * 64,
            protocol_version="1",
            status=ExecutorReleaseStatus.RETIRED,
            readiness_verified=True,
            artifact_retention_until=retention_until,
        )
    )
    database_session.commit()

    entries = {entry["id"]: entry for entry in _release_status(database_session, monkeypatch, capsys)["entries"]}

    retired = entries["retired-release"]
    retention_until_utc = retention_until.replace(tzinfo=timezone.utc)
    assert isoparse(retired["artifact_retention_until"]) == retention_until_utc
    assert retired["retirement_blocker"] == f"artifact retained until {retention_until_utc.isoformat()}"
