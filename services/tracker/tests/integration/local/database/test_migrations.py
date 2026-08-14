"""PostgreSQL tests for operational Alembic migration contracts."""

import os
import subprocess
import sys
from collections.abc import Generator
from datetime import UTC, datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from sqlalchemy import inspect, text
from sqlmodel import Session, create_engine
from testcontainers.postgres import PostgresContainer

from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
)

_TRACKER_ROOT = Path(__file__).resolve().parents[4]
_ALEMBIC_INI = _TRACKER_ROOT / "alembic.ini"
_EXECUTOR_RELEASE_OWNERSHIP_REVISION = "c7d8e9f0a1b2"
_EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR = "6f3c2d9a8b10"
_CURRENT_OWNERSHIP_REVISION = "e9f0a1b2c3d4"
_PREVIOUS_REVISION = "d8e9f0a1b2c3"
_MAINTENANCE_REVISION = "f0a1b2c3d4e5"
_TASK_ATTEMPT_FAILURE_HISTORY_REVISION = "a3f4b5c6d7e8"
_MIGRATION_ADVISORY_LOCK_ID = 0x56414C4B59524945


@pytest.fixture
def migration_database_url() -> Generator[str, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url()


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(_ALEMBIC_INI), *args],
        cwd=_TRACKER_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        capture_output=True,
        text=True,
        check=False,
    )


def test_executor_release_ownership_downgrade_restores_predecessor_schema(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _EXECUTOR_RELEASE_OWNERSHIP_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    downgrade = _run_alembic(migration_database_url, "downgrade", _EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr

    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    assert "executorrelease" not in inspector.get_table_names()
    assert "executoradmission" not in inspector.get_table_names()
    benchmark_columns = {column["name"] for column in inspector.get_columns("benchmark")}
    assert (
        not {
            "executor_release_id",
            "executor_artifact_uri",
            "executor_artifact_digest",
            "executor_protocol_version",
        }
        & benchmark_columns
    )
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        release_status_enum_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'executorreleasestatus')")
        ).scalar_one()
    assert revision == _EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR
    assert release_status_enum_exists is False
    engine.dispose()


def test_task_attempt_failure_history_migrates_legacy_failures_without_inventing_attempts(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _MAINTENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    org_id = uuid4()
    benchmark_id = uuid4()
    task_id = uuid4()
    legacy_failures = [
        (uuid4(), datetime(2026, 8, 10, 12, 0, tzinfo=UTC), "first legacy failure"),
        (uuid4(), datetime(2026, 8, 11, 12, 0, tzinfo=UTC), "second legacy failure"),
    ]
    benchmark_finished_at = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "legacy-failure-migration-org"},
        )
        connection.execute(
            text(
                "INSERT INTO benchmark "
                "(id, org_id, name, started_at, finished_at, status, error_message) "
                "VALUES (:id, :org_id, :name, :started_at, :finished_at, :status, :error_message)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "legacy-failure-migration-benchmark",
                "started_at": benchmark_finished_at,
                "finished_at": benchmark_finished_at,
                "status": "ERROR",
                "error_message": "benchmark-level failure",
            },
        )
        connection.execute(
            text(
                "INSERT INTO task "
                "(id, org_id, task_id, status, started_at, finished_at, benchmark) "
                "VALUES (:id, :org_id, :task_id, :status, :started_at, :finished_at, :benchmark)"
            ),
            {
                "id": task_id,
                "org_id": org_id,
                "task_id": "legacy-task",
                "status": "ERROR",
                "started_at": legacy_failures[0][1],
                "finished_at": legacy_failures[1][1],
                "benchmark": benchmark_id,
            },
        )
        connection.execute(
            text(
                "INSERT INTO errorresult (id, org_id, task, created_at, error_message) VALUES "
                "(:id, :org_id, :task, :created_at, :error_message)"
            ),
            [
                {
                    "id": failure_id,
                    "org_id": org_id,
                    "task": task_id,
                    "created_at": created_at,
                    "error_message": message,
                }
                for failure_id, created_at, message in legacy_failures
            ],
        )

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_ATTEMPT_FAILURE_HISTORY_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    with engine.connect() as connection:
        migrated_failures = (
            connection.execute(
                text(
                    "SELECT id, schema_version, org_id, benchmark_id, task, task_attempt_id, dispatch_id, "
                    "retry_sequence, created_at, category, producer, operation, error_type, error_message, "
                    "classification_state, cause_code, terminal_effect, safe_details "
                    "FROM failurerecord WHERE task = :task ORDER BY created_at"
                ),
                {"task": task_id},
            )
            .mappings()
            .all()
        )
        benchmark_failures = (
            connection.execute(
                text(
                    "SELECT id, schema_version, org_id, benchmark_id, task, task_attempt_id, dispatch_id, "
                    "retry_sequence, created_at, category, producer, operation, error_type, error_message, "
                    "classification_state, cause_code, terminal_effect, safe_details "
                    "FROM failurerecord WHERE task IS NULL AND benchmark_id = :benchmark_id"
                ),
                {"benchmark_id": benchmark_id},
            )
            .mappings()
            .all()
        )
        attempt_count = connection.execute(text("SELECT count(*) FROM taskattempt")).scalar_one()
        active_attempt_id = connection.execute(
            text("SELECT active_attempt_id FROM task WHERE id = :task_id"), {"task_id": task_id}
        ).scalar_one()

    assert len(migrated_failures) == len(legacy_failures)
    for migrated, (failure_id, created_at, message) in zip(migrated_failures, legacy_failures, strict=True):
        assert migrated["id"] == failure_id
        assert migrated["schema_version"] == 1
        assert migrated["org_id"] == org_id
        assert migrated["benchmark_id"] == benchmark_id
        assert migrated["task"] == task_id
        assert migrated["created_at"] == created_at.replace(tzinfo=None)
        assert migrated["error_message"] == message
        assert migrated["category"] == "unknown"
        assert migrated["classification_state"] == "legacy_unclassified"
        assert migrated["terminal_effect"] == "terminal"
        assert all(
            migrated[field] is None
            for field in (
                "task_attempt_id",
                "dispatch_id",
                "retry_sequence",
                "producer",
                "operation",
                "error_type",
                "cause_code",
                "safe_details",
            )
        )

    assert len(benchmark_failures) == 1
    benchmark_failure = benchmark_failures[0]
    assert benchmark_failure["schema_version"] == 1
    assert benchmark_failure["org_id"] == org_id
    assert benchmark_failure["benchmark_id"] == benchmark_id
    assert benchmark_failure["created_at"] == benchmark_finished_at.replace(tzinfo=None)
    assert benchmark_failure["error_message"] == "benchmark-level failure"
    assert benchmark_failure["category"] == "unknown"
    assert benchmark_failure["classification_state"] == "legacy_unclassified"
    assert benchmark_failure["terminal_effect"] == "terminal"
    assert all(
        benchmark_failure[field] is None
        for field in (
            "task",
            "task_attempt_id",
            "dispatch_id",
            "retry_sequence",
            "producer",
            "operation",
            "error_type",
            "cause_code",
            "safe_details",
        )
    )
    assert attempt_count == 0
    assert active_attempt_id is None
    engine.dispose()


def test_task_attempt_failure_history_head_is_roll_forward_only(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr

    downgrade = _run_alembic(migration_database_url, "downgrade", _MAINTENANCE_REVISION)

    assert downgrade.returncode != 0
    assert "cannot be reconstructed" in downgrade.stderr
    engine = create_engine(migration_database_url)
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _TASK_ATTEMPT_FAILURE_HISTORY_REVISION
    engine.dispose()


def test_maintenance_fence_migration_is_additive(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _MAINTENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    admission_columns = {column["name"] for column in inspect(engine).get_columns("executoradmission")}
    assert "maintenance_target_sha" in admission_columns
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _MAINTENANCE_REVISION
    engine.dispose()


def test_upgrade_waits_for_the_migration_advisory_lock(migration_database_url: str) -> None:
    engine = create_engine(migration_database_url)
    with engine.connect() as lock_holder, ThreadPoolExecutor(max_workers=1) as executor:
        lock_holder.execute(
            text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
        )
        lock_holder.commit()
        upgrade = executor.submit(_run_alembic, migration_database_url, "upgrade", _MAINTENANCE_REVISION)
        deadline = monotonic() + 10
        try:
            while monotonic() < deadline:
                with engine.connect() as observer:
                    waiting = observer.execute(
                        text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)")
                    ).scalar_one()
                if waiting:
                    break
                if upgrade.done():
                    result = upgrade.result()
                    pytest.fail(f"Alembic exited before waiting for the migration lock: {result.stderr}")
                sleep(0.05)
            else:
                pytest.fail("Alembic did not wait for the migration advisory lock")
            assert not upgrade.done()
        finally:
            lock_holder.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": _MIGRATION_ADVISORY_LOCK_ID},
            )
            lock_holder.commit()

        result = upgrade.result(timeout=30)

    assert result.returncode == 0, result.stderr
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _MAINTENANCE_REVISION
    engine.dispose()


def test_current_execution_ownership_migration_rejects_downgrade(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _CURRENT_OWNERSHIP_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    org_id = uuid4()
    benchmark_id = uuid4()
    with Session(engine) as session:
        session.add(Org(id=org_id, name="migration-test-org"))
        session.add(
            ExecutorRelease(
                id="migration-test-release",
                artifact_uri="s3://artifacts/migration-test-release.pex",
                artifact_digest="a" * 64,
                protocol_version="1",
                status=ExecutorReleaseStatus.ACTIVE,
                readiness_verified=True,
            )
        )
        session.commit()
        session.add(
            Benchmark(
                id=benchmark_id,
                org_id=org_id,
                name="migration-test-benchmark",
                status=BenchmarkStatus.IN_PROGRESS,
                arguments=BenchmarkArguments(
                    contract=AgentContractRequest(name="migration-test-agent", install_cmd="true", run_cmd="true"),
                    concurrency=1,
                ),
                current_execution_release_id="migration-test-release",
            )
        )
        session.commit()

    downgrade = _run_alembic(migration_database_url, "downgrade", _PREVIOUS_REVISION)

    assert downgrade.returncode != 0
    assert "current execution release ownership" in downgrade.stderr
    inspector = inspect(engine)
    assert "current_execution_release_id" in {column["name"] for column in inspector.get_columns("benchmark")}
    assert "ix_benchmark_current_execution_release_id" in {
        index["name"] for index in inspector.get_indexes("benchmark")
    }
    assert any(
        foreign_key["constrained_columns"] == ["current_execution_release_id"]
        for foreign_key in inspector.get_foreign_keys("benchmark")
    )
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        stored_owner = connection.execute(
            text("SELECT current_execution_release_id FROM benchmark WHERE id = :benchmark_id"),
            {"benchmark_id": benchmark_id},
        ).scalar_one()
    assert revision == _CURRENT_OWNERSHIP_REVISION
    assert stored_owner == "migration-test-release"
    engine.dispose()
