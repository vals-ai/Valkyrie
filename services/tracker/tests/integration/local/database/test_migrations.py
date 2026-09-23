"""PostgreSQL tests for operational Alembic migration contracts."""

import os
import asyncio
import subprocess
import sys
from collections.abc import Generator
from typing import Protocol
from datetime import UTC, datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, create_engine, select
from testcontainers.postgres import PostgresContainer
from tests.integration.local.database.conftest import local_postgres_url
from tests.factories import make_benchmark, make_task
from services.executor_host.supervisor import ArtifactDispatch, PostgresExecutorDispatchStore
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release

from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorTaskAttempt,
    ExecutorTaskReceipt,
    ExecutorRunReceipt,
    Org,
)

_TRACKER_ROOT = Path(__file__).resolve().parents[4]
_ALEMBIC_INI = _TRACKER_ROOT / "alembic.ini"
_EXECUTOR_RELEASE_OWNERSHIP_REVISION = "c7d8e9f0a1b2"
_EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR = "6f3c2d9a8b10"
_CURRENT_OWNERSHIP_REVISION = "e9f0a1b2c3d4"
_PREVIOUS_REVISION = "d8e9f0a1b2c3"
_MAINTENANCE_REVISION = "f0a1b2c3d4e5"
_ERROR_RESULT_PROVENANCE_REVISION = "a3f4b5c6d7e8"
_DISPATCH_LEASE_REVISION = "6a7b8c9d0e1f"
_MIGRATION_ADVISORY_LOCK_ID = 0x56414C4B59524945
_TASK_LISTING_REVISION = "2d3e4f5a6b7c"
_TASK_LISTING_PREDECESSOR = "1c2d3e4f5a6b"


def test_migration_graph_has_single_head() -> None:
    heads = ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_heads()

    assert len(heads) == 1, f"Expected one Alembic head, found {heads}"


def test_dispatch_lease_migration_adds_recovery_state(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _DISPATCH_LEASE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("executordispatch")}
    assert {
        "assigned_task_ids",
        "claim_deadline_at",
        "heartbeat_at",
        "lease_expires_at",
        "failure_reason",
    } <= columns
    assert any(
        index["name"] == "ix_executordispatch_status_lease_expires"
        for index in inspector.get_indexes("executordispatch")
    )
    engine.dispose()


@pytest.mark.parametrize("revision", ["3e4f5a6b7c8d", "4f5a6b7c8d9e", "5a6b7c8d9e0f"])
def test_dispatch_api_migration_preserves_a_live_legacy_claim(migration_database_url: str, revision: str) -> None:
    """Keep a legacy host claim valid across the additive executor API migration.

    Test cases:
    - The existing host claims a dispatch against the pre-API schema.
    - The same host renews and finishes that claim after the migration.
    - Migration leaves the release pin, start time, and credential eligibility unchanged.
    - New task ownership and receipt rows do not block legacy task deletion.
    """
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    try:
        with Session(engine, expire_on_commit=False) as session:
            org = Org(name="legacy-api-migration")
            session.add(org)
            session.flush()
            benchmark = make_benchmark(org_id=org.id)
            release = ExecutorRelease(
                id="legacy-api-migration",
                artifact_uri="s3://artifacts/legacy.pex",
                artifact_digest="a" * 64,
                protocol_version="2",
                readiness_verified=True,
            )
            register_release(session, release)
            pin_benchmark_to_release(benchmark, release)
            session.add(benchmark)
            session.flush()
            dispatch = create_executor_dispatch(
                benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=[]
            )
            session.add(dispatch)
            session.commit()
            benchmark_id, dispatch_id = benchmark.id, dispatch.id
            artifact = ArtifactDispatch.from_payload(
                {
                    "executor_release_id": release.id,
                    "executor_artifact_uri": release.artifact_uri,
                    "executor_artifact_digest": release.artifact_digest,
                    "executor_protocol_version": release.protocol_version,
                }
            )

        url = engine.url
        host = url.query.get("host", url.host)
        assert isinstance(host, str)
        assert url.username is not None and url.password is not None and url.database is not None
        store = PostgresExecutorDispatchStore(
            host=host, port=str(url.port or 5432), user=url.username, password=url.password, dbname=url.database
        )
        authority = asyncio.run(store.claim(str(dispatch_id), str(benchmark_id), artifact))
        assert authority is not None
        with Session(engine) as session:
            claimed = session.get(ExecutorDispatch, dispatch_id)
            assert claimed is not None
            started_at = claimed.started_at

        upgrade = _run_alembic(migration_database_url, "upgrade", revision)
        assert upgrade.returncode == 0, upgrade.stderr
        assert asyncio.run(store.is_current(authority))
        assert asyncio.run(store.heartbeat(authority))
        assert asyncio.run(store.claim(str(dispatch_id), str(benchmark_id), artifact)) is None
        assert asyncio.run(store.finish(authority))

        with Session(engine) as session:
            completed = session.get(ExecutorDispatch, dispatch_id)
            assert completed is not None
            assert completed.status == ExecutorDispatchStatus.FINISHED
            assert completed.started_at == started_at
            assert completed.executor_release_id == "legacy-api-migration"
            assert session.connection().execute(text("SELECT COUNT(*) FROM executordispatchaccess")).scalar_one() == 0

            if revision in ("4f5a6b7c8d9e", "5a6b7c8d9e0f"):
                task = make_task(benchmark, "migrated-task")
                session.add(task)
                session.flush()
                session.add_all(
                    [
                        ExecutorTaskAttempt(task_id=task.id, dispatch_id=dispatch_id, started_at=task.started_at),
                        ExecutorTaskReceipt(
                            dispatch_id=dispatch_id,
                            command_id=uuid4(),
                            task_id=task.id,
                            request_digest="a" * 64,
                            revision=0,
                        ),
                    ]
                )
                session.commit()
                session.delete(task)
                session.commit()
                assert not session.exec(select(ExecutorTaskAttempt)).all()
                assert not session.exec(select(ExecutorTaskReceipt)).all()
            if revision == "5a6b7c8d9e0f":
                receipt = ExecutorRunReceipt(
                    dispatch_id=dispatch_id, command_id=uuid4(), request_digest="b" * 64, status="FINISHED"
                )
                session.add(receipt)
                session.commit()
                session.delete(completed)
                session.commit()
                assert not session.exec(select(ExecutorRunReceipt)).all()
    finally:
        engine.dispose()


@pytest.fixture
def migration_database_url() -> Generator[str, None, None]:
    binary_directory = os.environ.get("TEST_POSTGRES_BIN")
    if binary_directory:
        with local_postgres_url(binary_directory) as url:
            yield url.render_as_string(hide_password=False)
        return

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


class _TaskListingIndexRow(Protocol):
    index_oid: int
    indisvalid: bool
    indisunique: bool
    indnkeyatts: int
    indnatts: int
    has_no_expressions: bool
    has_no_predicate: bool
    amname: str
    table_schema: str
    table_name: str
    key_columns: list[str]
    key_1_asc: bool
    key_1_nulls_last: bool
    key_2_asc: bool
    key_2_nulls_last: bool
    key_3_desc: bool
    key_3_nulls_first: bool


def _assert_canonical_task_listing_index(index: _TaskListingIndexRow) -> int:
    assert index.indisvalid is True
    assert index.indisunique is False
    assert index.indnkeyatts == 3
    assert index.indnatts == 3
    assert index.has_no_expressions is True
    assert index.has_no_predicate is True
    assert index.amname == "btree"
    assert index.table_schema == "public"
    assert index.table_name == "task"
    assert index.key_columns == ["benchmark", "org_id", "started_at"]
    assert index.key_1_asc is True
    assert index.key_1_nulls_last is True
    assert index.key_2_asc is True
    assert index.key_2_nulls_last is True
    assert index.key_3_desc is True
    assert index.key_3_nulls_first is True
    return index.index_oid


def test_task_listing_index_migration_is_retry_safe(migration_database_url: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    index_query = text(
        """
        SELECT
            i.indexrelid AS index_oid,
            i.indisvalid,
            i.indisunique,
            i.indnkeyatts,
            i.indnatts,
            i.indexprs IS NULL AS has_no_expressions,
            i.indpred IS NULL AS has_no_predicate,
            am.amname,
            tn.nspname AS table_schema,
            t.relname AS table_name,
            (
                SELECT array_agg(a.attname ORDER BY key.ordinality)
                FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
                JOIN pg_attribute AS a
                  ON a.attrelid = i.indrelid
                 AND a.attnum = key.attnum
            ) AS key_columns,
            pg_index_column_has_property(i.indexrelid, 1, 'asc') AS key_1_asc,
            pg_index_column_has_property(i.indexrelid, 1, 'nulls_last') AS key_1_nulls_last,
            pg_index_column_has_property(i.indexrelid, 2, 'asc') AS key_2_asc,
            pg_index_column_has_property(i.indexrelid, 2, 'nulls_last') AS key_2_nulls_last,
            pg_index_column_has_property(i.indexrelid, 3, 'desc') AS key_3_desc,
            pg_index_column_has_property(i.indexrelid, 3, 'nulls_first') AS key_3_nulls_first
        FROM pg_index AS i
        JOIN pg_class AS c ON c.oid = i.indexrelid
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        JOIN pg_class AS t ON t.oid = i.indrelid
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace
        JOIN pg_am AS am ON am.oid = c.relam
        WHERE c.relname = :index_name AND n.nspname = current_schema()
        """
    )
    revision_query = text("SELECT version_num FROM alembic_version")
    index_params = {"index_name": "ix_task_benchmark_org_started_at"}

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        _assert_canonical_task_listing_index(index)
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(
                'CREATE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" '
                "ON task (benchmark, org_id, started_at DESC)"
            )
        )
        matching_index_oid = connection.execute(index_query, index_params).one().index_oid
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        assert _assert_canonical_task_listing_index(index) == matching_index_oid
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION
    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        assert connection.execute(index_query, index_params).one_or_none() is None
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(
                'CREATE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" '
                "ON task (org_id, benchmark, started_at ASC)"
            )
        )
        wrong_index = connection.execute(index_query, index_params).one()
        assert wrong_index.indisvalid is True
        assert wrong_index.indisunique is False
        assert wrong_index.key_columns == ["org_id", "benchmark", "started_at"]
        assert wrong_index.key_1_asc is True
        assert wrong_index.key_2_asc is True
        assert wrong_index.key_3_desc is False

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        _assert_canonical_task_listing_index(index)
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    org_id = uuid4()
    benchmark_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "task-listing-retry-org"},
        )
        connection.execute(
            text(
                "INSERT INTO benchmark "
                "(id, org_id, name, started_at, status) "
                "VALUES (:id, :org_id, :name, :started_at, :status)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "task-listing-retry-benchmark",
                "started_at": datetime(2026, 9, 21, tzinfo=UTC),
                "status": "IN_PROGRESS",
            },
        )
        for task_id in (uuid4(), uuid4()):
            connection.execute(
                text(
                    "INSERT INTO task "
                    "(id, org_id, task_id, status, started_at, benchmark) "
                    "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
                ),
                {
                    "id": task_id,
                    "org_id": org_id,
                    "task_id": str(task_id),
                    "status": "PENDING",
                    "started_at": datetime(2026, 9, 21, tzinfo=UTC),
                    "benchmark": benchmark_id,
                },
            )

    with engine.connect() as connection:
        with pytest.raises(DBAPIError):
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                text('CREATE UNIQUE INDEX CONCURRENTLY "ix_task_benchmark_org_started_at" ON task (org_id)')
            )
        invalid_index = connection.execute(index_query, index_params).one()
        assert invalid_index.indisvalid is False
        assert invalid_index.indisunique is True

    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        index = connection.execute(index_query, index_params).one()
        assert index.indisvalid is True
        assert index.indisunique is False
        assert index.key_columns == ["benchmark", "org_id", "started_at"]
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_REVISION

    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            text('DROP INDEX CONCURRENTLY IF EXISTS "ix_task_benchmark_org_started_at"')
        )
    downgrade = _run_alembic(migration_database_url, "downgrade", _TASK_LISTING_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        assert connection.execute(index_query, index_params).one_or_none() is None
        assert connection.execute(revision_query).scalar_one() == _TASK_LISTING_PREDECESSOR
    engine.dispose()


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


def test_error_result_provenance_migration_preserves_legacy_rows(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _MAINTENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    engine = create_engine(migration_database_url)
    org_id = uuid4()
    benchmark_id = uuid4()
    task_id = uuid4()
    raw_insert_id = uuid4()
    legacy_failures = [
        (uuid4(), datetime(2026, 8, 10, 12, 0, tzinfo=UTC), "first legacy failure"),
        (uuid4(), datetime(2026, 8, 11, 12, 0, tzinfo=UTC), "second legacy failure"),
    ]
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "legacy-error-result-migration-org"},
        )
        connection.execute(
            text(
                "INSERT INTO benchmark (id, org_id, name, started_at, status) "
                "VALUES (:id, :org_id, :name, :started_at, :status)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "legacy-error-result-migration-benchmark",
                "started_at": legacy_failures[0][1],
                "status": "IN_PROGRESS",
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

    upgrade = _run_alembic(migration_database_url, "upgrade", _ERROR_RESULT_PROVENANCE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr

    with engine.begin() as connection:
        migrated_rows = (
            connection.execute(
                text(
                    "SELECT id, org_id, task, created_at, error_message, producer, operation, "
                    "error_type, cause_code, retry_scheduled, failed_attempt_number "
                    "FROM errorresult WHERE task = :task ORDER BY created_at"
                ),
                {"task": task_id},
            )
            .mappings()
            .all()
        )
        connection.execute(
            text(
                "INSERT INTO errorresult (id, org_id, task, created_at, error_message) "
                "VALUES (:id, :org_id, :task, :created_at, :error_message)"
            ),
            {
                "id": raw_insert_id,
                "org_id": org_id,
                "task": task_id,
                "created_at": datetime(2026, 8, 12, 12, 0, tzinfo=UTC),
                "error_message": "terminal row using database default",
            },
        )
        raw_retry_scheduled = connection.execute(
            text("SELECT retry_scheduled FROM errorresult WHERE id = :id"),
            {"id": raw_insert_id},
        ).scalar_one()

        inspector = inspect(connection)
        error_result_columns = {column["name"]: column for column in inspector.get_columns("errorresult")}
        error_result_indexes = {
            (index["name"], tuple(index["column_names"]), index["unique"])
            for index in inspector.get_indexes("errorresult")
        }

    assert len(migrated_rows) == len(legacy_failures)
    for migrated, (failure_id, created_at, message) in zip(migrated_rows, legacy_failures, strict=True):
        assert migrated["id"] == failure_id
        assert migrated["org_id"] == org_id
        assert migrated["task"] == task_id
        assert migrated["created_at"] == created_at.replace(tzinfo=None)
        assert migrated["error_message"] == message
        assert migrated["retry_scheduled"] is False
        assert all(
            migrated[field] is None
            for field in (
                "producer",
                "operation",
                "error_type",
                "cause_code",
                "failed_attempt_number",
            )
        )

    assert raw_retry_scheduled is False
    assert set(error_result_columns) == {
        "id",
        "org_id",
        "task",
        "created_at",
        "error_message",
        "producer",
        "operation",
        "error_type",
        "cause_code",
        "retry_scheduled",
        "failed_attempt_number",
    }
    assert error_result_columns["retry_scheduled"]["nullable"] is False
    assert error_result_indexes == {
        ("ix_errorresult_org_task_created_at", ("org_id", "task", "created_at"), False),
    }

    downgrade = _run_alembic(migration_database_url, "downgrade", _MAINTENANCE_REVISION)
    assert downgrade.returncode == 0, downgrade.stderr

    with engine.connect() as connection:
        inspector = inspect(connection)
        downgraded_columns = {column["name"] for column in inspector.get_columns("errorresult")}
        downgraded_indexes = {index["name"] for index in inspector.get_indexes("errorresult")}
        downgraded_rows = (
            connection.execute(
                text("SELECT id, org_id, task, created_at, error_message FROM errorresult ORDER BY created_at")
            )
            .mappings()
            .all()
        )
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()

    assert downgraded_columns == {"id", "org_id", "task", "created_at", "error_message"}
    assert "ix_errorresult_org_task_created_at" not in downgraded_indexes
    assert [dict(row) for row in downgraded_rows] == [
        {
            "id": legacy_failures[0][0],
            "org_id": org_id,
            "task": task_id,
            "created_at": legacy_failures[0][1].replace(tzinfo=None),
            "error_message": legacy_failures[0][2],
        },
        {
            "id": legacy_failures[1][0],
            "org_id": org_id,
            "task": task_id,
            "created_at": legacy_failures[1][1].replace(tzinfo=None),
            "error_message": legacy_failures[1][2],
        },
        {
            "id": raw_insert_id,
            "org_id": org_id,
            "task": task_id,
            "created_at": datetime(2026, 8, 12, 12, 0),
            "error_message": "terminal row using database default",
        },
    ]
    assert revision == _MAINTENANCE_REVISION
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
        # Insert with explicit columns: the schema is pinned at the ownership
        # revision, which predates columns the current ORM model would include.
        arguments = BenchmarkArguments(
            contract=AgentContractRequest(name="migration-test-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        )
        session.execute(
            text(
                "INSERT INTO benchmark"
                " (id, org_id, name, started_at, status, arguments, docent_reading_status,"
                " current_execution_release_id)"
                " VALUES (:id, :org_id, :name, now(), CAST(:status AS benchmarkstatus),"
                " CAST(:arguments AS json), CAST(:docent_reading_status AS docentreadingstatus), :release_id)"
            ),
            {
                "id": benchmark_id,
                "org_id": org_id,
                "name": "migration-test-benchmark",
                "status": BenchmarkStatus.IN_PROGRESS.value,
                "arguments": arguments.model_dump_json(),
                "docent_reading_status": "IDLE",
                "release_id": "migration-test-release",
            },
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
