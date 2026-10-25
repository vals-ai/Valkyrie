"""PostgreSQL tests for operational Alembic migration contracts.

Run: pytest tests/integration/local/database/test_migrations.py
"""

import subprocess
import traceback
from collections.abc import Generator
from typing import Protocol
from datetime import UTC, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

import tracker.config as tracker_config
from tracker.database.models import (
    AgentContractRequest,
    AWSBenchmarkArguments,
    Benchmark,
    BenchmarkStatus,
    LocalBenchmarkArguments,
    Project,
    RunProject,
    SpendType,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)
from tracker.local.resources import LocalResources

_TRACKER_ROOT = Path(__file__).resolve().parents[4]
_ALEMBIC_INI = _TRACKER_ROOT / "alembic.ini"
_EXECUTOR_RELEASE_OWNERSHIP_REVISION = "c7d8e9f0a1b2"
_EXECUTOR_RELEASE_OWNERSHIP_PREDECESSOR = "6f3c2d9a8b10"
_CURRENT_OWNERSHIP_REVISION = "e9f0a1b2c3d4"
_PREVIOUS_REVISION = "d8e9f0a1b2c3"
_MAINTENANCE_REVISION = "f0a1b2c3d4e5"
_ERROR_RESULT_PROVENANCE_REVISION = "a3f4b5c6d7e8"
_DISPATCH_LEASE_REVISION = "6a7b8c9d0e1f"
_DROP_DISPATCH_LEASE_REVISION = "5b6c7d8e9f0a"
_DROP_DISPATCH_LEASE_PREDECESSOR = "4a5b6c7d8e9f"
_DROP_RELEASE_READINESS_REVISION = "6c7d8e9f0a1b"
_MIGRATION_ADVISORY_LOCK_ID = 0x56414C4B59524945
_TASK_LISTING_REVISION = "2d3e4f5a6b7c"
_TASK_LISTING_PREDECESSOR = "1c2d3e4f5a6b"
_SANDBOX_BUILD_RESERVATION_REVISION = "4f5a6b7c8d9e"
_SANDBOX_BUILD_RESERVATION_PREDECESSOR = "3e4f5a6b7c8d"
_RESERVED_ADMISSION_REVISION = "d9e0f1a2b3c4"
_RESERVED_ADMISSION_PREDECESSOR = "c1d2e3f4a5b6"
_ORG_BUCKET_REVISION = "a7b8c9d0e1f2"
_ORG_BUCKET_PREDECESSOR = "b3c4d5e6f7a8"
_DROP_AWS_MANAGED_REVISION = "c4d5e6f7a8b9"
_PROJECT_REVISION = "7b1e4c9d2a60"
_PROJECT_PREDECESSOR = "b7e4c1a9d2f0"


def test_migration_graph_has_single_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(_ALEMBIC_INI)))
    heads = scripts.get_heads()

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


def test_dispatch_migration_retains_only_local_lease_state_and_drops_release_readiness(
    migration_database_url: str,
) -> None:
    lease_columns = {"claim_deadline_at", "heartbeat_at", "lease_expires_at"}
    readiness_columns = {"readiness_verified", "readiness_metadata"}

    def lease_state() -> tuple[set[str], set[str], bool]:
        engine = create_engine(migration_database_url)
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns("executordispatch")}
        release_columns = {column["name"] for column in inspector.get_columns("executorrelease")}
        indexed = any(
            index["name"] == "ix_executordispatch_status_lease_expires"
            for index in inspector.get_indexes("executordispatch")
        )
        engine.dispose()
        return columns & lease_columns, release_columns & readiness_columns, indexed

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_DISPATCH_LEASE_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == (lease_columns, readiness_columns, True)

    engine = create_engine(migration_database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO executorrelease"
                " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                " readiness_metadata, created_at)"
                " VALUES ('lease-drop-release', 'source:///src', :digest, '5', 'ACTIVE', true, '{}', now())"
            ),
            {"digest": "a" * 64},
        )
    engine.dispose()

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_DISPATCH_LEASE_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == ({"lease_expires_at"}, readiness_columns, True)

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_RELEASE_READINESS_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert lease_state() == ({"lease_expires_at"}, set(), True)

    downgrade = _run_alembic(migration_database_url, "downgrade", _DROP_DISPATCH_LEASE_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    assert lease_state() == (lease_columns, readiness_columns, True)


@pytest.fixture(scope="module")
def migration_postgres() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def migration_database_url(migration_postgres: PostgresContainer) -> Generator[str, None, None]:
    """Give each migration test an empty database in the module's shared Postgres."""
    name = f"migration_{uuid4().hex}"
    admin = create_engine(migration_postgres.get_connection_url(), isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f"CREATE DATABASE {name}"))
    yield make_url(migration_postgres.get_connection_url()).set(database=name).render_as_string(hide_password=False)
    with admin.connect() as connection:
        connection.execute(text(f"DROP DATABASE {name} WITH (FORCE)"))
    admin.dispose()


def _run_alembic(database_url: str, action: str, revision: str) -> subprocess.CompletedProcess[str]:
    """Run an Alembic command in-process, reporting failure the way the CLI would."""
    try:
        with patch.object(tracker_config, "DATABASE_URL", database_url), patch("logging.config.fileConfig"):
            getattr(command, action)(Config(str(_ALEMBIC_INI)), revision)
    except Exception:
        return subprocess.CompletedProcess([action, revision], 1, "", traceback.format_exc())
    return subprocess.CompletedProcess([action, revision], 0, "", "")


def test_failure_category_migration_aborts_on_busy_table_and_retries(
    migration_database_url: str,
) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", _TASK_LISTING_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    try:
        with engine.connect() as blocker:
            blocker.execute(text("SELECT id FROM errorresult"))
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_run_alembic, migration_database_url, "upgrade", "head")
                try:
                    blocked = future.result(timeout=10)
                finally:
                    # Release even on assertion failure so the subprocess can exit.
                    blocker.rollback()
            assert blocked.returncode != 0
            assert "lock timeout" in blocked.stderr

        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                _TASK_LISTING_REVISION
            )
            assert (
                connection.execute(text("SELECT count(*) FROM pg_type WHERE typname = 'failurecategory'")).scalar_one()
                == 0
            )
            assert "category" not in {column["name"] for column in inspect(connection).get_columns("errorresult")}

        retry = _run_alembic(migration_database_url, "upgrade", "head")
        assert retry.returncode == 0, retry.stderr
        with engine.connect() as connection:
            columns = {column["name"]: column for column in inspect(connection).get_columns("errorresult")}
            assert columns["category"]["nullable"] is True
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("predecessor", ["3e4f5a6b7c8d", "b4c5d6e7f8a9"])
def test_dispatch_and_failure_category_heads_upgrade_together(migration_database_url: str, predecessor: str) -> None:
    upgrade = _run_alembic(migration_database_url, "upgrade", predecessor)
    assert upgrade.returncode == 0, upgrade.stderr
    merge = _run_alembic(migration_database_url, "upgrade", "1f12bae14561")
    assert merge.returncode == 0, merge.stderr
    engine = create_engine(migration_database_url)
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "1f12bae14561"
            inspector = inspect(connection)
            assert "executor_dispatch_payload" in inspector.get_table_names()
            columns = {column["name"]: column for column in inspector.get_columns("errorresult")}
            assert columns["category"]["nullable"] is True
    finally:
        engine.dispose()


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
        session.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": "migration-test-org"},
        )
        session.commit()
        # Insert with explicit columns: the schema is pinned at the ownership
        # revision, which predates columns the current ORM model would include.
        session.execute(
            text(
                "INSERT INTO executorrelease"
                " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                " readiness_metadata, created_at)"
                " VALUES (:id, :artifact_uri, :artifact_digest, '1', 'ACTIVE', true, '{}', :created_at)"
            ),
            {
                "id": "migration-test-release",
                "artifact_uri": "s3://artifacts/migration-test-release.pex",
                "artifact_digest": "a" * 64,
                "created_at": datetime.now(UTC),
            },
        )
        arguments = AWSBenchmarkArguments(
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


def test_sandbox_build_reservation_upgrade_and_downgrade(
    migration_database_url: str,
) -> None:
    def migrate(direction: str, revision: str) -> None:
        result = _run_alembic(migration_database_url, direction, revision)
        assert result.returncode == 0, result.stderr

    migrate("upgrade", _SANDBOX_BUILD_RESERVATION_PREDECESSOR)
    engine = create_engine(migration_database_url)
    attempt_started_at = datetime(2026, 9, 29, 12)
    org_id = uuid4()
    task_row_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            {"id": org_id, "name": f"migration-reservation-{org_id}"},
        )
    with Session(engine) as session:
        benchmark = Benchmark(
            org_id=org_id,
            name="migration reservation",
            arguments=AWSBenchmarkArguments(
                contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
                queue_pool_id="pool_migration",
            ),
        )
        session.add(benchmark)
        session.flush()
        session.execute(
            text(
                "INSERT INTO task (id, org_id, task_id, status, started_at, benchmark) "
                "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
            ),
            {
                "id": task_row_id,
                "org_id": org_id,
                "task_id": "reserved-task",
                "status": "BUILDING",
                "started_at": attempt_started_at,
                "benchmark": benchmark.id,
            },
        )
        session.commit()

    migrate("upgrade", _SANDBOX_BUILD_RESERVATION_REVISION)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO sandboxbuildreservation "
                "(task_row_id, attempt_started_at, reserved_at, pool_id, "
                "requested_vcpu, requested_memory, requested_disk) "
                "VALUES (:task_row_id, :attempt_started_at, :reserved_at, :pool_id, "
                ":requested_vcpu, :requested_memory, :requested_disk)"
            ),
            {
                "task_row_id": task_row_id,
                "attempt_started_at": attempt_started_at,
                "reserved_at": attempt_started_at,
                "pool_id": "pool_migration",
                "requested_vcpu": 1,
                "requested_memory": 2,
                "requested_disk": 3,
            },
        )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE task SET status = 'ERROR', finished_at = :finished_at WHERE id = :id"),
            {"id": task_row_id, "finished_at": attempt_started_at + timedelta(seconds=1)},
        )
        remaining = connection.execute(
            text("SELECT count(*) FROM sandboxbuildreservation WHERE task_row_id = :id"), {"id": task_row_id}
        ).scalar_one()
        assert remaining == 1

    migrate("downgrade", _SANDBOX_BUILD_RESERVATION_PREDECESSOR)
    assert "sandboxbuildreservation" not in inspect(engine).get_table_names()
    engine.dispose()


class TestReservedAdmissionMigration:
    """The combined migration preserves the head revision and supports admission writes."""

    @pytest.fixture
    def database(self, migration_database_url: str) -> Generator[Engine, None, None]:
        predecessor = _run_alembic(migration_database_url, "upgrade", _RESERVED_ADMISSION_PREDECESSOR)
        assert predecessor.returncode == 0, predecessor.stderr
        engine = create_engine(migration_database_url)
        try:
            yield engine
        finally:
            engine.dispose()

    @pytest.fixture
    def admission_rows(self, database: Engine) -> tuple[list[Task], ExecutorDispatch]:
        org_id = uuid4()
        with database.begin() as connection:
            connection.execute(
                text("INSERT INTO org (id, name) VALUES (:id, :name)"),
                {"id": org_id, "name": f"migration-admission-{org_id}"},
            )
        with Session(database, expire_on_commit=False) as session:
            release = ExecutorRelease(
                id="migration-admission-release",
                artifact_uri="s3://artifacts/migration-admission.pex",
                artifact_digest="a" * 64,
                protocol_version="test",
            )
            benchmark = Benchmark(
                org_id=org_id,
                name="reserved admission migration",
                arguments=AWSBenchmarkArguments(
                    contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
                    concurrency=3,
                    queue_pool_id="pool_migration",
                ),
            )
            # This historical schema predates the release task-definition column.
            session.execute(
                text(
                    "INSERT INTO executorrelease"
                    " (id, artifact_uri, artifact_digest, protocol_version, status, readiness_verified,"
                    " readiness_metadata, created_at)"
                    " VALUES (:id, :uri, :digest, :protocol, 'CANDIDATE', true, '{}', :created_at)"
                ),
                {
                    "id": release.id,
                    "uri": release.artifact_uri,
                    "digest": release.artifact_digest,
                    "protocol": release.protocol_version,
                    "created_at": release.created_at,
                },
            )
            session.add(benchmark)
            session.flush()
            dispatch = ExecutorDispatch(
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                status=ExecutorDispatchStatus.RUNNING,
                executor_release_id=release.id,
                executor_artifact_uri=release.artifact_uri,
                executor_artifact_digest=release.artifact_digest,
                executor_protocol_version=release.protocol_version,
            )
            tasks = [
                Task(
                    org_id=org_id,
                    task_id=name,
                    benchmark=benchmark.id,
                    status=status,
                    started_at=datetime(2026, 10, 16, 12),
                )
                for name, status in (
                    ("building", TaskStatus.BUILDING),
                    ("ready", TaskStatus.PENDING),
                    ("rejected", TaskStatus.PENDING),
                )
            ]
            session.add(dispatch)
            session.execute(
                text(
                    "INSERT INTO task (id, org_id, task_id, status, started_at, benchmark) "
                    "VALUES (:id, :org_id, :task_id, :status, :started_at, :benchmark)"
                ),
                [
                    {
                        "id": task.id,
                        "org_id": task.org_id,
                        "task_id": task.task_id,
                        "status": task.status.value,
                        "started_at": task.started_at,
                        "benchmark": task.benchmark,
                    }
                    for task in tasks
                ],
            )
            session.commit()

            return tasks, dispatch

    @pytest.mark.parametrize("populated", [False, True], ids=["empty-ledger", "pre-domain-ledger"])
    def test_upgrade_and_downgrade(
        self,
        migration_database_url: str,
        database: Engine,
        admission_rows: tuple[list[Task], ExecutorDispatch],
        populated: bool,
    ) -> None:
        tasks, dispatch = admission_rows
        building, ready, rejected = tasks
        original_columns = {
            column["name"]: (str(column["type"]), column["nullable"], column["default"])
            for column in inspect(database).get_columns("sandboxbuildreservation")
        }
        if populated:
            with database.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO sandboxbuildreservation "
                        "(task_row_id, attempt_started_at, reserved_at, pool_id, "
                        "requested_vcpu, requested_memory, requested_disk) "
                        "VALUES (:id, :attempt, :attempt, 'pool_migration', 1, 2, 20)"
                    ),
                    {"id": building.id, "attempt": building.started_at},
                )

        upgrade = _run_alembic(migration_database_url, "upgrade", _RESERVED_ADMISSION_REVISION)
        assert upgrade.returncode == 0, upgrade.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM sandboxbuildreservation")).scalar_one() == 0
        with Session(database) as session:
            for attempt, released_at in (
                (building.started_at - timedelta(minutes=1), building.started_at),
                (building.started_at, None),
            ):
                session.add(
                    SandboxBuildReservation(
                        task_row_id=building.id,
                        attempt_started_at=attempt,
                        reserved_at=attempt,
                        released_at=released_at,
                        pool_id="pool_migration",
                        target_id="region-a",
                        sandbox_class="sysbox",
                        requested_vcpu=1.25,
                        requested_memory=2.25,
                        requested_disk=20.25,
                        requested_gpu=0,
                    )
                )
            session.add_all(
                [
                    SandboxAdmissionCandidate(
                        task_row_id=ready.id,
                        attempt_started_at=ready.started_at,
                        executor_dispatch_id=dispatch.id,
                        admission_ready=True,
                        pool_id="pool_migration",
                        target_id="region-a",
                        sandbox_class="sysbox",
                        requested_vcpu=1.25,
                        requested_memory=2.25,
                        requested_disk=20.25,
                        requested_gpu=0,
                    ),
                    SandboxAdmissionCandidate(
                        task_row_id=rejected.id,
                        attempt_started_at=rejected.started_at,
                        executor_dispatch_id=dispatch.id,
                        admission_ready=False,
                        pool_id="pool_migration",
                        target_id="region-b",
                        sandbox_class="sysbox",
                        requested_vcpu=0,
                        requested_memory=0,
                        requested_disk=0,
                        requested_gpu=1,
                    ),
                ]
            )
            session.commit()

        # Rows written after the reserved-admission upgrade survive later head migrations.
        current_head = _run_alembic(migration_database_url, "upgrade", "head")
        assert current_head.returncode == 0, current_head.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()
            )
            reservations = connection.execute(
                text(
                    "SELECT attempt_started_at, released_at, requested_vcpu, requested_memory, requested_disk "
                    "FROM sandboxbuildreservation ORDER BY attempt_started_at"
                )
            ).all()
            candidates = connection.execute(
                text(
                    "SELECT task_row_id, admission_ready, target_id, sandbox_class, "
                    "requested_vcpu, requested_memory, requested_disk, requested_gpu "
                    "FROM sandboxadmissioncandidate"
                )
            ).all()

        assert [tuple(row) for row in reservations] == [
            (building.started_at - timedelta(minutes=1), building.started_at, 1.25, 2.25, 20.25),
            (building.started_at, None, 1.25, 2.25, 20.25),
        ]
        assert {row[0]: tuple(row[1:]) for row in candidates} == {
            ready.id: (True, "region-a", "sysbox", 1.25, 2.25, 20.25, 0),
            rejected.id: (False, "region-b", "sysbox", 0, 0, 0, 1),
        }

        downgrade = _run_alembic(migration_database_url, "downgrade", _RESERVED_ADMISSION_PREDECESSOR)
        assert downgrade.returncode == 0, downgrade.stderr

        inspector = inspect(database)
        assert {
            column["name"]: (str(column["type"]), column["nullable"], column["default"])
            for column in inspector.get_columns("sandboxbuildreservation")
        } == original_columns
        assert inspector.get_pk_constraint("sandboxbuildreservation")["constrained_columns"] == ["task_row_id"]
        assert "sandboxadmissioncandidate" not in inspector.get_table_names()
        with database.connect() as connection:
            remaining = connection.execute(
                text(
                    "SELECT task_row_id, attempt_started_at, requested_vcpu, requested_memory, requested_disk "
                    "FROM sandboxbuildreservation"
                )
            ).all()

        assert [tuple(row) for row in remaining] == [(building.id, building.started_at, 1, 2, 20)]

        repeated_upgrade = _run_alembic(migration_database_url, "upgrade", "head")
        assert repeated_upgrade.returncode == 0, repeated_upgrade.stderr

        with database.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM sandboxbuildreservation")).scalar_one() == 0
            statuses = {
                task_id: status for task_id, status in connection.execute(text("SELECT id, status FROM task")).all()
            }

        assert statuses == {
            building.id: TaskStatus.BUILDING,
            ready.id: TaskStatus.PENDING,
            rejected.id: TaskStatus.PENDING,
        }


def test_org_bucket_migration_records_the_deployment_bucket_for_vals_only(
    migration_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Vals keeps the bucket it already uses; Tracker creates every other organization's bucket later."""
    monkeypatch.setattr(tracker_config, "AWS_DEPLOYMENT_S3_BUCKET", "agentic-harness-test")
    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_PREDECESSOR)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO org (id, name) VALUES (:id, :name)"),
            [{"id": uuid4(), "name": "vals.ai"}, {"id": uuid4(), "name": "acme.example"}],
        )

    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_REVISION)

    assert upgrade.returncode == 0, upgrade.stderr
    with engine.connect() as connection:
        buckets = {row.name: row.storage_key for row in connection.execute(text("SELECT name, storage_key FROM org"))}
    assert buckets["vals.ai"] == "agentic-harness-test"
    assert buckets["acme.example"] is None
    downgrade = _run_alembic(migration_database_url, "downgrade", _ORG_BUCKET_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    engine.dispose()


def test_dropping_aws_managed_downgrades_with_every_aws_run_marked_managed(migration_database_url: str) -> None:
    """The flag is gone after upgrade; downgrade restores it so the previous Tracker can still resume AWS runs."""
    upgrade = _run_alembic(migration_database_url, "upgrade", _ORG_BUCKET_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    org_id = uuid4()
    contract = AgentContractRequest(name="migration-test-agent", install_cmd="true", run_cmd="true")
    runs = {
        "aws": AWSBenchmarkArguments(contract=contract, concurrency=1).model_dump_json(),
        "local": LocalBenchmarkArguments(
            contract=contract,
            concurrency=1,
            properties=LocalResources(data_root=Path("/tmp/valkyrie")),
            sandbox_provider="docker",
        ).model_dump_json(),
    }
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": "org"})
        for name, arguments in runs.items():
            connection.execute(
                text(
                    "INSERT INTO benchmark"
                    " (id, org_id, name, started_at, finished_at, status, arguments, docent_reading_status)"
                    " VALUES (:id, :org_id, :name, now(), now(), CAST(:status AS benchmarkstatus),"
                    " CAST(:arguments AS json), CAST('IDLE' AS docentreadingstatus))"
                ),
                {
                    "id": uuid4(),
                    "org_id": org_id,
                    "name": name,
                    "status": BenchmarkStatus.FINISHED.value,
                    "arguments": arguments,
                },
            )

    upgrade = _run_alembic(migration_database_url, "upgrade", _DROP_AWS_MANAGED_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    assert "aws_managed" not in {column["name"] for column in inspect(engine).get_columns("benchmark")}

    downgrade = _run_alembic(migration_database_url, "downgrade", _ORG_BUCKET_REVISION)
    assert downgrade.returncode == 0, downgrade.stderr
    with engine.connect() as connection:
        flags = dict(connection.execute(text("SELECT name, aws_managed FROM benchmark")).all())
    assert flags == {"aws": True, "local": False}
    engine.dispose()


def test_project_migration_matches_the_models_and_downgrades(migration_database_url: str) -> None:
    """Project handles are unique per org while names may repeat, a run's last-appended link is its project even when link timestamps tie, and downgrade removes both tables."""
    upgrade = _run_alembic(migration_database_url, "upgrade", _PROJECT_REVISION)
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_engine(migration_database_url)
    inspector = inspect(engine)
    for table in ("project", "run_project"):
        assert {column["name"]: column["nullable"] for column in inspector.get_columns(table)} == {
            column.name: column.nullable for column in SQLModel.metadata.tables[table].columns
        }
    assert [
        (constraint["name"], constraint["column_names"]) for constraint in inspector.get_unique_constraints("project")
    ] == [("unique_project_handle_per_org", ["org_id", "handle"])]
    assert [(index["name"], index["column_names"]) for index in inspector.get_indexes("run_project")] == [
        ("ix_run_project_benchmark_id", ["benchmark_id"])
    ]
    assert [
        (foreign_key["constrained_columns"], foreign_key["referred_table"], foreign_key["referred_columns"])
        for foreign_key in inspector.get_foreign_keys("project")
    ] == [(["org_id"], "org", ["id"])]
    assert sorted(
        (foreign_key["constrained_columns"], foreign_key["referred_table"], foreign_key["referred_columns"])
        for foreign_key in inspector.get_foreign_keys("run_project")
    ) == [(["benchmark_id"], "benchmark", ["id"]), (["project_id"], "project", ["id"])]
    assert [column["name"] for column in inspector.get_columns("run_project") if column.get("identity")] == ["id"]

    org_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO org (id, name) VALUES (:id, :name)"), {"id": org_id, "name": "org"})
    with Session(engine) as session:
        run, reversed_run = (
            Benchmark(
                org_id=org_id,
                name="projects",
                arguments=AWSBenchmarkArguments(
                    contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"), concurrency=1
                ),
            )
            for _ in range(2)
        )
        router_bench = Project(
            org_id=org_id,
            handle="router-bench",
            name="Router Bench",
            spend_type=SpendType.DEVELOPMENT,
            subcategory="new_benchmark_generation",
        )
        google_report = Project(
            org_id=org_id,
            handle="google-report-xyz",
            name="Google Report XYZ",
            spend_type=SpendType.CUSTOMER,
            subcategory="report",
            customer="google",
            extra_tags={"engagement": "q4"},
        )
        session.add_all([run, reversed_run, router_bench, google_report])
        session.flush()
        moved_at = datetime.now(UTC)
        session.add_all(
            [
                RunProject(benchmark_id=run.id, project_id=router_bench.id, created_at=moved_at),
                RunProject(benchmark_id=run.id, project_id=google_report.id, created_at=moved_at),
                RunProject(benchmark_id=reversed_run.id, project_id=google_report.id, created_at=moved_at),
                RunProject(benchmark_id=reversed_run.id, project_id=router_bench.id, created_at=moved_at),
            ]
        )
        session.commit()
        reversed_current = reversed_run.fetch_project(session)
        assert reversed_current is not None
        assert reversed_current.handle == "router-bench"
        newest = run.fetch_project(session)
        assert newest is not None
        assert (newest.handle, newest.name, newest.spend_type, newest.extra_tags) == (
            "google-report-xyz",
            "Google Report XYZ",
            SpendType.CUSTOMER,
            {"engagement": "q4"},
        )
        session.add(
            Project(
                org_id=org_id,
                handle="router-bench-2",
                name="Router Bench",
                spend_type=SpendType.INTERNAL,
                subcategory="blog",
            )
        )
        session.commit()
        session.add(
            Project(
                org_id=org_id,
                handle="router-bench",
                name="Another name",
                spend_type=SpendType.INTERNAL,
                subcategory="blog",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

    downgrade = _run_alembic(migration_database_url, "downgrade", _PROJECT_PREDECESSOR)
    assert downgrade.returncode == 0, downgrade.stderr
    assert {"project", "run_project"}.isdisjoint(inspect(engine).get_table_names())
    with engine.connect() as connection:
        assert connection.execute(text("SELECT 1 FROM pg_type WHERE typname = 'spendtype'")).first() is None
    engine.dispose()
