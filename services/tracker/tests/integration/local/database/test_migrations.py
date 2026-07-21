"""PostgreSQL tests for operational Alembic migration contracts."""

import os
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path
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
_CURRENT_OWNERSHIP_REVISION = "e9f0a1b2c3d4"
_PREVIOUS_REVISION = "d8e9f0a1b2c3"


@pytest.fixture(scope="module")
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
