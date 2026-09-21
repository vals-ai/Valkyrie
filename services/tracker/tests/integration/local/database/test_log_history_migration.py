"""Validate the additive history column against a disposable PostgreSQL database."""

import asyncio
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import ValidationError
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlmodel import Session, col, create_engine, select
from testcontainers.postgres import PostgresContainer

from tests.integration.local.database.test_run_purge import contract, prepared_operator
from tracker.database.models import Benchmark, RunLifecycle
from tracker.run_purge import PurgeOperator, build_plan
from tracker.runtime.log_history_reference import LogHistoryReference


def test_log_history_additive_migration_and_typed_storage(postgres_container: PostgresContainer) -> None:
    """The disposable container always supplies an administrator, so this can never be skipped."""
    tracker_root = Path(__file__).resolve().parents[4]
    supplied = os.environ.get("TRACKER_HISTORY_TEST_ADMIN_URL") or postgres_container.get_connection_url()
    admin_url = make_url(supplied).set(database="postgres")
    database_name = f"tracker_history_{uuid4().hex}"
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{database_name}"'))

    database_url = admin_url.set(database=database_name)
    engine = create_engine(database_url)
    run_id, org_id, operation_id = uuid4(), uuid4(), uuid4()
    try:

        def upgrade(revision: str) -> None:
            result = subprocess.run(
                ["uv", "run", "--no-sync", "alembic", "upgrade", revision],
                cwd=tracker_root,
                env={
                    **os.environ,
                    "DATABASE_URL": database_url.render_as_string(hide_password=False).replace("%2F", "/"),
                },
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr

        upgrade("8c9d0e1f2a3b")
        with engine.begin() as connection:
            connection.execute(text("INSERT INTO org (id, name) VALUES (:id, 'history-migration')"), {"id": org_id})
            connection.execute(
                text(
                    "INSERT INTO benchmark (id, org_id, name, started_at, status) VALUES (:id, :org, 'legacy', '2020-01-01', 'IN_PROGRESS')"
                ),
                {"id": run_id, "org": org_id},
            )

        upgrade("head")
        assert ScriptDirectory.from_config(Config(str(tracker_root / "alembic.ini"))).get_heads() == ["0e1f2a3b4c5d"]
        columns = {column["name"]: column for column in inspect(engine).get_columns("benchmark")}
        assert columns["log_history"]["nullable"]
        with engine.connect() as connection:
            row = connection.execute(
                text("SELECT log_history, started_at FROM benchmark WHERE id=:id"), {"id": run_id}
            ).one()
            assert row[0] is None and row[1].year == 2020

        reference = LogHistoryReference.model_validate(
            {
                "run_id": str(run_id),
                "operation_id": str(operation_id),
                "parent_plan_sha256": "a" * 64,
                "manifest": {
                    "key": f"benchmarks/{run_id}/log-history/{operation_id}/v1/manifest.json",
                    "version_id": "immutable",
                    "sha256": "b" * 64,
                    "size_bytes": 512,
                },
            }
        )
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE benchmark SET log_history=CAST(:value AS JSON) WHERE id=:id"),
                {"value": reference.model_dump_json(), "id": run_id},
            )
        with Session(engine) as session:
            assert session.exec(select(col(Benchmark.log_history)).where(Benchmark.id == run_id)).one() == reference

        with engine.begin() as connection:
            connection.execute(text("UPDATE benchmark SET log_history='{}'::json WHERE id=:id"), {"id": run_id})
        with Session(engine) as session, pytest.raises(ValidationError):
            session.exec(select(col(Benchmark.log_history)).where(Benchmark.id == run_id)).one()

        async def purge_customer_reference() -> None:
            with Session(engine) as session:
                operator, boundary = prepared_operator(session)
                target_id = operator.plan.runs[0].scope.run_id
                target = session.get(Benchmark, target_id)
                assert target is not None
                target.log_history = LogHistoryReference(
                    run_id=target_id,
                    operation_id=operation_id,
                    parent_plan_sha256="a" * 64,
                    manifest=reference.manifest.model_copy(
                        update={"key": f"benchmarks/{target_id}/log-history/{operation_id}/v1/manifest.json"}
                    ),
                )
                session.add(target)
                session.commit()
                operator = PurgeOperator(
                    session, build_plan(session, operator.plan.identity), boundary, host_contract=contract()
                )
                await operator.prepare()
                boundary.fenced = True
                await operator.purge()
                assert session.get(Benchmark, target_id) is None
                retained = session.get(RunLifecycle, target_id)
                assert retained is not None and retained.phase == "complete"
                assert "log_history" not in retained.model_dump_json()
                assert "immutable" not in retained.model_dump_json()
                assert "manifest.json" not in retained.model_dump_json()

        asyncio.run(purge_customer_reference())
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{database_name}"'))
        admin.dispose()
