"""Run with `uv run pytest tests/integration/local/database/test_schema.py`.

Exercise schema and model events against disposable Postgres.
"""

import re
from typing import Any, cast

from sqlalchemy import CheckConstraint, Enum as SAEnum, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, Session, inspect

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)


def _normalized_sql(expression: str) -> str:
    normalized = expression.replace('"', "")
    normalized = re.sub(r"::[A-Za-z_][A-Za-z0-9_]*", "", normalized)
    normalized = normalized.replace("<>", "!=").replace("(", "").replace(")", "")
    return " ".join(normalized.split())


def test_task_attempt_failure_history_schema_matches_metadata(postgres_engine: Engine) -> None:
    inspector = inspect(postgres_engine)
    table_names = set(inspector.get_table_names())
    expected_tables = {"taskattempt", "failurerecord"}
    assert expected_tables <= table_names
    assert "errorresult" not in table_names

    for table_name in expected_tables:
        metadata_table = SQLModel.metadata.tables[table_name]
        actual_columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        assert set(actual_columns) == {column.name for column in metadata_table.columns}
        assert {column_name: column.nullable for column_name, column in metadata_table.columns.items()} == {
            column_name: column["nullable"] for column_name, column in actual_columns.items()
        }

        expected_indexes = {
            (index.name, tuple(column.name for column in index.columns)) for index in metadata_table.indexes
        }
        actual_indexes = {
            (index["name"], tuple(index["column_names"]))
            for index in inspector.get_indexes(table_name)
            if index["name"]
        }
        assert actual_indexes == expected_indexes

        expected_unique_constraints = {
            tuple(column.name for column in constraint.columns)
            for constraint in metadata_table.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        expected_unique_constraints.update((column.name,) for column in metadata_table.columns if column.unique)
        actual_unique_constraints = {
            tuple(constraint["column_names"]) for constraint in inspector.get_unique_constraints(table_name)
        }
        assert actual_unique_constraints == expected_unique_constraints

        expected_checks = {
            constraint.name: _normalized_sql(str(constraint.sqltext))
            for constraint in metadata_table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        actual_checks = {
            constraint["name"]: _normalized_sql(constraint["sqltext"])
            for constraint in inspector.get_check_constraints(table_name)
        }
        assert actual_checks == expected_checks

        expected_primary_key = tuple(column.name for column in metadata_table.primary_key.columns)
        actual_primary_key = tuple(inspector.get_pk_constraint(table_name)["constrained_columns"])
        assert actual_primary_key == expected_primary_key

        expected_foreign_keys = {
            (
                tuple(constraint.column_keys),
                constraint.elements[0].target_fullname.split(".")[0],
                tuple(element.column.name for element in constraint.elements),
            )
            for constraint in metadata_table.constraints
            if isinstance(constraint, ForeignKeyConstraint)
        }
        actual_foreign_keys = {
            (
                tuple(foreign_key["constrained_columns"]),
                foreign_key["referred_table"],
                tuple(foreign_key["referred_columns"]),
            )
            for foreign_key in inspector.get_foreign_keys(table_name)
        }
        assert actual_foreign_keys == expected_foreign_keys

    for table_name, column_name in (
        ("task", "active_attempt_id"),
        ("evaluationresult", "task_attempt_id"),
    ):
        columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        assert columns[column_name]["nullable"] is True
        assert any(tuple(index["column_names"]) == (column_name,) for index in inspector.get_indexes(table_name))
        assert any(
            tuple(foreign_key["constrained_columns"]) == (column_name,)
            and foreign_key["referred_table"] == "taskattempt"
            and tuple(foreign_key["referred_columns"]) == ("id",)
            for foreign_key in inspector.get_foreign_keys(table_name)
        )

    expected_enums: dict[str, tuple[str, ...]] = {}
    for table_name in expected_tables:
        for column in SQLModel.metadata.tables[table_name].columns:
            if isinstance(column.type, SAEnum) and column.type.name:
                expected_enums[column.type.name] = tuple(column.type.enums)
    actual_enums = {
        enum["name"]: tuple(enum["labels"])
        for enum in cast(Any, inspector).get_enums()
        if enum["name"] in expected_enums
    }
    assert actual_enums == expected_enums


class TestTrackerSchema:
    """Tracker schema tables and database status hooks."""

    def test_tracker_schema_contains_core_tables(self, postgres_engine: Engine) -> None:
        """Migrations and models must create the tracker persistence boundary.

        Test cases:
        - Disposable Postgres contains the benchmark and task tables.
        - Evaluation and final-score tables are also present.
        """
        tables = set(inspect(postgres_engine).get_table_names())

        assert {"benchmark", "task", "evaluationresult", "finalevaluation"} <= tables

    def test_current_execution_release_is_nullable_indexed_foreign_key(self, postgres_engine: Engine) -> None:
        inspector = inspect(postgres_engine)
        columns = {column["name"]: column for column in inspector.get_columns("benchmark")}
        assert columns["current_execution_release_id"]["nullable"] is True

        foreign_keys = inspector.get_foreign_keys("benchmark")
        assert any(
            foreign_key["constrained_columns"] == ["current_execution_release_id"]
            and foreign_key["referred_table"] == "executorrelease"
            and foreign_key["referred_columns"] == ["id"]
            for foreign_key in foreign_keys
        )
        indexes = inspector.get_indexes("benchmark")
        assert any(index["column_names"] == ["current_execution_release_id"] for index in indexes)

    def test_terminal_statuses_set_finished_timestamps_in_postgres(self, postgres_session: Session) -> None:
        """Terminal state transitions must persist completion timestamps in production Postgres.

        Test cases:
        - Finishing a benchmark sets its completion timestamp.
        - Marking a task as errored sets its completion timestamp.
        """
        postgres_session.add(Org(id=TEST_ORG_ID, name="postgres-status-org"))
        postgres_session.flush()

        benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="postgres-status-events",
            arguments=BenchmarkArguments(
                contract=AgentContractRequest(name="status-agent", install_cmd="true", run_cmd="true"),
                concurrency=1,
            ),
        )
        postgres_session.add(benchmark)
        postgres_session.flush()
        assert benchmark.finished_at is None

        benchmark.status = BenchmarkStatus.FINISHED
        postgres_session.add(benchmark)
        postgres_session.flush()
        assert benchmark.finished_at is not None

        task = Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id="status-task")
        postgres_session.add(task)
        postgres_session.flush()
        assert task.finished_at is None

        task.status = TaskStatus.ERROR
        postgres_session.add(task)
        postgres_session.flush()
        assert task.finished_at is not None
