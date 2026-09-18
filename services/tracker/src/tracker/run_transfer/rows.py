"""Private stored-column closure. No ORM serialization or timestamp listeners."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from sqlalchemy import JSON, MetaData, Table, inspect, select
from sqlmodel import Session, SQLModel

from tracker.lifecycle import LifecycleConflict

TABLES = (
    "taskbreakdown",
    "benchmark",
    "task",
    "evaluationresult",
    "errorresult",
    "finalevaluation",
    "executordispatch",
)


def digest(value: object) -> str:
    def scalar(item: object) -> str:
        if isinstance(item, datetime):
            return item.isoformat()
        if isinstance(item, UUID):
            return str(item)
        raise TypeError("Unsupported stored value")

    return hashlib.sha256(
        json.dumps(value, default=scalar, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def tables(session: Session) -> dict[str, Table]:
    connection = session.connection()
    inspector = inspect(connection)
    metadata = MetaData()
    result: dict[str, Table] = {}
    supported = set(TABLES) | {"org", "executorrelease", "runlifecycle"}
    for name in supported:
        table = Table(name, metadata, autoload_with=connection)
        expected = SQLModel.metadata.tables[name]
        actual_columns = {
            (
                column.name,
                str(column.type.compile(dialect=connection.dialect)).replace("DOUBLE PRECISION", "FLOAT"),
                column.nullable,
                column.primary_key,
            )
            for column in table.columns
        }
        expected_columns = {
            (
                column.name,
                str(column.type.compile(dialect=connection.dialect)).replace("DOUBLE PRECISION", "FLOAT"),
                column.nullable,
                column.primary_key,
            )
            for column in expected.columns
        }
        actual_foreign_keys = {(key.parent.name, key.target_fullname) for key in table.foreign_keys}
        expected_foreign_keys = {(key.parent.name, key.target_fullname) for key in expected.foreign_keys}
        if actual_columns != expected_columns or actual_foreign_keys != expected_foreign_keys:
            raise LifecycleConflict("Unsupported stored schema")
        result[name] = table

    for name in inspector.get_table_names():
        for key in inspector.get_foreign_keys(name):
            if key["referred_table"] in TABLES and name not in TABLES:
                raise LifecycleConflict("Unknown reverse schema reference")
    return result


@dataclass
class RowClosure:
    rows: dict[str, list[dict[str, Any]]]
    sql_nulls: dict[str, list[list[str]]]

    @classmethod
    def read(cls, session: Session, run_id: UUID, org_id: UUID) -> "RowClosure":
        schema = tables(session)
        rows: dict[str, list[dict[str, Any]]] = {}
        nulls: dict[str, list[list[str]]] = {}

        def read(name: str, predicate: Any) -> list[dict[str, Any]]:
            table = schema[name]
            values = (
                session.connection()
                .execute(select(table).where(predicate).order_by(table.c.id).with_for_update())
                .mappings()
                .all()
            )
            rows[name] = [dict(value) for value in values]
            nulls[name] = []
            json_columns = [column for column in table.columns if isinstance(column.type, JSON)]
            for value in values:
                flags: Any = (
                    session.connection()
                    .execute(
                        select(*(column.is_(None).label(column.name) for column in json_columns)).where(
                            table.c.id == value["id"]
                        )
                    )
                    .mappings()
                    .one()
                    if json_columns
                    else {}
                )
                nulls[name].append(sorted(name for name, flag in cast(dict[str, bool], flags).items() if flag))
            return rows[name]

        benchmarks = read("benchmark", schema["benchmark"].c.id == run_id)
        if len(benchmarks) != 1 or benchmarks[0]["org_id"] != org_id:
            raise LifecycleConflict("Run is absent or outside organization")
        tasks = read("task", schema["task"].c.benchmark == run_id)
        task_ids = [task["id"] for task in tasks]
        breakdown_ids = [task["task_breakdown"] for task in tasks if task["task_breakdown"] is not None]
        read("taskbreakdown", schema["taskbreakdown"].c.id.in_(breakdown_ids))
        read("evaluationresult", schema["evaluationresult"].c.task.in_(task_ids))
        read("errorresult", schema["errorresult"].c.task.in_(task_ids))
        read("finalevaluation", schema["finalevaluation"].c.benchmark == run_id)
        read("executordispatch", schema["executordispatch"].c.benchmark_id == run_id)
        for values in rows.values():
            if any("org_id" in value and value["org_id"] != org_id for value in values):
                raise LifecycleConflict("Child organization differs")
        shared = (
            session.connection()
            .execute(
                select(schema["task"].c.id).where(
                    schema["task"].c.task_breakdown.in_(breakdown_ids), schema["task"].c.benchmark != run_id
                )
            )
            .first()
        )
        if shared is not None:
            raise LifecycleConflict("Breakdown has an unscoped reverse reference")
        return cls(rows, nulls)

    @property
    def sha256(self) -> str:
        return digest({"rows": self.rows, "sql_nulls": self.sql_nulls})

    def summary(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "tables": {
                name: {
                    "count": len(self.rows[name]),
                    "sha256": digest({"rows": self.rows[name], "sql_nulls": self.sql_nulls[name]}),
                }
                for name in TABLES
            },
        }

    def check_conflicts(self, session: Session) -> None:
        schema = tables(session)
        for name in TABLES:
            table = schema[name]
            ids = [row["id"] for row in self.rows[name]]
            if session.connection().execute(select(table.c.id).where(table.c.id.in_(ids))).first() is not None:
                raise LifecycleConflict("Destination primary key conflict")
        instances = [row["instance_id"] for row in self.rows["evaluationresult"] if row["instance_id"] is not None]
        table = schema["evaluationresult"]
        if (
            session.connection().execute(select(table.c.id).where(table.c.instance_id.in_(instances))).first()
            is not None
        ):
            raise LifecycleConflict("Destination global instance identity conflict")

    def insert(self, session: Session) -> None:
        self.check_conflicts(session)
        schema = tables(session)
        for name in TABLES:
            table = schema[name]
            for row, nulls in zip(self.rows[name], self.sql_nulls[name], strict=True):
                value = dict(row)
                for column in table.columns:
                    if isinstance(column.type, JSON):
                        column.type.none_as_null = True
                        if value[column.name] is None and column.name not in nulls:
                            value[column.name] = JSON.NULL
                session.connection().execute(table.insert().values(**value))

    def delete(self, session: Session) -> None:
        schema = tables(session)
        for name in reversed(TABLES):
            table = schema[name]
            session.connection().execute(table.delete().where(table.c.id.in_([row["id"] for row in self.rows[name]])))
