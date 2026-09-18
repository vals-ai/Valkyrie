"""Exact row inventory and fail-closed PostgreSQL deletion."""

from uuid import UUID

from sqlalchemy import text
from sqlmodel import Session

from tracker.lifecycle import LifecycleConflict
from tracker.run_purge.contracts import RowScope

_TABLES = (
    "benchmark",
    "task",
    "evaluationresult",
    "errorresult",
    "finalevaluation",
    "executordispatch",
    "taskbreakdown",
)
_EXPECTED = {
    ("task", "benchmark", "benchmark", "id"),
    ("task", "task_breakdown", "taskbreakdown", "id"),
    ("evaluationresult", "task", "task", "id"),
    ("errorresult", "task", "task", "id"),
    ("finalevaluation", "benchmark", "benchmark", "id"),
    ("executordispatch", "benchmark_id", "benchmark", "id"),
}


def verify_foreign_keys(session: Session) -> None:
    if session.get_bind().dialect.name != "postgresql":
        raise LifecycleConflict("Purge requires PostgreSQL foreign key inspection")
    foreign_keys = (
        session.connection()
        .execute(
            text("""
        SELECT ns.nspname, source.relname, a.attname, nt.nspname, target.relname, b.attname,
               cardinality(c.conkey), cardinality(c.confkey)
        FROM pg_constraint c
        JOIN pg_class source ON source.oid=c.conrelid
        JOIN pg_namespace ns ON ns.oid=source.relnamespace
        JOIN pg_class target ON target.oid=c.confrelid
        JOIN pg_namespace nt ON nt.oid=target.relnamespace
        JOIN pg_attribute a ON a.attrelid=source.oid AND a.attnum=c.conkey[1]
        JOIN pg_attribute b ON b.attrelid=target.oid AND b.attnum=c.confkey[1]
        WHERE c.contype='f' AND target.relname = ANY(:tables)
    """),
            {"tables": list(_TABLES)},
        )
        .all()
    )
    found: set[tuple[str, str, str, str]] = set()
    for source_schema, source, column, target_schema, target, target_column, count, target_count in foreign_keys:
        edge = (source, column, target, target_column)
        if (
            source_schema != "public"
            or target_schema != "public"
            or count != 1
            or target_count != 1
            or edge not in _EXPECTED
        ):
            raise LifecycleConflict("Unknown foreign key blocks purge")
        found.add(edge)
    if found != _EXPECTED:
        raise LifecycleConflict("Expected foreign key graph is incomplete")


def inventory_rows(session: Session, run_id: UUID, org_id: UUID) -> tuple[RowScope, ...]:
    result: list[RowScope] = []
    selectors = {
        "benchmark": "id=:run_id",
        "task": "benchmark=:run_id",
        "finalevaluation": "benchmark=:run_id",
        "executordispatch": "benchmark_id=:run_id",
        "evaluationresult": "task IN (SELECT id FROM public.task WHERE benchmark=:run_id)",
        "errorresult": "task IN (SELECT id FROM public.task WHERE benchmark=:run_id)",
        "taskbreakdown": "id IN (SELECT task_breakdown FROM public.task WHERE benchmark=:run_id)",
    }
    for table in _TABLES:
        selector = selectors[table]
        ids = tuple(
            session.connection()
            .execute(text(f"SELECT id FROM public.{table} WHERE {selector} ORDER BY id"), {"run_id": run_id})
            .scalars()
            .all()
        )
        if table not in {"executordispatch", "taskbreakdown"}:
            mismatch = (
                session.connection()
                .execute(
                    text(f"SELECT count(*) FROM public.{table} WHERE {selector} AND org_id != :org_id"),
                    {"run_id": run_id, "org_id": org_id},
                )
                .scalar_one()
            )
            if mismatch:
                raise LifecycleConflict("Run child org does not match")
        result.append(RowScope.model_validate({"table": table, "ids": ids}))
    return tuple(result)


def delete_rows(session: Session, rows: tuple[RowScope, ...]) -> None:
    session.connection().execute(
        text("LOCK TABLE " + ", ".join(f"public.{table}" for table in _TABLES) + " IN SHARE ROW EXCLUSIVE MODE")
    )
    verify_foreign_keys(session)
    by_table = {row.table: row.ids for row in rows}
    for table in (
        "evaluationresult",
        "errorresult",
        "finalevaluation",
        "executordispatch",
        "task",
        "benchmark",
        "taskbreakdown",
    ):
        ids = by_table[table]
        if not ids:
            continue
        orphan = (
            " AND NOT EXISTS (SELECT 1 FROM public.task WHERE task_breakdown=taskbreakdown.id)"
            if table == "taskbreakdown"
            else ""
        )
        session.connection().execute(
            text(f"DELETE FROM public.{table} WHERE id = ANY(:ids){orphan}"), {"ids": list(ids)}
        )
    session.expire_all()


def verify_rows_absent(session: Session, rows: tuple[RowScope, ...]) -> None:
    for row in rows:
        if not row.ids:
            continue
        orphan = (
            " AND NOT EXISTS (SELECT 1 FROM public.task WHERE task_breakdown=taskbreakdown.id)"
            if row.table == "taskbreakdown"
            else ""
        )
        count = (
            session.connection()
            .execute(
                text(f"SELECT count(*) FROM public.{row.table} WHERE id = ANY(:ids){orphan}"), {"ids": list(row.ids)}
            )
            .scalar_one()
        )
        if count:
            raise LifecycleConflict("Scoped rows remain after purge")
