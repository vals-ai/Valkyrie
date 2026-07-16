"""Local Postgres schema integration tests."""

from sqlalchemy.engine import Engine
from sqlmodel import inspect


def test_tracker_schema_contains_core_tables(postgres_engine: Engine) -> None:
    """Migrations and models must create the tracker persistence boundary.

    Test cases:
    - Disposable Postgres contains the benchmark and task tables.
    - Evaluation and final-score tables are also present.
    """
    tables = set(inspect(postgres_engine).get_table_names())

    assert {"benchmark", "task", "evaluationresult", "finalevaluation"} <= tables
