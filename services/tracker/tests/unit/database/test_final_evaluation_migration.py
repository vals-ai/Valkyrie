import runpy
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest


def _migration() -> dict[str, Any]:
    path = (
        Path(__file__).parents[3] / "src/tracker/database/migrations/versions/2d7a4c9e1b30_unique_final_evaluation.py"
    )
    return runpy.run_path(str(path))


def test_migration_reports_duplicates_before_schema_change(monkeypatch: pytest.MonkeyPatch) -> None:
    migration = _migration()
    connection = Mock()
    connection.execute.return_value.all.return_value = [("benchmark-a", 2), ("benchmark-b", 3)]
    create_unique_constraint = Mock()
    monkeypatch.setattr(migration["op"], "get_bind", lambda: connection)
    monkeypatch.setattr(migration["op"], "create_unique_constraint", create_unique_constraint)

    with pytest.raises(RuntimeError, match=r"benchmark-a \(2 rows\), benchmark-b \(3 rows\)"):
        migration["upgrade"]()

    create_unique_constraint.assert_not_called()


def test_migration_adds_uniqueness_after_clean_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    migration = _migration()
    connection = Mock()
    connection.execute.return_value.all.return_value = []
    create_unique_constraint = Mock()
    monkeypatch.setattr(migration["op"], "get_bind", lambda: connection)
    monkeypatch.setattr(migration["op"], "create_unique_constraint", create_unique_constraint)

    migration["upgrade"]()

    create_unique_constraint.assert_called_once_with(
        "unique_final_evaluation_per_benchmark",
        "finalevaluation",
        ["benchmark"],
    )
