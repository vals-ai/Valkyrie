"""Unit tests for organization-scoped database queries.

Run: uv run pytest tests/unit/database/test_scoping.py
"""

from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Benchmark, Org
from tracker.database.scoping import assert_org, scoped_select


def _make_org(name: str = "test-org") -> Org:
    return Org(id=uuid4(), name=name)


class TestScopedSelect:
    """Organization-scoped database queries."""

    def test_scoped_select_filters_by_org(self, empty_database_session: Session) -> None:
        org_a = _make_org("org-a")
        org_b = _make_org("org-b")
        empty_database_session.add_all([org_a, org_b])
        empty_database_session.flush()

        bench_a = make_benchmark("bench-a", org_id=org_a.id)
        bench_b = make_benchmark("bench-b", org_id=org_b.id)
        empty_database_session.add_all([bench_a, bench_b])
        empty_database_session.commit()

        results = empty_database_session.exec(scoped_select(Benchmark, org_a)).all()
        assert len(results) == 1
        assert results[0].name == "bench-a"

    def test_scoped_select_returns_empty_for_wrong_org(self, empty_database_session: Session) -> None:
        org_a = _make_org("org-a")
        org_b = _make_org("org-b")
        empty_database_session.add_all([org_a, org_b])
        empty_database_session.flush()

        bench_a = make_benchmark(org_id=org_a.id)
        empty_database_session.add(bench_a)
        empty_database_session.commit()

        results = empty_database_session.exec(scoped_select(Benchmark, org_b)).all()
        assert len(results) == 0


class TestAssertOrg:
    """Organization ownership checks for database rows."""

    def test_assert_org_returns_row_for_matching_org(self) -> None:
        org = _make_org()
        bench = make_benchmark(org_id=org.id)
        result = assert_org(bench, org)
        assert result is bench

    def test_assert_org_raises_404_for_none(self) -> None:
        org = _make_org()
        with pytest.raises(HTTPException) as exc_info:
            assert_org(None, org)
        assert exc_info.value.status_code == 404

    def test_assert_org_raises_404_for_wrong_org(self) -> None:
        org_a = _make_org("org-a")
        org_b = _make_org("org-b")
        bench = make_benchmark(org_id=org_a.id)
        with pytest.raises(HTTPException) as exc_info:
            assert_org(bench, org_b)
        assert exc_info.value.status_code == 404
