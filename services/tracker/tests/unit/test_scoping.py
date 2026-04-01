from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine

from tracker.database.models import Benchmark, BenchmarkArguments, AgentContractRequest, Org
from tracker.database.scoping import assert_org, scoped_select


@pytest.fixture
def session():
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _make_org(name: str = "test-org") -> Org:
    return Org(id=uuid4(), name=name)


def _make_benchmark(org: Org, name: str = "swebench") -> Benchmark:
    return Benchmark(
        name=name,
        org_id=org.id,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="echo", run_cmd="echo"),
            concurrency=1,
        ),
    )


def test_scoped_select_filters_by_org(session: Session):
    org_a = _make_org("org-a")
    org_b = _make_org("org-b")
    session.add_all([org_a, org_b])
    session.flush()

    bench_a = _make_benchmark(org_a, "bench-a")
    bench_b = _make_benchmark(org_b, "bench-b")
    session.add_all([bench_a, bench_b])
    session.commit()

    results = session.exec(scoped_select(Benchmark, org_a)).all()
    assert len(results) == 1
    assert results[0].name == "bench-a"


def test_scoped_select_returns_empty_for_wrong_org(session: Session):
    org_a = _make_org("org-a")
    org_b = _make_org("org-b")
    session.add_all([org_a, org_b])
    session.flush()

    bench_a = _make_benchmark(org_a)
    session.add(bench_a)
    session.commit()

    results = session.exec(scoped_select(Benchmark, org_b)).all()
    assert len(results) == 0


def test_assert_org_returns_row_for_matching_org():
    org = _make_org()
    bench = _make_benchmark(org)
    result = assert_org(bench, org)
    assert result is bench


def test_assert_org_raises_404_for_none():
    org = _make_org()
    with pytest.raises(HTTPException) as exc_info:
        assert_org(None, org)
    assert exc_info.value.status_code == 404


def test_assert_org_raises_404_for_wrong_org():
    org_a = _make_org("org-a")
    org_b = _make_org("org-b")
    bench = _make_benchmark(org_a)
    with pytest.raises(HTTPException) as exc_info:
        assert_org(bench, org_b)
    assert exc_info.value.status_code == 404
