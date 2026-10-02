"""Run list fields come from stored arguments without more queries."""

from benchmark_service.schemas import DatasetVersion
import pytest
from sqlalchemy import event
from sqlalchemy.dialects.sqlite import dialect
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import BenchmarkArgumentsType, Org
from tracker.types import FetchBenchmarksRequest
from tracker.utils.reporting import build_benchmark_table_rows, fetch_filtered_benchmark_rows


@pytest.mark.parametrize("batch", [True, False])
@pytest.mark.parametrize("priority", [0, 4])
def test_run_list_fields_present(database_session: Session, batch: bool, priority: int) -> None:
    benchmark = make_benchmark()
    version = DatasetVersion(id="release-2026-10-01", label="October release")
    benchmark.arguments = benchmark.arguments.model_copy(
        update={"sandbox_provider": "modal", "priority": priority, "dataset_version": version}
    )

    row = (
        build_benchmark_table_rows([benchmark], database_session)[0]
        if batch
        else benchmark.create_benchmark_table_row(database_session)
    )

    assert row.sandbox_provider == "modal"
    assert row.priority == priority
    assert row.dataset_version == version
    assert row.model_dump(mode="json")["dataset_version"] == {
        "id": "release-2026-10-01",
        "label": "October release",
    }


@pytest.mark.parametrize("batch", [True, False])
def test_run_list_fields_missing(database_session: Session, batch: bool) -> None:
    benchmark = make_benchmark()
    arguments = BenchmarkArgumentsType().process_result_value(
        {"contract": benchmark.arguments.contract.model_dump(), "concurrency": 1}, dialect()
    )
    assert arguments is not None
    benchmark.arguments = arguments

    row = (
        build_benchmark_table_rows([benchmark], database_session)[0]
        if batch
        else benchmark.create_benchmark_table_row(database_session)
    )

    assert row.sandbox_provider is None
    assert row.priority is None
    assert row.dataset_version is None
    assert benchmark.arguments.sandbox_provider == "daytona"
    body = row.model_dump(mode="json")
    assert body["sandbox_provider"] is None
    assert body["priority"] is None
    assert body["dataset_version"] is None


@pytest.mark.parametrize("run_count", [1, 25])
def test_run_list_query_count(database_session: Session, run_count: int) -> None:
    for _ in range(run_count):
        benchmark = make_benchmark()
        benchmark.arguments = benchmark.arguments.model_copy(
            update={
                "sandbox_provider": "modal",
                "priority": 2,
                "dataset_version": DatasetVersion(id="release-1", label=None),
            }
        )
        database_session.add(benchmark)
    database_session.commit()
    database_session.expire_all()
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    statements: list[str] = []

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    bind = database_session.get_bind()
    event.listen(bind, "before_cursor_execute", record_statement)
    try:
        benchmarks, total_count, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(limit=run_count), database_session, org
        )
        rows = build_benchmark_table_rows(benchmarks, database_session)
        bodies = [row.model_dump(mode="json") for row in rows]
    finally:
        event.remove(bind, "before_cursor_execute", record_statement)

    assert total_count == run_count
    assert len(statements) == 4
    assert len(bodies) == run_count
    assert all(body["sandbox_provider"] == "modal" for body in bodies)
    assert all(body["priority"] == 2 for body in bodies)
    assert all(body["dataset_version"] == {"id": "release-1", "label": None} for body in bodies)
