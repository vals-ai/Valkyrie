"""Unit tests for tracker database model behavior.

Run: uv run pytest tests/unit/database/test_models.py
"""

from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkArgumentsType,
    Task,
    TaskStatus,
)


def test_direct_benchmark_storage_omits_scheduler_fields(database_session: Session) -> None:
    stored = BenchmarkArgumentsType().process_bind_param(
        BenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
            concurrency=5,
        ),
        database_session.get_bind().dialect,
    )

    assert stored is not None
    assert "priority" not in stored
    assert "queue_pool_id" not in stored


def test_create_benchmark_table_row_counts_stopped_tasks_as_finished(database_session: Session) -> None:
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
            concurrency=1,
        ),
    )
    database_session.add(benchmark)
    database_session.commit()

    for task_id, status in (
        ("finished", TaskStatus.FINISHED),
        ("errored", TaskStatus.ERROR),
        ("stopped", TaskStatus.STOPPED),
        ("pending", TaskStatus.PENDING),
    ):
        database_session.add(Task(org_id=TEST_ORG_ID, benchmark=benchmark.id, task_id=task_id, status=status))
    database_session.commit()

    row = benchmark.create_benchmark_table_row(database_session)

    assert row.total_tasks == 4
    assert row.finished_tasks == 3
