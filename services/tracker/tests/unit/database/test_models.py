"""Unit tests for tracker database model behavior.

Run: uv run pytest tests/unit/database/test_models.py
"""

from pydantic import BaseModel, ConfigDict
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


def test_benchmark_arguments_storage_omits_empty_scheduler_fields(database_session: Session) -> None:
    class LegacyBenchmarkArguments(BaseModel):
        model_config = ConfigDict(extra="forbid")

        contract: AgentContractRequest
        concurrency: int
        task_ids: list[str] | None
        slice_str: str | None
        lambda_function: str | None
        dataset: str | None
        sandbox_provider: str
        sandbox_provider_secret_name: str | None

    arguments_type = BenchmarkArgumentsType()
    contract = AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run")
    dialect = database_session.get_bind().dialect

    direct = arguments_type.process_bind_param(
        BenchmarkArguments(contract=contract, concurrency=5),
        dialect,
    )
    queued = arguments_type.process_bind_param(
        BenchmarkArguments(contract=contract, concurrency=5, priority=0, queue_pool_id="pool-id"),
        dialect,
    )

    assert direct is not None
    LegacyBenchmarkArguments.model_validate(direct)
    assert queued is not None
    assert queued["priority"] == 0
    assert queued["queue_pool_id"] == "pool-id"


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
