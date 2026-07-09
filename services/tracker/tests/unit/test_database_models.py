from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Task, TaskStatus


def test_agent_contract_request_defaults_old_payloads_without_changing_serialized_shape() -> None:
    contract = AgentContractRequest.model_validate(
        {
            "name": "legacy-agent",
            "install_cmd": "echo install",
            "run_cmd": "echo run",
            "secrets": {"API_KEY": "providerApiKeys"},
        }
    )

    assert contract.secret_bundles == []
    assert "secret_bundles" not in contract.model_dump()
    arguments = BenchmarkArguments(contract=contract, concurrency=1)
    assert "secret_bundles" not in arguments.model_dump()["contract"]


def test_benchmark_arguments_persist_nonempty_secret_bundles(database_session: Session) -> None:
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(
                name="agent",
                install_cmd="echo install",
                run_cmd="echo run",
                secret_bundles=["providerApiKeys"],
            ),
            concurrency=1,
        ),
    )
    database_session.add(benchmark)
    database_session.commit()
    database_session.expire_all()

    persisted = database_session.get(Benchmark, benchmark.id)

    assert persisted is not None
    assert persisted.arguments.contract.secret_bundles == ["providerApiKeys"]


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
