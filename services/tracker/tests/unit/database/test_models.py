"""Unit tests for tracker database model behavior.

Run: uv run pytest tests/unit/database/test_models.py
"""

import pytest
from pydantic import ValidationError
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    OutputArtifact,
    Task,
    TaskStatus,
)


def test_required_output_artifact_omits_default_from_serialized_contract() -> None:
    artifact = OutputArtifact(path="logs/result.json")

    assert artifact.model_dump(mode="json") == {
        "path": "logs/result.json",
        "source": None,
    }


@pytest.mark.parametrize(
    "artifacts",
    [
        [
            "artifacts/result.json",
            OutputArtifact(
                path="artifacts//result.json",
                source="/logs/optional.json",
                required=False,
            ),
        ],
        [
            OutputArtifact(
                path="telemetry/result.json",
                source="/logs/first.json",
                required=False,
            ),
            OutputArtifact(
                path="telemetry//result.json",
                source="/logs/second.json",
                required=False,
            ),
        ],
    ],
    ids=["required-optional", "optional-optional"],
)
def test_agent_contract_rejects_duplicate_normalized_output_artifact_paths(
    artifacts: list[str | OutputArtifact],
) -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        AgentContractRequest(name="agent", output_artifacts=artifacts)


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
