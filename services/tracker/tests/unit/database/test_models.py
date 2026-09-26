"""Unit tests for tracker database model behavior.

Run: uv run pytest tests/unit/database/test_models.py
"""

from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy.engine.default import DefaultDialect
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    AWSBenchmarkArguments,
    Benchmark,
    BenchmarkArgumentsType,
    LocalBenchmarkArguments,
    OutputArtifact,
    Task,
    TaskStatus,
)
from tracker.local.resources import LocalResources


def test_local_arguments_round_trip_preserves_resources_and_scheduler_fields() -> None:
    """Local resources and scheduler state survive the database JSON conversion."""
    arguments = LocalBenchmarkArguments(
        contract=AgentContractRequest(name="agent"),
        concurrency=2,
        properties=LocalResources(data_root=Path("/tmp/valkyrie"), secrets_file=Path("/tmp/secrets.env")),
        sandbox_provider="docker",
        priority=2,
        queue_pool_id="pool",
    )
    column = BenchmarkArgumentsType()
    stored = column.process_bind_param(arguments, DefaultDialect())

    assert stored is not None
    assert stored["properties"] == {
        "data_root": str(arguments.properties.data_root),
        "secrets_file": str(arguments.properties.secrets_file),
    }
    restored = column.process_result_value(stored, DefaultDialect())

    assert isinstance(restored, LocalBenchmarkArguments)
    assert restored == arguments
    assert restored.priority == 2
    assert restored.queue_pool_id == "pool"


def test_legacy_arguments_decode_as_aws_without_resource_defaults() -> None:
    """Rows predating environment and resource fields retain AWS fallback behavior."""
    restored = BenchmarkArgumentsType().process_result_value(
        {"contract": {"name": "agent"}, "concurrency": 5}, DefaultDialect()
    )

    assert isinstance(restored, AWSBenchmarkArguments)
    assert restored.environment == "aws"
    assert restored.properties is None


@pytest.mark.parametrize(
    "runtime_fields",
    [
        {"environment": "local"},
        {"environment": "local", "properties": None},
        {
            "environment": "local",
            "properties": {"region": "us-east-1", "s3_bucket": "bucket", "log_group": "logs", "log_retention_days": 30},
        },
        {"environment": "aws", "properties": {"data_root": "/tmp/valkyrie"}},
    ],
    ids=["missing-local-resources", "null-local-resources", "aws-resources-for-local", "local-resources-for-aws"],
)
def test_stored_arguments_reject_mismatched_resources(runtime_fields: dict[str, object]) -> None:
    """Database decoding rejects saved environments without compatible resources."""
    with pytest.raises(ValidationError):
        BenchmarkArgumentsType().process_result_value(
            {"contract": {"name": "agent"}, "concurrency": 5, **runtime_fields}, DefaultDialect()
        )


@pytest.mark.parametrize("priority", [0, 4])
def test_benchmark_arguments_accepts_priority_bounds(priority: int) -> None:
    arguments = AWSBenchmarkArguments(
        contract=AgentContractRequest(name="agent"),
        concurrency=5,
        priority=priority,
    )

    assert arguments.priority == priority


@pytest.mark.parametrize("priority", [False, True, "1", 1.0, -1, 5])
def test_benchmark_arguments_rejects_invalid_priority(priority: object) -> None:
    with pytest.raises(ValidationError):
        AWSBenchmarkArguments.model_validate(
            {
                "contract": AgentContractRequest(name="agent"),
                "concurrency": 5,
                "priority": priority,
            },
        )


def test_direct_benchmark_storage_omits_scheduler_fields(database_session: Session) -> None:
    stored = BenchmarkArgumentsType().process_bind_param(
        AWSBenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="echo install", run_cmd="echo run"),
            concurrency=5,
        ),
        database_session.get_bind().dialect,
    )

    assert stored is not None
    assert "priority" not in stored
    assert "queue_pool_id" not in stored


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
        arguments=AWSBenchmarkArguments(
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
