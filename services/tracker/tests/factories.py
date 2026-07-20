"""Typed database-model factories shared across tracker tests."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Task,
    TaskStatus,
)


def make_benchmark(
    name: str = "swebench",
    *,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    org_id: UUID = TEST_ORG_ID,
    contract: AgentContractRequest | None = None,
    agent_name: str = "a",
    concurrency: int = 1,
    started_at: datetime | None = None,
    started_by_id: str | None = None,
    started_by_email: str | None = None,
    label: str | None = None,
    session: Session | None = None,
) -> Benchmark:
    """Build a benchmark with deterministic tracker defaults.

    Arguments
    - name: Benchmark catalog name.
    - status: Initial benchmark status.
    - org_id: Organization that owns the benchmark.
    - contract: Optional agent contract; a deterministic contract is used by default.
    - agent_name: Agent contract name stored with the benchmark.
    - concurrency: Requested task concurrency.
    - started_at: Optional fixed start time.
    - started_by_id: Optional starter identifier.
    - started_by_email: Optional starter identity.
    - label: Optional benchmark label.
    - session: Optional database session that persists the benchmark.

    Returns
    - A benchmark row, persisted when a session is provided.
    """
    benchmark = Benchmark(
        org_id=org_id,
        name=name,
        status=status,
        label=label,
        started_by_id=started_by_id,
        started_by_email=started_by_email,
        arguments=BenchmarkArguments(
            contract=contract or AgentContractRequest(name=agent_name, install_cmd="i", run_cmd="r"),
            concurrency=concurrency,
        ),
    )
    if started_at is not None:
        benchmark.started_at = started_at
    if session is not None:
        session.add(benchmark)
        session.commit()

    return benchmark


def make_task(
    benchmark: Benchmark,
    task_id: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> Task:
    """Build a task belonging to a benchmark.

    Arguments
    - benchmark: Parent benchmark row.
    - task_id: Dataset task identifier.
    - status: Initial task status.
    - started_at: Optional fixed start time.
    - finished_at: Optional fixed finish time.

    Returns
    - A task row ready to persist.
    """
    task = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id=task_id,
        status=status,
        finished_at=finished_at,
    )
    if started_at is not None:
        task.started_at = started_at

    return task


def make_evaluation_result(
    task: Task,
    instance_id: str,
    result: dict[str, Any],
    created_at: datetime,
    *,
    exit_reason: AgentCausedExitReason | None = None,
) -> EvaluationResult:
    """Build an evaluation attempt for a task.

    Arguments
    - task: Task associated with the evaluation.
    - instance_id: Benchmark-service instance identifier.
    - result: Evaluation payload.
    - created_at: Fixed ordering timestamp.
    - exit_reason: Optional agent exit classification.

    Returns
    - An evaluation-result row ready to persist.
    """
    return EvaluationResult(
        org_id=task.org_id,
        task=task.id,
        instance_id=instance_id,
        result=result,
        agent_caused_exit_reason=exit_reason,
        created_at=created_at,
    )


def make_error_result(task: Task, error_message: str, created_at: datetime) -> ErrorResult:
    """Build an error attempt for a task.

    Arguments
    - task: Task associated with the error.
    - error_message: Stored failure detail.
    - created_at: Fixed ordering timestamp.

    Returns
    - An error-result row ready to persist.
    """
    return ErrorResult(
        org_id=task.org_id,
        task=task.id,
        error_message=error_message,
        created_at=created_at,
    )
