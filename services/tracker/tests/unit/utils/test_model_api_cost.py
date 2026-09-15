"""Unit tests for cumulative model/API cost accounting.

Run: uv run pytest tests/unit/utils/test_model_api_cost.py
"""

from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from executor_protocol import ExecutorDispatchStatus
from sqlalchemy import literal
from sqlmodel import Session

from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    bind_task_to_dispatch,
    create_task_environment,
    run_process_task,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, ExecutorDispatch, Task, TaskStatus
from tracker.exceptions import AgentRunFailedError, ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.types import HarnessConfig
from tracker.utils import task_execution as task_execution_module
from tracker.utils.reporting import BenchmarkContext

_set_model_api_cost = getattr(task_execution_module, "_set_model_api_cost")
_StaleTaskAttempt = getattr(task_execution_module, "_StaleTaskAttempt")


@pytest.fixture(autouse=True)
def use_test_engine(database_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(task_execution_module, "engine", database_session.bind)


def _persist_running_task(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> tuple[Task, Benchmark, ExecutionAuthority]:
    _, task, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)
    task.status = TaskStatus.IN_PROGRESS
    bind_task_to_dispatch(database_session, task, authority)
    benchmark = database_session.get(Benchmark, benchmark_id)
    assert benchmark is not None
    return task, benchmark, authority


@pytest.mark.parametrize("stale", [True, False])
@pytest.mark.parametrize("amount", [None, Decimal("5")])
def test_stale_or_revoked_worker_cannot_change_cost(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    stale: bool,
    amount: Decimal | None,
) -> None:
    task, _, authority = _persist_running_task(contract, database_session, harness_config)
    started_at = task.started_at
    if stale:
        task.started_at += timedelta(seconds=1)
        database_session.add(task)
    else:
        dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FINISHED
        database_session.add(dispatch)
    database_session.commit()

    with pytest.raises(_StaleTaskAttempt if stale else ExecutionAuthorityRevoked):
        _set_model_api_cost(task, started_at, authority, literal(amount) if amount is not None else None)

    database_session.refresh(task)
    assert task.model_api_cost_usd == Decimal("0")


def test_interrupted_attempt_leaves_total_unknown_across_retries(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, benchmark, authority = _persist_running_task(contract, database_session, harness_config)
    assert _set_model_api_cost(task, task.started_at, authority) == Decimal("0")
    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None

    task.started_at += timedelta(seconds=1)
    database_session.add(task)
    database_session.commit()
    assert _set_model_api_cost(task, task.started_at, authority) is None


@pytest.mark.parametrize(
    ("reports", "initial_cost", "expected_cost"),
    [
        (["0.10", "0.20"], "0", "0.30"),
        ([None, "0.20"], "0", None),
        (["0.10", None], "0", None),
        (["0.20"], None, None),  # Legacy tasks have no known starting total.
        (["0"], "0", "0"),
    ],
)
async def test_process_task_accumulates_only_complete_cost(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    aws_runtime: AWSRuntime,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    reports: list[str | None],
    initial_cost: str | None,
    expected_cost: str | None,
) -> None:
    request, task, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)
    task.model_api_cost_usd = Decimal(initial_cost) if initial_cost is not None else None
    database_session.add(task)
    database_session.commit()

    async def report_cost(*_args: Any, **kwargs: Any) -> tuple[None, float]:
        for report in reports:
            await kwargs["on_agent_start"]()
            if report is not None:
                await kwargs["on_model_api_cost"](Decimal(report))
        return None, 0.0

    monkeypatch.setattr(task_execution_module, "run_agent", report_cost)

    await run_process_task(request, task, benchmark_id, aws_runtime, authority)

    database_session.refresh(task)
    assert task.status == TaskStatus.FINISHED
    assert task.model_api_cost_usd == (Decimal(expected_cost) if expected_cost is not None else None)


async def test_failed_agent_attempt_without_report_is_unavailable(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    aws_runtime: AWSRuntime,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, task, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)

    async def fail_without_report(*_args: Any, **kwargs: Any) -> None:
        await kwargs["on_agent_start"]()
        raise AgentRunFailedError("agent failed")

    monkeypatch.setattr(task_execution_module, "run_agent", fail_without_report)

    await run_process_task(request, task, benchmark_id, aws_runtime, authority)

    database_session.refresh(task)
    benchmark = database_session.get(Benchmark, benchmark_id)
    assert benchmark is not None
    assert task.status == TaskStatus.ERROR
    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None
