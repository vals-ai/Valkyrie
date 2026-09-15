"""Unit tests for cumulative model/API cost accounting.

Run: uv run pytest tests/unit/utils/test_model_api_cost.py
"""

from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from executor_protocol import ExecutorDispatchStatus
from sqlmodel import Session

from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    bind_task_to_dispatch,
    create_task_environment,
    run_process_task,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, ExecutorDispatch, Task, TaskStatus
from tracker.exceptions import AgentRunFailedError
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.types import HarnessConfig
from tracker.utils import task_execution as task_execution_module
from tracker.utils.reporting import BenchmarkContext

_begin_model_api_cost_attempt = getattr(task_execution_module, "_begin_model_api_cost_attempt")
_record_model_api_cost_report = getattr(task_execution_module, "_record_model_api_cost_report")


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


def test_cost_accumulates_across_retries(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, benchmark, authority = _persist_running_task(contract, database_session, harness_config)
    first_attempt = task.started_at

    assert _begin_model_api_cost_attempt(task, TEST_ORG, first_attempt, authority)
    assert _record_model_api_cost_report(task, TEST_ORG, first_attempt, authority, Decimal("0.10"))

    task.started_at = first_attempt + timedelta(seconds=1)
    database_session.add(task)
    database_session.commit()
    database_session.refresh(task)
    second_attempt = task.started_at

    assert _begin_model_api_cost_attempt(task, TEST_ORG, second_attempt, authority)
    assert _record_model_api_cost_report(task, TEST_ORG, second_attempt, authority, Decimal("0.20"))

    database_session.refresh(task)
    details = BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details
    assert task.model_api_cost_usd == Decimal("0.30")
    assert task.model_api_cost_attempt_count == 2
    assert task.model_api_cost_report_count == 2
    assert details.model_api_cost_usd == Decimal("0.30")


def test_valid_then_missing_retry_makes_total_unavailable(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, benchmark, authority = _persist_running_task(contract, database_session, harness_config)
    first_attempt = task.started_at
    assert _begin_model_api_cost_attempt(task, TEST_ORG, first_attempt, authority)
    assert _record_model_api_cost_report(task, TEST_ORG, first_attempt, authority, Decimal("0.10"))

    task.started_at = first_attempt + timedelta(seconds=1)
    database_session.add(task)
    database_session.commit()
    database_session.refresh(task)
    assert _begin_model_api_cost_attempt(task, TEST_ORG, task.started_at, authority)

    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None


def test_stale_attempt_cannot_start_or_record_cost(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, _, authority = _persist_running_task(contract, database_session, harness_config)
    stale_attempt = task.started_at
    assert _begin_model_api_cost_attempt(task, TEST_ORG, stale_attempt, authority)

    task.started_at = stale_attempt + timedelta(seconds=1)
    database_session.add(task)
    database_session.commit()

    assert not _record_model_api_cost_report(task, TEST_ORG, stale_attempt, authority, Decimal("5"))
    assert not _begin_model_api_cost_attempt(task, TEST_ORG, stale_attempt, authority)
    database_session.refresh(task)
    assert task.model_api_cost_usd == Decimal("0")
    assert task.model_api_cost_report_count == 0


def test_revoked_authority_cannot_start_attempt(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, _, authority = _persist_running_task(contract, database_session, harness_config)
    dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FINISHED
    database_session.add(dispatch)
    database_session.commit()

    assert not _begin_model_api_cost_attempt(task, TEST_ORG, task.started_at, authority)
    database_session.refresh(task)
    assert task.model_api_cost_attempt_count == 0


def test_revoked_authority_cannot_record_report(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, benchmark, authority = _persist_running_task(contract, database_session, harness_config)
    assert _begin_model_api_cost_attempt(task, TEST_ORG, task.started_at, authority)

    dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FINISHED
    database_session.add(dispatch)
    database_session.commit()

    assert not _record_model_api_cost_report(task, TEST_ORG, task.started_at, authority, Decimal("5"))
    database_session.refresh(task)
    assert task.model_api_cost_report_count == 0
    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None


def test_legacy_task_keeps_run_total_unavailable(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    task, benchmark, _ = _persist_running_task(contract, database_session, harness_config)
    task.model_api_cost_usd = None
    task.model_api_cost_attempt_count = None
    task.model_api_cost_report_count = None
    database_session.add(task)
    database_session.commit()

    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None


async def test_process_task_wires_attempt_and_report_callbacks(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    aws_runtime: AWSRuntime,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, task, benchmark_id, authority = create_task_environment(contract, database_session, harness_config)

    async def report_cost(*_args: Any, **kwargs: Any) -> tuple[None, float]:
        await kwargs["on_agent_start"]()
        await kwargs["on_model_api_cost"](Decimal("0.30"))
        return None, 0.0

    monkeypatch.setattr(task_execution_module, "run_agent", report_cost)

    await run_process_task(request, task, benchmark_id, aws_runtime, authority)

    database_session.refresh(task)
    assert task.status == TaskStatus.FINISHED
    assert task.model_api_cost_usd == Decimal("0.30")
    assert task.model_api_cost_attempt_count == 1
    assert task.model_api_cost_report_count == 1


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
    assert task.model_api_cost_attempt_count == 1
    assert task.model_api_cost_report_count == 0
    assert BenchmarkContext(benchmark, database_session, TEST_ORG).benchmark_details.model_api_cost_usd is None
