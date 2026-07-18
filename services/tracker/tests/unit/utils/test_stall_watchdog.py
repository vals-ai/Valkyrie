"""Unit tests for the per-task stall watchdog.

Run: uv run pytest tests/unit/utils/test_stall_watchdog.py
"""

import asyncio
from asyncio import Semaphore
from typing import Any

import pytest
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session, desc, select

import tracker.utils.task_execution as task_execution_module
from tests.unit.utils.task_execution_support import TEST_ORG, create_task_environment
from tracker.database.models import AgentContractRequest, ErrorResult, Task, TaskStatus
from tracker.types import HarnessConfig
from tracker.utils import fetch_sandbox_provider_config
from tracker.utils.task_execution import TrackedTask, process_task


def _latest_task_error(database_session: Session, task_row: Task) -> str:
    return database_session.exec(
        select(ErrorResult.error_message)
        .where(ErrorResult.task == task_row.id)
        .where(ErrorResult.org_id == task_row.org_id)
        .order_by(desc(ErrorResult.created_at))
    ).one()


@pytest.mark.usefixtures("process_benchmark_env")
async def test_stall_watchdog_fails_hung_task(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    """A task whose benchmark-service call hangs must be terminated and marked ERROR.

    Without the watchdog, TrackedTask.run awaits the hung coroutine forever, so the
    outer asyncio.wait_for guard here would raise TimeoutError and fail the test.
    With the watchdog, the attempt is cancelled after TASK_MAX_DURATION_SECONDS and
    the task is committed as ERROR with a descriptive stall message.
    """
    start_benchmark_request, task_row, benchmark_id = create_task_environment(
        contract, database_session, harness_config
    )

    hang_reached = asyncio.Event()

    async def _hang_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        hang_reached.set()
        await asyncio.sleep(3600)  # Simulate a wedged sandbox / websocket that never returns.
        return {}

    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _hang_evaluate_instance)
    # Tighten the wall-clock ceiling so the watchdog fires quickly in the test.
    monkeypatch.setattr(task_execution_module, "TASK_MAX_DURATION_SECONDS", 0.3)

    coro = process_task(
        task_row=task_row,
        start_benchmark_request=start_benchmark_request,
        benchmark_service=start_benchmark_request.benchmark_service,
        benchmark_id=benchmark_id,
        task_id="task_0",
        harness_config=harness_config,
        org=TEST_ORG,
        sandbox_provider_config=fetch_sandbox_provider_config(
            harness_config.sandbox_provider_secret_name,
            harness_config.aws,
            start_benchmark_request.sandbox_provider,
        ),
        creation_semaphore=Semaphore(1),
    )
    tracked_task = TrackedTask(coro, TEST_ORG)

    # The outer guard must comfortably exceed the watchdog deadline but stay well under the
    # simulated 3600s hang, so a regression (no watchdog) surfaces as a wait_for TimeoutError.
    result = await asyncio.wait_for(tracked_task.run(Semaphore(1), task_row), timeout=15)

    assert hang_reached.is_set()
    assert result == {"task_0": None}

    database_session.refresh(task_row)
    assert task_row.status == TaskStatus.ERROR
    error_message = _latest_task_error(database_session, task_row)
    assert "stall-watchdog max duration" in error_message
