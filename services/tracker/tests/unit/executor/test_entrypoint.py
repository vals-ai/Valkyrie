"""Run with `uv run pytest tests/unit/executor/test_entrypoint.py`."""

import asyncio
import os
import signal
import sys
import threading
from datetime import timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from pytest import MonkeyPatch

from tracker.config import ENVIRONMENT
from tracker.database.models import FailureCategory
from tracker.executor import entrypoint
from tracker.executor.dispatch_control import ClaimedDispatch


@pytest.fixture
def lifecycle(monkeypatch: MonkeyPatch) -> dict[str, Mock]:
    mocks = {name: Mock() for name in ("_finish", "_fail", "record_dispatch_completion")}
    for name, mock in mocks.items():
        monkeypatch.setattr(entrypoint, name, mock)
    return mocks


def _claimed(dispatch_id: UUID) -> ClaimedDispatch:
    return ClaimedDispatch(
        dispatch_id=dispatch_id,
        benchmark_id=uuid4(),
        release_id="release-1",
        assigned_task_ids=["task-1"],
        payload={
            "start_benchmark_request_json": {"benchmark": "test-benchmark"},
            "benchmark_id_str": "benchmark-id",
            "verified_task_ids": ["task-1"],
            "telemetry_context_json": {"request_id": "request-id", "trace_headers": {}},
        },
    )


def test_main_runs_the_dispatch_and_flushes(monkeypatch: MonkeyPatch) -> None:
    dispatch_id = uuid4()
    run_dispatch = AsyncMock()
    configure = Mock()
    flush = Mock()
    monkeypatch.setattr(entrypoint, "run_dispatch", run_dispatch)
    monkeypatch.setattr(entrypoint, "configure_logging", Mock())
    monkeypatch.setattr(entrypoint, "configure_observability", configure)
    monkeypatch.setattr(entrypoint, "_flush_observability", flush)
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", "--dispatch-id", str(dispatch_id)])

    entrypoint.main()

    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    run_dispatch.assert_awaited_once_with(dispatch_id)
    flush.assert_called_once_with()


def test_unclaimable_dispatch_runs_nothing(monkeypatch: MonkeyPatch, lifecycle: dict[str, Mock]) -> None:
    process_benchmark = AsyncMock()
    monkeypatch.setattr(entrypoint, "_claim", lambda _dispatch_id: None)
    monkeypatch.setattr(entrypoint, "process_benchmark", process_benchmark)

    asyncio.run(entrypoint.run_dispatch(uuid4()))

    process_benchmark.assert_not_awaited()
    lifecycle["_finish"].assert_not_called()
    lifecycle["_fail"].assert_not_called()


def test_failed_dispatch_records_the_failure_and_reraises(monkeypatch: MonkeyPatch, lifecycle: dict[str, Mock]) -> None:
    dispatch_id = uuid4()
    claimed = _claimed(dispatch_id)
    capture = Mock()
    monkeypatch.setattr(entrypoint, "_claim", lambda _dispatch_id: claimed)
    monkeypatch.setattr(entrypoint, "process_benchmark", AsyncMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(entrypoint, "capture_dispatch_error", capture)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(entrypoint.run_dispatch(dispatch_id))

    lifecycle["_fail"].assert_called_once_with(
        claimed, "Executor dispatch failed: boom", "RuntimeError", "EXECUTOR_FAILED", FailureCategory.UNKNOWN
    )
    lifecycle["_finish"].assert_not_called()
    capture.assert_called_once()


def test_sigterm_cancels_the_dispatch_and_records_cancellation(
    monkeypatch: MonkeyPatch, lifecycle: dict[str, Mock]
) -> None:
    dispatch_id = uuid4()
    claimed = _claimed(dispatch_id)
    cancellation = Mock()

    async def hang_until_terminated(**_kwargs: object) -> None:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(30)

    monkeypatch.setattr(entrypoint, "_claim", lambda _dispatch_id: claimed)
    monkeypatch.setattr(entrypoint, "process_benchmark", hang_until_terminated)
    monkeypatch.setattr(entrypoint, "record_dispatch_cancellation", cancellation)

    async def terminated_by_signal() -> None:
        dispatch = asyncio.create_task(entrypoint.run_dispatch(dispatch_id))
        try:
            done, _ = await asyncio.wait({dispatch}, timeout=2)
            assert dispatch in done, "SIGTERM did not cancel benchmark work"
            await dispatch
        finally:
            dispatch.cancel()
            await asyncio.gather(dispatch, return_exceptions=True)

    asyncio.run(terminated_by_signal())

    lifecycle["_fail"].assert_called_once_with(
        claimed,
        "Executor dispatch was cancelled",
        "ExecutorDispatchCancelled",
        "EXECUTOR_CANCELLED",
        FailureCategory.CANCELLED,
    )
    lifecycle["_finish"].assert_not_called()
    cancellation.assert_called_once()


def test_local_lease_cancels_work_while_renewal_is_blocked(
    monkeypatch: MonkeyPatch, lifecycle: dict[str, Mock]
) -> None:
    renewal_started = threading.Event()
    release_renewal = threading.Event()
    work_cancelled = threading.Event()

    def blocked_renewal(_dispatch_id: UUID) -> bool:
        renewal_started.set()
        assert release_renewal.wait(5)
        return True

    async def benchmark(**_kwargs: object) -> None:
        try:
            await asyncio.Future()
        finally:
            work_cancelled.set()
            release_renewal.set()

    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setattr(entrypoint, "LOCAL_DISPATCH_LEASE", timedelta(seconds=0.1))
    monkeypatch.setattr(entrypoint, "_LOCAL_RENEW_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(entrypoint, "_claim", _claimed)
    monkeypatch.setattr(entrypoint, "_renew_local", blocked_renewal)
    monkeypatch.setattr(entrypoint, "process_benchmark", benchmark)

    async def expired_by_lease() -> None:
        dispatch = asyncio.create_task(entrypoint.run_dispatch(uuid4()))
        try:
            done, _ = await asyncio.wait({dispatch}, timeout=1)
            assert dispatch in done, "Lease expiry did not cancel benchmark work"
            await dispatch
        finally:
            release_renewal.set()
            dispatch.cancel()
            await asyncio.gather(dispatch, return_exceptions=True)

    asyncio.run(expired_by_lease())
    assert renewal_started.is_set()
    assert work_cancelled.is_set()
    lifecycle["_finish"].assert_not_called()
