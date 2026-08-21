"""Tests for the immutable executor PEX entrypoint.

Run: uv run pytest tests/unit/executor/test_entrypoint.py
"""

import asyncio
import json
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
import sentry_sdk
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation import set_span_in_context
from pytest import MonkeyPatch

from tracker.config import ENVIRONMENT
from tracker.executor import entrypoint as executor_entrypoint
from tracker.logging import benchmark_id_var, request_id_var, task_id_var


@pytest.fixture(autouse=True)
def observability(monkeypatch: MonkeyPatch) -> tuple[Mock, Mock]:
    configure = Mock()
    flush = Mock()
    monkeypatch.setattr(executor_entrypoint, "configure_observability", configure)
    monkeypatch.setattr(executor_entrypoint, "_flush_observability", flush)
    return configure, flush


@pytest.mark.asyncio
async def test_sigterm_cancels_executor_and_awaits_cleanup(monkeypatch: MonkeyPatch) -> None:
    started = asyncio.Event()
    cleaned_up = asyncio.Event()
    signal_handler: Callable[[], None] | None = None
    loop = asyncio.get_running_loop()

    async def process_benchmark(**_payload: object) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned_up.set()

    def add_signal_handler(received_signal: signal.Signals, handler: Callable[[], None]) -> None:
        nonlocal signal_handler
        assert received_signal == signal.SIGTERM
        signal_handler = handler

    def remove_signal_handler(received_signal: signal.Signals) -> bool:
        return received_signal == signal.SIGTERM

    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(loop, "add_signal_handler", add_signal_handler)
    monkeypatch.setattr(loop, "remove_signal_handler", remove_signal_handler)

    executor = asyncio.create_task(
        executor_entrypoint._run_executor(  # pyright: ignore[reportPrivateUsage]
            {
                "start_benchmark_request_json": {},
                "benchmark_id_str": "benchmark-id",
                "verified_task_ids": [],
                "executor_dispatch_id": "dispatch-id",
            }
        )
    )
    await started.wait()
    assert signal_handler is not None
    signal_handler()
    await executor

    assert cleaned_up.is_set()


@pytest.mark.asyncio
async def test_unhandled_executor_error_is_captured_with_run_context(monkeypatch: MonkeyPatch) -> None:
    error = RuntimeError("executor failed")

    async def process_benchmark(**_payload: object) -> None:
        raise error

    captured_context: dict[str, str] = {}

    def capture_exception(captured_error: BaseException) -> None:
        assert captured_error is error
        captured_context["benchmark_id"] = benchmark_id_var.get()
        captured_context["request_id"] = request_id_var.get()

    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(sentry_sdk, "capture_exception", capture_exception)

    with pytest.raises(RuntimeError, match="executor failed"):
        await executor_entrypoint._run_executor(  # pyright: ignore[reportPrivateUsage]
            {
                "benchmark_id_str": "benchmark-id",
                "telemetry_context_json": {"request_id": "request-id", "trace_headers": {}},
                "executor_dispatch_id": "dispatch-id",
            }
        )

    assert captured_context == {"benchmark_id": "benchmark-id", "request_id": "request-id"}


def test_main_forwards_executor_payload(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    observability: tuple[Mock, Mock],
) -> None:
    payload = {
        "start_benchmark_request_json": {"benchmark": "test-benchmark"},
        "benchmark_id_str": "benchmark-id",
        "verified_task_ids": ["task-1", "task-2"],
        "executor_dispatch_id": "dispatch-id",
    }
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    process_benchmark = AsyncMock()
    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    executor_entrypoint.main()

    configure, flush = observability
    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    flush.assert_called_once_with()
    process_benchmark.assert_awaited_once_with(
        start_benchmark_request_json=payload["start_benchmark_request_json"],
        benchmark_id_str=payload["benchmark_id_str"],
        verified_task_ids=payload["verified_task_ids"],
        execution_context_json=None,
        executor_dispatch_id=payload["executor_dispatch_id"],
    )


def test_main_forwards_managed_execution_payload(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    observability: tuple[Mock, Mock],
) -> None:
    payload = {
        "execution_context_json": {"benchmark_id": "benchmark-id"},
        "executor_dispatch_id": "dispatch-id",
    }
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    process_benchmark = AsyncMock()
    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    executor_entrypoint.main()

    configure, flush = observability
    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    flush.assert_called_once_with()
    process_benchmark.assert_awaited_once_with(
        start_benchmark_request_json=None,
        benchmark_id_str=None,
        verified_task_ids=None,
        execution_context_json=payload["execution_context_json"],
        executor_dispatch_id=payload["executor_dispatch_id"],
    )


def test_executor_context_restores_run_and_trace_context(monkeypatch: MonkeyPatch) -> None:
    parent_span = NonRecordingSpan(
        SpanContext(
            trace_id=0x1234567890ABCDEF1234567890ABCDEF,
            span_id=0x1234567890ABCDEF,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
    )
    extract = Mock(return_value=set_span_in_context(parent_span))
    monkeypatch.setattr(executor_entrypoint, "extract", extract)
    request_token = request_id_var.set("outer-request")
    benchmark_token = benchmark_id_var.set("outer-benchmark")
    task_token = task_id_var.set("outer-task")

    try:
        with executor_entrypoint._executor_context(  # pyright: ignore[reportPrivateUsage]
            {
                "execution_context_json": {"benchmark_id": "benchmark-id"},
                "telemetry_context_json": {
                    "request_id": "request-id",
                    "trace_headers": {"sentry-trace": "trace-header", "baggage": "baggage-header"},
                },
            }
        ):
            assert request_id_var.get() == "request-id"
            assert benchmark_id_var.get() == "benchmark-id"
            assert task_id_var.get() == ""
            assert trace.get_current_span().get_span_context() == parent_span.get_span_context()

        assert request_id_var.get() == "outer-request"
        assert benchmark_id_var.get() == "outer-benchmark"
        assert task_id_var.get() == "outer-task"
        assert not trace.get_current_span().get_span_context().is_valid
    finally:
        task_id_var.reset(task_token)
        benchmark_id_var.reset(benchmark_token)
        request_id_var.reset(request_token)

    extract.assert_called_once_with({"sentry-trace": "trace-header", "baggage": "baggage-header"})


@pytest.mark.parametrize("payload", [[], "not an object", 1, None])
def test_main_rejects_non_object_payload(
    payload: object,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    with pytest.raises(SystemExit, match="Invalid executor payload: expected object"):
        executor_entrypoint.main()


def test_main_rejects_malformed_json(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text("not-json", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    with pytest.raises(json.JSONDecodeError):
        executor_entrypoint.main()
