"""Tests for the immutable executor PEX entrypoint.

Run: uv run pytest tests/unit/executor/test_entrypoint.py
"""

import asyncio
import json
import signal
import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import Mock

import pytest
import sentry_sdk
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation import set_span_in_context
from pytest import MonkeyPatch

from executor_protocol import AccessKeyExecutorExecution, ExecutorTelemetryContext, ManagedExecutorExecution
from tracker.config import ENVIRONMENT
from tracker.database.models import AgentContractRequest
from tracker.executor import entrypoint as executor_entrypoint
from tracker.logging import benchmark_id_var, request_id_var, task_id_var
from tracker.types import (
    AccessKeyExecutionRequest,
    ExecutorProcessPayload,
    HarnessConfig,
    ManagedExecutionContext,
    StartBenchmarkRequest,
)

_BENCHMARK_ID = "00000000-0000-0000-0000-000000000001"


def _access_key_payload(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    *,
    telemetry_context: ExecutorTelemetryContext | None = None,
) -> ExecutorProcessPayload:
    request = AccessKeyExecutionRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        harness_config=harness_config,
    )
    return ExecutorProcessPayload(
        execution=AccessKeyExecutorExecution(
            request=request,
            benchmark_id=_BENCHMARK_ID,
            verified_task_ids=["task-1", "task-2"],
        ),
        telemetry_context=telemetry_context or ExecutorTelemetryContext(),
        executor_dispatch_id="dispatch-id",
    )


def _managed_payload(contract: AgentContractRequest) -> ExecutorProcessPayload:
    request = StartBenchmarkRequest(
        contract=contract,
        benchmark_name="test-benchmark",
        sandbox_provider_secret_name="provider-secret",
    )
    return ExecutorProcessPayload(
        execution=ManagedExecutorExecution(
            context=ManagedExecutionContext(
                version=2,
                benchmark_id=_BENCHMARK_ID,
                verified_task_ids=["task-1", "task-2"],
                start_benchmark_request=request,
            )
        ),
        executor_dispatch_id="dispatch-id",
    )


@pytest.fixture(autouse=True)
def observability(monkeypatch: MonkeyPatch) -> tuple[Mock, Mock]:
    configure = Mock()
    flush = Mock()
    monkeypatch.setattr(executor_entrypoint, "configure_observability", configure)
    monkeypatch.setattr(executor_entrypoint, "_flush_observability", flush)
    return configure, flush


async def test_sigterm_cancels_executor_and_awaits_cleanup(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    monkeypatch: MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cleaned_up = asyncio.Event()
    signal_handler: Callable[[], None] | None = None
    loop = asyncio.get_running_loop()

    async def process_benchmark(_execution: object, *, executor_dispatch_id: str) -> None:
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
            _access_key_payload(contract, harness_config)
        )
    )
    await started.wait()
    assert signal_handler is not None
    signal_handler()
    await executor

    assert cleaned_up.is_set()


async def test_unhandled_executor_error_is_captured_with_run_context(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    monkeypatch: MonkeyPatch,
) -> None:
    error = RuntimeError("executor failed")

    async def process_benchmark(_execution: object, *, executor_dispatch_id: str) -> None:
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
            _access_key_payload(
                contract,
                harness_config,
                telemetry_context=ExecutorTelemetryContext(request_id="request-id"),
            )
        )

    assert captured_context == {"benchmark_id": _BENCHMARK_ID, "request_id": "request-id"}


def test_main_forwards_executor_payload(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    observability: tuple[Mock, Mock],
) -> None:
    payload = _access_key_payload(contract, harness_config)
    wire_payload = payload.to_wire()
    wire_payload.pop("telemetry_context_json")
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(wire_payload), encoding="utf-8")
    observed: dict[str, object] = {}

    async def process_benchmark(execution: object, *, executor_dispatch_id: str) -> None:
        observed.update(
            execution=execution,
            executor_dispatch_id=executor_dispatch_id,
            request_id=request_id_var.get(),
        )

    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    executor_entrypoint.main()

    configure, flush = observability
    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    flush.assert_called_once_with()
    assert observed == {
        "execution": payload.execution,
        "executor_dispatch_id": payload.executor_dispatch_id,
        "request_id": "",
    }


def test_main_forwards_managed_execution_payload(
    contract: AgentContractRequest,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    observability: tuple[Mock, Mock],
) -> None:
    payload = _managed_payload(contract)
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps(payload.to_wire()), encoding="utf-8")
    observed: dict[str, object] = {}

    async def process_benchmark(execution: object, *, executor_dispatch_id: str) -> None:
        observed.update(execution=execution, executor_dispatch_id=executor_dispatch_id)

    monkeypatch.setattr(executor_entrypoint, "process_benchmark", process_benchmark)
    monkeypatch.setattr(sys, "argv", ["executor-entrypoint", str(payload_path)])

    executor_entrypoint.main()

    configure, flush = observability
    configure.assert_called_once_with("valkyrie-executor", environment=ENVIRONMENT)
    flush.assert_called_once_with()
    assert observed == {
        "execution": payload.execution,
        "executor_dispatch_id": payload.executor_dispatch_id,
    }


def test_executor_context_restores_run_and_trace_context(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    monkeypatch: MonkeyPatch,
) -> None:
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
    payload = _access_key_payload(
        contract,
        harness_config,
        telemetry_context=ExecutorTelemetryContext(
            request_id="request-id",
            trace_headers={"sentry-trace": "trace-header", "baggage": "baggage-header"},
        ),
    )

    try:
        with executor_entrypoint._executor_context(payload):  # pyright: ignore[reportPrivateUsage]
            assert request_id_var.get() == "request-id"
            assert benchmark_id_var.get() == _BENCHMARK_ID
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
