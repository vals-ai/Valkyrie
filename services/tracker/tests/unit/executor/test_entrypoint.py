"""Tests for the immutable executor PEX entrypoint."""

import asyncio
import json
import signal
import sys
from pathlib import Path
from collections.abc import Callable
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch

from tracker.executor import entrypoint as executor_entrypoint


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


def test_main_forwards_executor_payload(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
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

    process_benchmark.assert_awaited_once_with(
        start_benchmark_request_json=payload["start_benchmark_request_json"],
        benchmark_id_str=payload["benchmark_id_str"],
        verified_task_ids=payload["verified_task_ids"],
        executor_dispatch_id=payload["executor_dispatch_id"],
    )


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
