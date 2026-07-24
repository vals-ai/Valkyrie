"""Tests for the immutable executor PEX entrypoint."""

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from pytest import MonkeyPatch

from tracker import executor_entrypoint


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
