"""Tests for canonical run API adapters.

Run: pytest tests/unit/test_canonical_runs.py

Covers canonical start routing and response serialization.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import Request
from sqlmodel import Session

import main as tracker_main
from tracker.auth import RequestIdentity
from tracker.database.models import AgentContractRequest
from tracker.types import StartBenchmarkResponse, StartRunRequest


@pytest.mark.parametrize("managed_s3_bucket", [None, "owner-run-artifacts"])
async def test_start_run_selects_storage_mode_and_returns_run_id(
    managed_s3_bucket: str | None,
    contract: AgentContractRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = UUID("8fe27d06-7c4e-40ef-9c4c-7925fd1f7f76")
    legacy_response = StartBenchmarkResponse(
        benchmark_name="swebench",
        agent_name=contract.name,
        benchmark_id=run_id,
        concurrency=5,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        task_count=1,
        cloudwatch_url="https://console.aws.amazon.com/cloudwatch",
        s3_bucket_url="https://console.aws.amazon.com/s3",
    )
    standard_start = AsyncMock(return_value=legacy_response)
    managed_start = AsyncMock(return_value=legacy_response)
    monkeypatch.setattr(tracker_main, "start_benchmark", standard_start)
    monkeypatch.setattr(tracker_main, "start_benchmark_with_storage", managed_start)
    request = StartRunRequest(
        contract=contract,
        benchmark_name="swebench",
        managed_s3_bucket=managed_s3_bucket,
    )

    response = await tracker_main.start_run(
        MagicMock(spec=Request),
        request,
        MagicMock(spec=Session),
        MagicMock(spec=RequestIdentity),
    )

    legacy_payload = legacy_response.model_dump()
    assert legacy_payload["benchmark_id"] == run_id
    assert "run_id" not in legacy_payload

    payload = response.model_dump(by_alias=True)
    assert payload["run_id"] == run_id
    assert "benchmark_id" not in payload
    assert standard_start.await_count == (0 if managed_s3_bucket else 1)
    assert managed_start.await_count == (1 if managed_s3_bucket else 0)
