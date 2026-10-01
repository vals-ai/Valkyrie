"""HTTP behavior for ambiguous executor task-launch acknowledgement."""

from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlmodel import Session

import main
from tracker.executor.dispatch_control import EnqueueFailureResolution


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolution", "expected_status", "expected_detail"),
    [
        (EnqueueFailureResolution.SUPERSEDED, 409, "Executor dispatch was superseded by a newer Retry"),
        (
            EnqueueFailureResolution.FAILED,
            503,
            "Executor dispatch enqueue acknowledgement failed; use Retry to continue",
        ),
    ],
)
async def test_launch_failure_preserves_http_contract(
    resolution: EnqueueFailureResolution,
    expected_status: int,
    expected_detail: str,
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
) -> None:
    dispatch = SimpleNamespace(id=uuid4(), benchmark_id=uuid4())
    monkeypatch.setattr(main, "launch_dispatch", AsyncMock(side_effect=RuntimeError("ECS transport failed")))
    monkeypatch.setattr(main, "_resolve_enqueue_failure", lambda *_args: resolution)

    with pytest.raises(HTTPException) as raised:
        await main._launch_executor_dispatch(dispatch, session=database_session, verified_task_ids=["task-1"])

    assert raised.value.status_code == expected_status
    if resolution == EnqueueFailureResolution.SUPERSEDED:
        assert raised.value.detail == expected_detail
    else:
        assert raised.value.detail == {
            "message": expected_detail,
            "benchmark_id": str(dispatch.benchmark_id),
            "executor_dispatch_id": str(dispatch.id),
        }


@pytest.mark.asyncio
async def test_ambiguous_launch_acknowledgement_accepts_claimed_dispatch(
    monkeypatch: pytest.MonkeyPatch, database_session: Session
) -> None:
    dispatch = SimpleNamespace(id=uuid4(), benchmark_id=uuid4())
    monkeypatch.setattr(main, "launch_dispatch", AsyncMock(side_effect=TimeoutError("ECS response lost")))
    monkeypatch.setattr(main, "_resolve_enqueue_failure", lambda *_args: EnqueueFailureResolution.DELIVERED)

    await main._launch_executor_dispatch(dispatch, session=database_session, verified_task_ids=[])
