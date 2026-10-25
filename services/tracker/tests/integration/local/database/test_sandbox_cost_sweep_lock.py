"""Only one Tracker worker at a time runs the sandbox cost sweep."""

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine

import tracker.sandbox_cost as sandbox_cost
from tracker.scheduler.store import sandbox_cost_sweep_lock

NOW = datetime(2026, 10, 23, 18, tzinfo=UTC)


async def _lock_is_free(engine: Engine) -> bool:
    async with sandbox_cost_sweep_lock(engine) as acquired:
        return acquired


async def test_a_single_sweeper_runs_and_frees_the_lock(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def sweep(_bind: object, *, now: datetime) -> int:
        return 2

    monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

    assert await sandbox_cost.sweep_sandbox_costs_once(postgres_engine, now=NOW) == 2
    assert await _lock_is_free(postgres_engine)


async def test_a_concurrent_sweep_skips_while_another_worker_runs(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    sweeps: list[str] = []

    async def sweep(_bind: object, *, now: datetime) -> int:
        sweeps.append("ran")
        started.set()
        await finish.wait()
        return 3

    monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

    first = asyncio.create_task(sandbox_cost.sweep_sandbox_costs_once(postgres_engine, now=NOW))
    await started.wait()
    second = await sandbox_cost.sweep_sandbox_costs_once(postgres_engine, now=NOW)
    finish.set()

    assert second is None
    assert await first == 3
    assert sweeps == ["ran"]
    assert await _lock_is_free(postgres_engine)


async def test_the_lock_is_released_after_the_sweep_raises(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def sweep(_bind: object, *, now: datetime) -> int:
        raise RuntimeError("daytona unavailable")

    monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

    with pytest.raises(RuntimeError, match="daytona unavailable"):
        await sandbox_cost.sweep_sandbox_costs_once(postgres_engine, now=NOW)

    assert await _lock_is_free(postgres_engine)
