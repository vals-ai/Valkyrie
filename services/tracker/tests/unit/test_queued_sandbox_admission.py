"""Build locks are acquired and released outside queued authority transactions.

Run: pytest tests/unit/test_queued_sandbox_admission.py
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest
from benchmark_service import ResourceCapacity, Sandbox, SandboxCapacity, SandboxReservationDemand

from tracker.executor.execution_authority import ExecutionAuthority
from tracker.sandbox import SandboxCreationState
from tracker.scheduler import admission, store
from tracker.scheduler.store import reserved_queue_pool_id


@pytest.mark.parametrize("claims", [[True], [False, True]])
async def test_build_lock_waits_outside_authority_transactions(
    monkeypatch: pytest.MonkeyPatch,
    claims: list[bool],
) -> None:
    active_sessions = 0

    @contextmanager
    def open_session(_bind: object) -> Generator[Mock, None, None]:
        nonlocal active_sessions
        active_sessions += 1
        try:
            yield Mock()
        finally:
            active_sessions -= 1

    build_lock = Mock(held=False)

    async def acquire() -> bool:
        assert active_sessions == 0
        build_lock.held = True

        return True

    async def release() -> None:
        assert active_sessions == 0
        build_lock.held = False

    build_lock.acquire = AsyncMock(side_effect=acquire)
    build_lock.release = AsyncMock(side_effect=release)
    pool_lock = MagicMock()
    pool_lock.__aenter__ = AsyncMock(return_value=True)
    pool_lock.__aexit__ = AsyncMock(return_value=False)
    provider = Mock()
    provider.check_admission = AsyncMock(return_value=True)
    provider.reservable_demand = AsyncMock(
        return_value=SandboxReservationDemand(
            target_id="region-a",
            sandbox_class="sysbox",
            vcpu=1,
            memory=2,
            disk=3,
            gpu=0,
        )
    )
    capacity = Mock()
    capacity.get = AsyncMock(
        return_value=admission.CapacitySnapshot(
            capacities={
                ("region-a", "sysbox"): SandboxCapacity(
                    cpu=ResourceCapacity(total=8, used=0),
                    memory=ResourceCapacity(total=16, used=0),
                    disk=ResourceCapacity(total=24, used=0),
                )
            },
            read_at=datetime(2026, 7, 27, tzinfo=UTC),
            fetched_at=0.0,
        )
    )
    context = admission.SandboxQueueContext(
        provider=provider,
        pool_id=reserved_queue_pool_id("pool"),
        engine=Mock(),
        capacity_cache=capacity,
        poll_interval_seconds=0,
    )
    sandbox = Mock(spec=Sandbox)
    remaining_claims = iter(claims)
    claimed = False
    cleaned_up = False

    def claim(*_args: object) -> bool:
        nonlocal claimed
        assert active_sessions == 1
        assert build_lock.held
        claimed = next(remaining_claims)

        return claimed

    @asynccontextmanager
    async def create() -> AsyncGenerator[Sandbox, None]:
        nonlocal cleaned_up
        assert active_sessions == 0
        assert build_lock.held
        assert claimed
        try:
            yield sandbox
        finally:
            cleaned_up = True

    @asynccontextmanager
    async def turn(*_args: object, **_kwargs: object) -> AsyncGenerator[Mock, None]:
        yield Mock(spec_set=["snapshot"], snapshot=await capacity.get())

    monkeypatch.setattr(admission, "Session", open_session)
    monkeypatch.setattr(admission, "task_build_lock", Mock(return_value=build_lock))
    monkeypatch.setattr(admission, "queue_pool_lock", Mock(return_value=pool_lock))
    monkeypatch.setattr(admission, "lock_execution_authority", Mock())
    monkeypatch.setattr(context.admission_turns, "turn", turn)
    monkeypatch.setattr(admission, "_is_waiting_without_lock", Mock(return_value=True))
    monkeypatch.setattr(admission, "_is_waiting", Mock(return_value=True))
    monkeypatch.setattr(admission, "_remaining_budget", Mock(return_value=None))
    monkeypatch.setattr(admission, "prepare_admission_candidate", Mock())
    monkeypatch.setattr(admission, "clear_admission_candidate", Mock())
    monkeypatch.setattr(admission, "claim_prepared_task", claim)
    monkeypatch.setattr(admission, "release_reservation", Mock())
    monkeypatch.setattr(admission, "_start_claimed_task", Mock(return_value=True))

    async with AsyncExitStack() as stack, AsyncExitStack() as preparation_stack:
        await preparation_stack.enter_async_context(context.preparation_slots)
        result = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=uuid4(),
            expected_started_at=datetime(2026, 7, 27, tzinfo=UTC),
            authority=ExecutionAuthority(benchmark_id=uuid4(), dispatch_id=uuid4()),
            source=Mock(),
            resources=Mock(),
            creation_state=SandboxCreationState(cleanup_pending=False),
            create=create,
            preparation_stack=preparation_stack,
        )

    assert result is sandbox
    assert cleaned_up
    assert not build_lock.held
    assert active_sessions == 0


@pytest.mark.parametrize("gpu_work", [False, True], ids=["cpu", "gpu"])
def test_unrelated_quota_exhaustion_does_not_block_zero_charge_dimensions(gpu_work: bool) -> None:
    demand = SandboxReservationDemand(
        target_id="region-a",
        sandbox_class="sysbox",
        vcpu=0 if gpu_work else 1,
        memory=0 if gpu_work else 2,
        disk=0 if gpu_work else 3,
        gpu=1 if gpu_work else 0,
    )
    capacity = SandboxCapacity(
        cpu=ResourceCapacity(total=8, used=8 if gpu_work else 0),
        memory=ResourceCapacity(total=16, used=16 if gpu_work else 0),
        disk=ResourceCapacity(total=24, used=24 if gpu_work else 0),
        gpu=ResourceCapacity(total=1, used=0 if gpu_work else 1),
    )
    held = store.ActiveReservations(
        count=1,
        vcpu=8 if gpu_work else 0,
        memory=16 if gpu_work else 0,
        disk=24 if gpu_work else 0,
        gpu=0 if gpu_work else 1,
    )

    assert store.fits_in_capacity(demand, capacity, held)
