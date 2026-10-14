"""Queued admission must not await build locks inside authority transactions."""

from collections.abc import AsyncGenerator, Generator
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID, uuid4

from benchmark_service import ImageSource, ResourceCapacity, Resources, SandboxCapacity, SandboxSource, SnapshotSource
import pytest

from tracker.database.models import TaskStatus
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.sandbox import SandboxCreationState
from tracker.scheduler import admission
from tracker.scheduler.store import reserved_queue_pool_id


@pytest.mark.parametrize("claims", [[True], [False, True]])
@pytest.mark.parametrize("eligibility", [[True], [False, True]], ids=["eligible", "wait_then_eligible"])
async def test_build_lock_waits_outside_authority_transactions(
    monkeypatch: pytest.MonkeyPatch, claims: list[bool], eligibility: list[bool]
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
    provider.reservable_demand = AsyncMock(return_value=Mock())
    context = admission.SandboxQueueContext(
        provider=provider, pool_id=reserved_queue_pool_id("pool"), engine=Mock(), poll_interval_seconds=0
    )
    sandbox = Mock()
    start = AsyncMock(return_value=sandbox)
    claim = Mock(side_effect=claims)
    eligible = Mock(side_effect=eligibility + [True] * (2 * len(claims)))
    reservations = Mock(return_value={})
    polled_while_ineligible = False

    async def poll(_seconds: float) -> None:
        nonlocal polled_while_ineligible
        if not eligibility[0] and eligible.call_count == 1:
            reservations.assert_not_called()
            provider.check_admission.assert_not_awaited()
            assert active_sessions == 0
            polled_while_ineligible = True

    monkeypatch.setattr(admission, "Session", open_session)
    monkeypatch.setattr(admission, "task_build_lock", Mock(return_value=build_lock))
    monkeypatch.setattr(admission, "queue_pool_lock", Mock(return_value=pool_lock))
    monkeypatch.setattr(admission, "_reset_abandoned_pool_builds", AsyncMock())
    monkeypatch.setattr(admission, "lock_execution_authority", Mock())
    monkeypatch.setattr(admission, "eligible_task_is", eligible)
    monkeypatch.setattr(admission, "_is_waiting", Mock(return_value=True))
    monkeypatch.setattr(admission, "reserved_builds", reservations)
    monkeypatch.setattr(admission.asyncio, "sleep", AsyncMock(side_effect=poll))
    monkeypatch.setattr(admission, "_reservable_capacity", AsyncMock(return_value=Mock()))
    monkeypatch.setattr(admission, "_fits", Mock(return_value=True))
    monkeypatch.setattr(admission, "claim_eligible_task", claim)
    monkeypatch.setattr(admission, "release_reservation", Mock())
    monkeypatch.setattr(admission, "_start_sandbox", start)
    async with AsyncExitStack() as stack:
        result = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=uuid4(),
            expected_started_at=datetime.now(UTC),
            authority=ExecutionAuthority(benchmark_id=uuid4(), dispatch_id=uuid4()),
            source=Mock(),
            resources=Mock(),
            creation_state=SandboxCreationState(cleanup_pending=False),
            create=Mock(),
        )
    assert result is sandbox
    assert claim.call_count == len(claims)
    assert polled_while_ineligible == (not eligibility[0])
    reservations.assert_called()
    start.assert_awaited_once()
    assert not build_lock.held


@pytest.mark.parametrize(
    ("source", "resources"),
    [
        (SnapshotSource(snapshot="snapshot"), Resources(vcpu=1, memory=2, disk=3)),
        (ImageSource(image="image"), Resources(vcpu=1, memory=2, disk=3, gpu=1)),
    ],
    ids=["snapshot", "gpu_image"],
)
async def test_unknown_demand_waits_for_reservation_drain_even_when_it_fits(
    monkeypatch: pytest.MonkeyPatch, source: SandboxSource, resources: Resources
) -> None:
    status = TaskStatus.PENDING
    reserved: dict[UUID, Resources] = {uuid4(): Resources(vcpu=1, memory=2, disk=3)}
    counters = {"claim": 0, "create": 0, "start": 0, "cleanup": 0}
    polled: list[tuple[TaskStatus, int]] = []
    pool_held = False

    @contextmanager
    def open_session(_bind: object) -> Generator[Mock, None, None]:
        yield Mock()

    async def acquire_pool() -> bool:
        nonlocal pool_held
        assert not pool_held
        pool_held = True
        return True

    async def release_pool(*_args: object) -> None:
        nonlocal pool_held
        assert pool_held
        pool_held = False

    async def drain_reservation(_seconds: float) -> None:
        nonlocal reserved
        polled.append((status, len(reserved)))
        assert counters == {"claim": 0, "create": 0, "start": 0, "cleanup": 0}
        assert not pool_held
        reserved = {}

    def get_reservations(*_args: object) -> dict[UUID, Resources]:
        return reserved

    def claim_task(*_args: object, **_kwargs: object) -> bool:
        nonlocal status
        counters["claim"] += 1
        status = TaskStatus.BUILDING
        return True

    def start_task(*_args: object, **_kwargs: object) -> bool:
        nonlocal status
        assert status == TaskStatus.BUILDING
        assert counters["create"] == 1
        counters["start"] += 1
        status = TaskStatus.IN_PROGRESS
        return True

    build_lock = Mock(held=False)
    build_lock.acquire = AsyncMock()
    build_lock.release = AsyncMock()
    pool_lock = MagicMock()
    pool_lock.__aenter__ = AsyncMock(side_effect=acquire_pool)
    pool_lock.__aexit__ = AsyncMock(side_effect=release_pool)
    provider = Mock()
    provider.check_admission = AsyncMock(return_value=True)
    provider.reservable_demand = AsyncMock(return_value=None)
    provider.get_capacity = AsyncMock(
        return_value=SandboxCapacity(
            cpu=ResourceCapacity(total=8, used=0),
            memory=ResourceCapacity(total=16, used=0),
            disk=ResourceCapacity(total=24, used=0),
        )
    )
    context = admission.SandboxQueueContext(
        provider=provider, pool_id=reserved_queue_pool_id("pool"), engine=Mock(), poll_interval_seconds=0
    )
    sandbox = Mock()
    creation_state = SandboxCreationState(cleanup_pending=False)

    @asynccontextmanager
    async def create() -> AsyncGenerator[Mock]:
        assert not reserved
        assert status == TaskStatus.BUILDING
        assert pool_held
        assert not build_lock.held
        counters["create"] += 1
        creation_state.cleanup_pending = True
        try:
            yield sandbox
        finally:
            counters["cleanup"] += 1
            creation_state.cleanup_pending = False

    monkeypatch.setattr(admission, "Session", open_session)
    monkeypatch.setattr(admission, "task_build_lock", Mock(return_value=build_lock))
    monkeypatch.setattr(admission, "queue_pool_lock", Mock(return_value=pool_lock))
    monkeypatch.setattr(admission, "_reset_abandoned_pool_builds", AsyncMock())
    monkeypatch.setattr(admission, "lock_execution_authority", Mock())
    monkeypatch.setattr(admission, "eligible_task_is", Mock(return_value=True))
    monkeypatch.setattr(admission, "reserved_builds", Mock(side_effect=get_reservations))
    monkeypatch.setattr(admission, "claim_eligible_task", Mock(side_effect=claim_task))
    monkeypatch.setattr(admission, "_start_claimed_task", Mock(side_effect=start_task))
    monkeypatch.setattr(admission.asyncio, "sleep", AsyncMock(side_effect=drain_reservation))

    async with AsyncExitStack() as stack:
        result = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=uuid4(),
            expected_started_at=datetime.now(UTC),
            authority=ExecutionAuthority(benchmark_id=uuid4(), dispatch_id=uuid4()),
            source=source,
            resources=resources,
            creation_state=creation_state,
            create=create,
        )
        assert result is sandbox
        assert status == TaskStatus.IN_PROGRESS
        assert not pool_held
        assert creation_state.cleanup_pending
        assert counters == {"claim": 1, "create": 1, "start": 1, "cleanup": 0}

    assert polled == [(TaskStatus.PENDING, 1)]
    assert not reserved
    assert counters == {"claim": 1, "create": 1, "start": 1, "cleanup": 1}
    assert not creation_state.cleanup_pending
    build_lock.acquire.assert_not_awaited()
