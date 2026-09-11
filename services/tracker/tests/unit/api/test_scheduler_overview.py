"""Tests for PostgreSQL-backed scheduler overview reads."""

import asyncio
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from benchmark_service import ResourceCapacity, SandboxCapacity, SandboxCapacityDomain
from fastapi.testclient import TestClient
import pytest
from sqlmodel import Session

from main import app
from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
import tracker.api.scheduler_overview as scheduler_overview_api
from tracker.api.scheduler_overview import _read_active_rows, read_scheduler_overview  # pyright: ignore[reportPrivateUsage]
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.scheduler.store import queue_pool_id
from tracker.types import SchedulerOverviewResponse, SchedulerPoolResponse, SchedulerSummaryResponse


_client = TestClient(app)
_PROVIDER_POOL_ID = "daytona:capacity-test"
_QUEUE_POOL_ID = queue_pool_id(_PROVIDER_POOL_ID)


def _queue(benchmark: Benchmark, *, pool_id: str, priority: int) -> Benchmark:
    benchmark.arguments = benchmark.arguments.model_copy(update={"priority": priority, "queue_pool_id": pool_id})

    return benchmark


def _provider_queue(
    benchmark: Benchmark,
    *,
    pool_id: str = _QUEUE_POOL_ID,
    secret_name: str | None = "provider-secret",
    aws_managed: bool = True,
) -> Benchmark:
    benchmark.aws_managed = aws_managed
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "priority": 3,
            "queue_pool_id": pool_id,
            "sandbox_provider": "daytona",
            "sandbox_provider_secret_name": secret_name,
        }
    )
    return benchmark


def _waiting_task(benchmark: Benchmark, task_id: str = "waiting") -> Task:
    return make_task(benchmark, task_id, status=TaskStatus.PENDING)


def _capacity_provider(
    monkeypatch: pytest.MonkeyPatch,
    result: list[SandboxCapacityDomain] | BaseException,
    *,
    pool_id: str = _PROVIDER_POOL_ID,
) -> tuple[Mock, AsyncMock]:
    capacity = AsyncMock(side_effect=result) if isinstance(result, BaseException) else AsyncMock(return_value=result)
    provider = Mock(admission_pool_id=pool_id, get_capacity_domains=capacity, close=AsyncMock())
    provider_config = Mock()
    provider_config.create_provider.return_value = provider
    fetch_config = AsyncMock(return_value=provider_config)
    monkeypatch.setattr(
        scheduler_overview_api,
        "deployment_aws_runtime",
        Mock(return_value=SimpleNamespace(clients=Mock())),
    )
    monkeypatch.setattr(scheduler_overview_api, "fetch_sandbox_provider_config_async", fetch_config)
    return provider, fetch_config


async def test_reports_org_queued_rows_in_priority_fifo_order(database_session: Session) -> None:
    now = datetime(2026, 7, 17, 12, tzinfo=UTC)
    pool_a, pool_b = "pool_a", "pool_b"
    other_org = Org(id=uuid4(), name="other")
    urgent_run = _queue(make_benchmark(name="urgent"), pool_id=pool_a, priority=0)
    second_run = _queue(make_benchmark(name="second"), pool_id=pool_b, priority=1)
    fifo_run = _queue(make_benchmark(name="fifo"), pool_id=pool_a, priority=3)
    direct = make_benchmark(name="direct")
    finished = _queue(make_benchmark(name="finished", status=BenchmarkStatus.FINISHED), pool_id=pool_a, priority=0)
    stopping = _queue(make_benchmark(name="stopping", status=BenchmarkStatus.STOPPING), pool_id=pool_a, priority=3)
    foreign = _queue(make_benchmark(name="foreign", org_id=other_org.id), pool_id=pool_a, priority=0)

    def task(
        benchmark: Benchmark,
        task_id: str,
        minutes: int,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Task:
        return make_task(benchmark, task_id, status=status, started_at=now - timedelta(minutes=minutes))

    foreign_first = task(foreign, "foreign-first", 20)
    urgent = task(urgent_run, "urgent", 15)
    second = task(second_run, "second", 10)
    fifo_first = task(fifo_run, "fifo-first", 5)
    fifo_first.id = UUID(int=1)
    fifo_second = task(fifo_run, "fifo-second", 5)
    fifo_second.id = UUID(int=2)
    stopping_active = task(stopping, "stopping-active", 32, TaskStatus.IN_PROGRESS)
    building = task(urgent_run, "building", 30, TaskStatus.BUILDING)
    running = task(second_run, "running", 25, TaskStatus.IN_PROGRESS)
    evaluating = task(fifo_run, "evaluating", 20, TaskStatus.EVALUATING)
    excluded = [
        task(finished, "finished-waiting", 25),
        task(finished, "finished-active", 35, TaskStatus.BUILDING),
        task(direct, "direct-waiting", 25),
        task(direct, "direct-active", 0, TaskStatus.IN_PROGRESS),
        task(foreign, "foreign-active", 0, TaskStatus.BUILDING),
    ]
    database_session.add_all(
        [
            other_org,
            urgent_run,
            second_run,
            fifo_run,
            direct,
            finished,
            stopping,
            foreign,
            foreign_first,
            urgent,
            second,
            fifo_first,
            fifo_second,
            stopping_active,
            building,
            running,
            evaluating,
            *excluded,
        ]
    )
    database_session.commit()

    overview = read_scheduler_overview(
        session=database_session,
        org_id=TEST_ORG_ID,
        now=now,
        waiting_limit=3,
        active_limit=3,
    )

    assert overview.observed_at == now
    assert overview.summary.model_dump() == {"waiting": 4, "building": 1, "in_progress": 2, "evaluating": 1}
    assert [(pool.pool_id, pool.waiting) for pool in overview.pools] == [(pool_a, 3), (pool_b, 1)]
    assert [
        (entry.external_task_id, entry.pool_id, entry.position, entry.priority, entry.enqueued_at)
        for entry in overview.waiting_entries
    ] == [
        (urgent.task_id, pool_a, 2, 0, urgent.started_at),
        (second.task_id, pool_b, 1, 1, second.started_at),
        (fifo_first.task_id, pool_a, 3, 3, fifo_first.started_at),
    ]
    assert [(entry.external_task_id, entry.status) for entry in overview.active_entries] == [
        (stopping_active.task_id, TaskStatus.IN_PROGRESS),
        (building.task_id, TaskStatus.BUILDING),
        (running.task_id, TaskStatus.IN_PROGRESS),
    ]
    assert overview.waiting_capped
    assert overview.active_capped


def test_active_read_refreshes_preloaded_task_state(database_session: Session) -> None:
    benchmark = _queue(make_benchmark(name="transitioning"), pool_id="pool_a", priority=3)
    task = make_task(benchmark, "transitioning", status=TaskStatus.PENDING)
    database_session.add_all([benchmark, task])
    database_session.commit()

    with Session(bind=database_session.bind) as writer:
        stored_task = writer.get(Task, task.id)
        assert stored_task is not None
        stored_task.status = TaskStatus.IN_PROGRESS
        writer.commit()

    rows, counts = _read_active_rows(session=database_session, org_id=TEST_ORG_ID, limit=100)

    assert counts == {TaskStatus.IN_PROGRESS: 1}
    assert [(row.task_id, row.status) for row, _benchmark in rows] == [(task.task_id, TaskStatus.IN_PROGRESS)]


def test_default_route_preserves_legacy_pool_shape(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _provider_queue(make_benchmark(started_by_email=None))
    database_session.add_all([benchmark, _waiting_task(benchmark)])
    database_session.commit()

    monkeypatch.setattr(
        scheduler_overview_api,
        "deployment_aws_runtime",
        Mock(side_effect=AssertionError("capacity opt-out must not resolve AWS")),
    )

    response = _client.get("/scheduler/overview")

    assert response.status_code == 200
    payload = response.json()
    assert payload["pools"] == [{"pool_id": _QUEUE_POOL_ID, "waiting": 1}]
    assert payload["waiting_entries"][0]["started_by_email"] is None


async def test_route_offloads_synchronous_database_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    overview_entered = Event()
    overview_release = Event()
    references_entered = Event()
    references_release = Event()
    timed_out_reads: list[str] = []
    overview = SchedulerOverviewResponse(
        observed_at=datetime.now(UTC),
        summary=SchedulerSummaryResponse(),
        pools=[],
        waiting_entries=[],
        active_entries=[],
        waiting_capped=False,
        active_capped=False,
    )

    def blocking_overview_read(**_kwargs: object) -> SchedulerOverviewResponse:
        overview_entered.set()
        if not overview_release.wait(timeout=1):
            timed_out_reads.append("overview")
        return overview

    def blocking_reference_read(**_kwargs: object) -> dict[str, set[object]]:
        references_entered.set()
        if not references_release.wait(timeout=1):
            timed_out_reads.append("references")
        return {}

    async def release_from_event_loop() -> None:
        while not overview_entered.is_set():
            await asyncio.sleep(0)
        overview_release.set()
        while not references_entered.is_set():
            await asyncio.sleep(0)
        references_release.set()

    enrich = AsyncMock(return_value=overview)
    monkeypatch.setattr(scheduler_overview_api, "read_scheduler_overview", blocking_overview_read)
    monkeypatch.setattr(scheduler_overview_api, "_read_waiting_pool_references", blocking_reference_read)
    monkeypatch.setattr(scheduler_overview_api, "_enrich_scheduler_capacity", enrich)

    result, _ = await asyncio.gather(
        scheduler_overview_api.get_scheduler_overview(
            waiting_limit=100,
            active_limit=100,
            include_capacity=True,
            org=Org(id=TEST_ORG_ID, name="test"),
            session=cast(Session, Mock()),
        ),
        release_from_event_loop(),
    )

    assert result is overview
    assert timed_out_reads == []
    enrich.assert_awaited_once_with(overview, org_id=TEST_ORG_ID, references={})


def test_capacity_route_projects_provider_values_and_uses_complete_pool_references(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _provider_queue(make_benchmark(name="first"))
    second = _provider_queue(make_benchmark(name="second"))
    database_session.add_all([first, second, _waiting_task(first, "first"), _waiting_task(second, "second")])
    database_session.commit()
    provider, fetch_config = _capacity_provider(
        monkeypatch,
        [
            SandboxCapacityDomain(
                target_id="region-b",
                sandbox_class="linux-vm",
                capacity=SandboxCapacity(
                    cpu=ResourceCapacity(total=16, used=3.5),
                    memory=ResourceCapacity(total=64, used=8),
                    disk=ResourceCapacity(total=100, used=25),
                ),
            ),
            SandboxCapacityDomain(
                target_id="region-a",
                sandbox_class="container",
                capacity=SandboxCapacity(
                    cpu=ResourceCapacity(total=8, used=2),
                    memory=ResourceCapacity(total=32, used=4),
                    disk=ResourceCapacity(total=50, used=10),
                ),
            ),
        ],
    )

    response = _client.get(
        "/scheduler/overview",
        params={"include_capacity": "true", "waiting_limit": 1},
    )

    assert response.status_code == 200
    (pool,) = response.json()["pools"]
    domains = pool.pop("capacity_domains")
    assert pool == {"pool_id": _QUEUE_POOL_ID, "waiting": 2, "provider": "daytona"}
    assert [(domain["target_id"], domain["sandbox_class"]) for domain in domains] == [
        ("region-b", "linux-vm"),
        ("region-a", "container"),
    ]
    assert domains[0]["capacity"]["cpu"] == {"available": 12.5, "total": 16.0}
    assert domains[1]["capacity"]["disk"] == {"available": 40.0, "total": 50.0}
    fetch_config.assert_awaited_once()
    provider.get_capacity_domains.assert_awaited_once()
    provider.close.assert_awaited_once()


def test_access_key_and_ambiguous_pools_never_use_deployment_aws(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    access_key = _provider_queue(make_benchmark(name="access-key"), aws_managed=False)
    managed = _provider_queue(make_benchmark(name="managed"))
    first = _provider_queue(make_benchmark(name="first"), pool_id="pool_ambiguous", secret_name="first-secret")
    second = _provider_queue(make_benchmark(name="second"), pool_id="pool_ambiguous", secret_name="second-secret")
    database_session.add_all(
        [
            access_key,
            managed,
            first,
            second,
            _waiting_task(access_key, "access-key"),
            _waiting_task(managed, "managed"),
            _waiting_task(first, "first"),
            _waiting_task(second, "second"),
        ]
    )
    database_session.commit()
    deployment_runtime = Mock(side_effect=AssertionError("unsafe deployment AWS access"))
    monkeypatch.setattr(scheduler_overview_api, "deployment_aws_runtime", deployment_runtime)

    response = _client.get("/scheduler/overview", params={"include_capacity": "true"})

    assert response.status_code == 200
    assert response.json()["pools"] == [
        {"pool_id": "pool_ambiguous", "waiting": 2, "provider": "daytona", "capacity_domains": None},
        {"pool_id": _QUEUE_POOL_ID, "waiting": 2, "provider": "daytona", "capacity_domains": None},
    ]
    deployment_runtime.assert_not_called()


async def test_capacity_timeout_closes_provider_and_pool_drift_skips_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _fetch_config = _capacity_provider(
        monkeypatch,
        AssertionError("drifted provider must not be observed"),
        pool_id="different-provider-pool",
    )

    drifted = await scheduler_overview_api._read_provider_capacity(  # pyright: ignore[reportPrivateUsage]
        org_id=TEST_ORG_ID,
        pool_id=_QUEUE_POOL_ID,
        provider_type="daytona",
        secret_name="provider-secret",
    )

    assert drifted is None
    provider.get_capacity_domains.assert_not_awaited()
    provider.close.assert_awaited_once()

    provider.admission_pool_id = _PROVIDER_POOL_ID
    provider.get_capacity_domains = AsyncMock(return_value=[])
    provider.close.reset_mock()

    empty = await scheduler_overview_api._read_provider_capacity(  # pyright: ignore[reportPrivateUsage]
        org_id=TEST_ORG_ID,
        pool_id=_QUEUE_POOL_ID,
        provider_type="daytona",
        secret_name="provider-secret",
    )

    assert empty == []
    provider.get_capacity_domains.assert_awaited_once()
    provider.close.assert_awaited_once()

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    provider.get_capacity_domains = AsyncMock(side_effect=wait_forever)
    provider.close.reset_mock()
    monkeypatch.setattr(scheduler_overview_api, "_CAPACITY_TIMEOUT_SECONDS", 0.01)

    timed_out = await scheduler_overview_api._read_provider_capacity(  # pyright: ignore[reportPrivateUsage]
        org_id=TEST_ORG_ID,
        pool_id=_QUEUE_POOL_ID,
        provider_type="daytona",
        secret_name="provider-secret",
    )

    assert timed_out is None
    provider.get_capacity_domains.assert_awaited_once()
    provider.close.assert_awaited_once()


async def test_capacity_enrichment_bounds_concurrent_provider_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    active = 0
    peak = 0
    cancelled = 0

    async def blocking_provider_config(*_args: object) -> None:
        nonlocal active, peak, cancelled
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1
            cancelled += 1

    pool_ids = [f"pool_{index}" for index in range(6)]
    overview = SchedulerOverviewResponse(
        observed_at=datetime.now(UTC),
        summary=SchedulerSummaryResponse(waiting=len(pool_ids)),
        pools=[SchedulerPoolResponse(pool_id=pool_id, waiting=1) for pool_id in pool_ids],
        waiting_entries=[],
        active_entries=[],
        waiting_capped=False,
        active_capped=False,
    )
    references = {
        pool_id: {
            scheduler_overview_api._PoolProviderReference(  # pyright: ignore[reportPrivateUsage]
                aws_managed=True,
                provider_type="daytona",
                secret_name=f"secret-{index}",
            )
        }
        for index, pool_id in enumerate(pool_ids)
    }
    monkeypatch.setattr(scheduler_overview_api, "_CAPACITY_MAX_CONCURRENCY", 2)
    monkeypatch.setattr(scheduler_overview_api, "_CAPACITY_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        scheduler_overview_api,
        "deployment_aws_runtime",
        Mock(return_value=SimpleNamespace(clients=Mock())),
    )
    monkeypatch.setattr(
        scheduler_overview_api,
        "fetch_sandbox_provider_config_async",
        blocking_provider_config,
    )

    result = await scheduler_overview_api._enrich_scheduler_capacity(  # pyright: ignore[reportPrivateUsage]
        overview,
        org_id=TEST_ORG_ID,
        references=references,
    )

    assert peak == 2
    assert active == 0
    assert cancelled == 2
    assert [pool.capacity_domains for pool in result.pools] == [None] * len(pool_ids)


def test_capacity_failure_keeps_overview_available(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _provider_queue(make_benchmark())
    database_session.add_all([benchmark, _waiting_task(benchmark)])
    database_session.commit()
    provider, _fetch_config = _capacity_provider(monkeypatch, RuntimeError("provider unavailable"))

    response = _client.get("/scheduler/overview", params={"include_capacity": "true"})

    assert response.status_code == 200
    assert response.json()["pools"] == [
        {"pool_id": _QUEUE_POOL_ID, "waiting": 1, "provider": "daytona", "capacity_domains": None}
    ]
    provider.close.assert_awaited_once()


async def test_provider_close_timeout_cancels_and_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def close() -> None:
        close_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            close_finished.set()

    provider = Mock()
    provider.close = AsyncMock(side_effect=close)
    monkeypatch.setattr(scheduler_overview_api, "_PROVIDER_CLOSE_TIMEOUT_SECONDS", 0.01)

    await asyncio.wait_for(
        scheduler_overview_api._close_provider(provider, _QUEUE_POOL_ID),  # pyright: ignore[reportPrivateUsage]
        timeout=0.2,
    )

    assert close_started.is_set()
    assert close_finished.is_set()
    provider.close.assert_awaited_once()


async def test_provider_close_is_terminal_before_caller_cancellation_propagates() -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def close() -> None:
        close_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            close_finished.set()

    provider = Mock()
    provider.close = AsyncMock(side_effect=close)
    closing = asyncio.create_task(
        scheduler_overview_api._close_provider(provider, _QUEUE_POOL_ID)  # pyright: ignore[reportPrivateUsage]
    )
    await close_started.wait()

    closing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await closing
    assert close_finished.is_set()
    provider.close.assert_awaited_once()
