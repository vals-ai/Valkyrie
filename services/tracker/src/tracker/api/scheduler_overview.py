"""Read-only snapshot of queued and active sandbox work."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxProvider
from fastapi import APIRouter, Depends, Query
from sqlalchemy import JSON, select as sa_select, type_coerce
from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import Session, col, func, select
from sqlmodel.sql.expression import Select
from starlette.concurrency import run_in_threadpool

from tracker.auth import get_current_org
from tracker.aws.resolver import deployment_aws_runtime
from tracker.aws.secrets import SecretsManagerStore
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.scheduler.store import queue_pool_id
from tracker.types import (
    SchedulerActiveEntryResponse,
    SchedulerActiveStatus,
    SchedulerCapacityResponse,
    SchedulerOverviewResponse,
    SchedulerPoolResponse,
    SchedulerResourceCapacityResponse,
    SchedulerSummaryResponse,
    SchedulerWaitingEntryResponse,
)
from tracker.utils.resources import fetch_sandbox_provider_config_async

router = APIRouter(prefix="/scheduler")
logger = get_logger(__name__)

_ACTIVE_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_CAPACITY_TIMEOUT_SECONDS = 2.0
_PROVIDER_CLOSE_TIMEOUT_SECONDS = 1.0
_CAPACITY_MAX_CONCURRENCY = 4


@dataclass(frozen=True)
class _PoolProviderReference:
    aws_managed: bool
    provider_type: str | None
    secret_name: str | None


def _queued_benchmarks_expression():
    arguments = type_coerce(col(Benchmark.arguments), JSON)

    return arguments["queue_pool_id"].as_string().is_not(None)


def _read_waiting_rows(
    *,
    session: Session,
    org_id: UUID,
    limit: int,
) -> tuple[list[tuple[Task, Benchmark, int, str, int]], dict[str, int]]:
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    pool_id = arguments["queue_pool_id"].as_string()
    queued_tasks = (
        sa_select(
            cast(ColumnElement[UUID], col(Task.id).label("task_id")),
            priority.label("priority"),
            pool_id.label("pool_id"),
            func.row_number()
            .over(
                partition_by=pool_id,
                order_by=(priority.asc(), col(Task.started_at).asc(), col(Task.id).asc()),
            )
            .label("position"),
        )
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(
            col(Task.status) == TaskStatus.PENDING,
            col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            _queued_benchmarks_expression(),
        )
        .subquery()
    )
    scope = (col(Task.org_id) == org_id, col(Benchmark.org_id) == org_id)
    pool_counts_statement = cast(
        Select[tuple[str, int]],
        sa_select(queued_tasks.c.pool_id, func.count(queued_tasks.c.task_id))
        .join(Task, col(Task.id) == queued_tasks.c.task_id)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .group_by(queued_tasks.c.pool_id),
    )
    pool_counts = dict(session.exec(pool_counts_statement).all())
    rows_statement = cast(
        Select[tuple[Task, Benchmark, int, str, int]],
        sa_select(
            Task,
            Benchmark,
            queued_tasks.c.priority,
            queued_tasks.c.pool_id,
            queued_tasks.c.position,
        )
        .join(queued_tasks, col(Task.id) == queued_tasks.c.task_id)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .order_by(queued_tasks.c.priority.asc(), col(Task.started_at).asc(), col(Task.id).asc())
        .limit(limit + 1),
    )
    rows = list(session.exec(rows_statement).all())

    return rows, pool_counts


def _read_waiting_pool_references(
    *,
    session: Session,
    org_id: UUID,
) -> dict[str, set[_PoolProviderReference]]:
    """Return every distinct provider reference contributing waiting work."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    pool_id = arguments["queue_pool_id"].as_string()
    provider_type = arguments["sandbox_provider"].as_string()
    secret_name = arguments["sandbox_provider_secret_name"].as_string()
    statement = cast(
        Select[tuple[str, bool, str | None, str | None]],
        sa_select(
            pool_id,
            col(Benchmark.aws_managed),
            provider_type,
            secret_name,
        )
        .select_from(Task)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(
            col(Task.org_id) == org_id,
            col(Benchmark.org_id) == org_id,
            col(Task.status) == TaskStatus.PENDING,
            col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
            _queued_benchmarks_expression(),
        )
        .distinct(),
    )
    references: dict[str, set[_PoolProviderReference]] = {}
    for stored_pool_id, aws_managed, stored_provider_type, stored_secret_name in session.exec(statement).all():
        references.setdefault(stored_pool_id, set()).add(
            _PoolProviderReference(
                aws_managed=aws_managed,
                provider_type=stored_provider_type,
                secret_name=stored_secret_name,
            )
        )

    return references


def _read_active_rows(
    *,
    session: Session,
    org_id: UUID,
    limit: int,
) -> tuple[list[tuple[Task, Benchmark]], dict[TaskStatus, int]]:
    scope = (
        col(Task.org_id) == org_id,
        col(Benchmark.org_id) == org_id,
        col(Benchmark.status).in_((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)),
        col(Task.status).in_(_ACTIVE_STATUSES),
        _queued_benchmarks_expression(),
    )
    counts = dict(
        session.exec(
            select(col(Task.status), func.count(col(Task.id)))
            .select_from(Task)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(*scope)
            .group_by(col(Task.status))
        ).all()
    )
    rows = session.exec(
        select(Task, Benchmark)
        .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
        .where(*scope)
        .order_by(col(Task.started_at).asc(), col(Task.id).asc())
        .limit(limit + 1)
        .execution_options(populate_existing=True)
    ).all()

    return list(rows), counts


def read_scheduler_overview(
    *,
    session: Session,
    org_id: UUID,
    now: datetime,
    waiting_limit: int,
    active_limit: int,
) -> SchedulerOverviewResponse:
    waiting_rows, pool_counts = _read_waiting_rows(session=session, org_id=org_id, limit=waiting_limit)
    active_rows, active_counts = _read_active_rows(session=session, org_id=org_id, limit=active_limit)
    waiting_entries = [
        SchedulerWaitingEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            pool_id=pool_id,
            position=position,
            priority=priority,
            enqueued_at=task.started_at,
        )
        for task, benchmark, priority, pool_id, position in waiting_rows[:waiting_limit]
    ]
    active_entries = [
        SchedulerActiveEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            status=SchedulerActiveStatus(task.status.value),
            started_at=task.started_at,
        )
        for task, benchmark in active_rows[:active_limit]
    ]

    return SchedulerOverviewResponse(
        observed_at=now,
        summary=SchedulerSummaryResponse(
            waiting=sum(pool_counts.values()),
            building=active_counts.get(TaskStatus.BUILDING, 0),
            in_progress=active_counts.get(TaskStatus.IN_PROGRESS, 0),
            evaluating=active_counts.get(TaskStatus.EVALUATING, 0),
        ),
        pools=[
            SchedulerPoolResponse(pool_id=pool_id, waiting=waiting) for pool_id, waiting in sorted(pool_counts.items())
        ],
        waiting_entries=waiting_entries,
        active_entries=active_entries,
        waiting_capped=len(waiting_rows) > waiting_limit,
        active_capped=len(active_rows) > active_limit,
    )


def _resource_capacity_response(available: float, total: float) -> SchedulerResourceCapacityResponse:
    return SchedulerResourceCapacityResponse(available=available, total=total)


def _capacity_response(capacity: SandboxCapacity) -> SchedulerCapacityResponse:
    return SchedulerCapacityResponse(
        cpu=_resource_capacity_response(capacity.cpu.available, capacity.cpu.total),
        memory=_resource_capacity_response(capacity.memory.available, capacity.memory.total),
        disk=_resource_capacity_response(capacity.disk.available, capacity.disk.total),
    )


async def _cancel_and_drain(task: asyncio.Task[None]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _close_provider(provider: SandboxProvider, pool_id: str) -> None:
    close_task = asyncio.create_task(provider.close())
    try:
        async with asyncio.timeout(_PROVIDER_CLOSE_TIMEOUT_SECONDS):
            await asyncio.shield(close_task)
    except TimeoutError:
        await _cancel_and_drain(close_task)
        logger.warning("Sandbox capacity provider close timed out for pool %s (%s)", pool_id, TimeoutError.__name__)
    except asyncio.CancelledError:
        await _cancel_and_drain(close_task)
        raise
    except Exception as error:
        logger.warning("Sandbox capacity provider close failed for pool %s (%s)", pool_id, type(error).__name__)


async def _read_provider_capacity(
    *,
    org_id: UUID,
    pool_id: str,
    provider_type: str,
    secret_name: str,
) -> SchedulerCapacityResponse | None:
    provider: SandboxProvider | None = None
    try:
        async with asyncio.timeout(_CAPACITY_TIMEOUT_SECONDS):
            try:
                runtime = deployment_aws_runtime(org_id)
                provider_config = await fetch_sandbox_provider_config_async(
                    secret_name,
                    SecretsManagerStore(runtime.clients),
                    provider_type,
                )
                created_provider = provider_config.create_provider()
                provider = created_provider
                provider_pool_id = created_provider.admission_pool_id
                if provider_pool_id is None or queue_pool_id(provider_pool_id) != pool_id:
                    raise ValueError("Sandbox capacity provider does not match the queued pool")
                capacity = await created_provider.get_capacity()
                if capacity is None:
                    return None
                return _capacity_response(capacity)
            finally:
                if provider is not None:
                    await _close_provider(provider, pool_id)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.warning("Sandbox capacity unavailable for pool %s (%s)", pool_id, type(error).__name__)
        return None


def _capacity_request(
    references: set[_PoolProviderReference],
) -> tuple[str | None, tuple[str, str] | None]:
    provider_types = {reference.provider_type for reference in references if reference.provider_type}
    provider_type = next(iter(provider_types)) if len(provider_types) == 1 else None
    if not references or any(not reference.aws_managed for reference in references):
        return provider_type, None

    configurations = {(reference.provider_type, reference.secret_name) for reference in references}
    if len(configurations) != 1:
        return provider_type, None
    configured_provider_type, secret_name = next(iter(configurations))
    if configured_provider_type is None or not secret_name:
        return provider_type, None
    return provider_type, (configured_provider_type, secret_name)


async def _enrich_scheduler_capacity(
    overview: SchedulerOverviewResponse,
    *,
    org_id: UUID,
    references: dict[str, set[_PoolProviderReference]],
) -> SchedulerOverviewResponse:
    pools: list[SchedulerPoolResponse] = []
    requests: asyncio.Queue[tuple[int, str, str]] = asyncio.Queue()
    for index, pool in enumerate(overview.pools):
        provider_type, request = _capacity_request(references.get(pool.pool_id, set()))
        pools.append(pool.model_copy(update={"provider": provider_type, "capacity": None}))
        if request is not None:
            requests.put_nowait((index, request[0], request[1]))

    async def enrich() -> None:
        while True:
            try:
                index, provider_type, secret_name = requests.get_nowait()
            except asyncio.QueueEmpty:
                return
            pool = pools[index]
            capacity = await _read_provider_capacity(
                org_id=org_id,
                pool_id=pool.pool_id,
                provider_type=provider_type,
                secret_name=secret_name,
            )
            pools[index] = pool.model_copy(update={"capacity": capacity})

    workers = [asyncio.create_task(enrich()) for _ in range(min(_CAPACITY_MAX_CONCURRENCY, requests.qsize()))]
    try:
        async with asyncio.timeout(_CAPACITY_TIMEOUT_SECONDS):
            await asyncio.gather(*workers)
    except TimeoutError:
        pass
    finally:
        for worker in workers:
            if not worker.done():
                worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    return overview.model_copy(update={"pools": pools})


@router.get("/overview", response_model=SchedulerOverviewResponse, response_model_exclude_unset=True)
async def get_scheduler_overview(
    waiting_limit: int = Query(default=100, ge=1, le=200),
    active_limit: int = Query(default=100, ge=1, le=200),
    include_capacity: bool = Query(default=False),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SchedulerOverviewResponse:
    overview = await run_in_threadpool(
        read_scheduler_overview,
        session=session,
        org_id=org.id,
        now=datetime.now(UTC),
        waiting_limit=waiting_limit,
        active_limit=active_limit,
    )
    if not include_capacity:
        return overview
    references = await run_in_threadpool(_read_waiting_pool_references, session=session, org_id=org.id)
    return await _enrich_scheduler_capacity(
        overview,
        org_id=org.id,
        references=references,
    )
