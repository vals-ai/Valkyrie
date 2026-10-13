"""Run with `uv run pytest tests/integration/local/database/test_scheduler.py`.

Exercise PostgreSQL-backed sandbox scheduling against disposable PostgreSQL.
"""

import asyncio
import threading
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import httpx
from benchmark_service import (
    DaytonaProviderConfig,
    ImageSource,
    ResourceCapacity,
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
)
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse, VerifyTaskIdsResponse
from fastapi import HTTPException, Request
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, create_engine, func, select

from tests.factories import make_task
from tracker.secret_access import tenant_secret_prefix
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    AWSBenchmarkArguments,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    ExecutorDispatchStatus,
    Org,
    OrgProviderSecret,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.exceptions import SandboxSetupError
import tracker.executor.dispatch_control as dispatch_control
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.release_control import promote_release
import tracker.scheduler.admission as admission
import tracker.scheduler.store as store
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.types import StartBenchmarkRequest
from tracker.utils import task_execution
import tracker.utils.run_orchestration as run_orchestration
import main as tracker_main

_ATTEMPT = datetime(2026, 7, 27, 12)
_SOURCE = ImageSource(image="scheduler-test-image")
_RESOURCES = Resources(vcpu=1, memory=2, disk=3)
_CAPACITY = SandboxCapacity(
    cpu=ResourceCapacity(total=8, used=0),
    memory=ResourceCapacity(total=16, used=0),
    disk=ResourceCapacity(total=24, used=0),
)


def _use_managed_runtime(monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> None:
    monkeypatch.setattr(
        tracker_main,
        "resolve_run_aws_runtime",
        Mock(return_value=aws_runtime),
    )
    monkeypatch.setattr(tracker_main, "resolve_start_aws_runtime", Mock(return_value=aws_runtime))


def _use_real_sandbox_recovery(service: AsyncMock) -> None:
    async def run(**kwargs: Any) -> Any:
        return await BenchmarkServiceClient.run_with_sandbox_recovery(
            cast(BenchmarkServiceClient, service),
            **kwargs,
        )

    service.run_with_sandbox_recovery.side_effect = run


class MockProvider:
    def __init__(
        self,
        pool_id: str,
        events: list[str],
        *,
        capacity: Sequence[bool] = (True,),
        on_check: Callable[[], Awaitable[None]] | None = None,
        snapshot: SandboxCapacity | Exception = _CAPACITY,
    ) -> None:
        self.admission_pool_id = pool_id
        self.events = events
        self.capacity = iter(capacity)
        self.on_check = on_check
        self.snapshot = snapshot
        self.sources: list[SandboxSource] = []

    async def check_admission(self, source: SandboxSource, _resources: Resources) -> bool:
        self.events.append("capacity")
        self.sources.append(source)
        if self.on_check:
            await self.on_check()
        return next(self.capacity, True)

    async def get_capacity(self) -> SandboxCapacity:
        self.events.append("read_capacity")
        if isinstance(self.snapshot, Exception):
            raise self.snapshot
        return self.snapshot

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        self.events.append("create")
        return cast(Sandbox, Mock(id="scheduler-sandbox", name=request.name, labels=request.labels))

    async def delete_sandbox(self, _sandbox_id: str) -> None:
        self.events.append("cleanup")

    async def get_sandbox(self, _sandbox_id: str) -> Sandbox:
        raise SandboxNotFoundError("No abandoned sandbox")


def _run(
    session: Session,
    pool_id: str,
    tasks: Sequence[tuple[str, TaskStatus, datetime]],
    *,
    priority: int = 3,
    concurrency: int = 1,
) -> tuple[Org, Benchmark, list[Task]]:
    org = Org(id=uuid4(), name=f"scheduler-{uuid4()}")
    benchmark = Benchmark(
        org_id=org.id,
        name=f"run-{uuid4()}",
        arguments=AWSBenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            concurrency=concurrency,
            priority=priority,
            queue_pool_id=pool_id,
            sandbox_provider_secret_name=f"{tenant_secret_prefix(org.id)}provider",
        ),
    )
    rows = [
        make_task(benchmark, task_id, status=status, started_at=started_at) for task_id, status, started_at in tasks
    ]
    session.add(org)
    session.flush()
    session.add(benchmark)
    session.add_all(rows)
    session.commit()
    return org, benchmark, rows


def _update_task(engine: Engine, task: Task, status: TaskStatus, started_at: datetime) -> None:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        row.status = status
        row.started_at = started_at
        session.commit()


def _task(engine: Engine, task: Task) -> Task:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        return row


def _set_concurrency(engine: Engine, benchmark: Benchmark, concurrency: int) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.arguments = row.arguments.model_copy(update={"concurrency": concurrency})
        session.commit()


def _revoke_dispatch(engine: Engine, authority: ExecutionAuthority) -> None:
    with Session(engine) as session:
        dispatch = session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FAILED
        session.commit()


def _sandbox(
    events: list[str],
    *,
    creation_state: SandboxCreationState,
    name: str = "sandbox",
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def context() -> AsyncGenerator[Sandbox]:
        creation_state.cleanup_pending = True
        events.append("create")
        if on_create:
            await on_create()
        sandbox = Mock(id=f"sandbox-{len(events)}", name=name)
        try:
            yield cast(Sandbox, sandbox)
        finally:
            if on_cleanup:
                await on_cleanup()
            events.append("cleanup")
            creation_state.cleanup_pending = False

    return context


def _context(
    engine: Engine,
    provider_pool_id: str,
    events: list[str],
    *,
    capacity: Sequence[bool] = (True,),
    on_check: Callable[[], Awaitable[None]] | None = None,
    snapshot: SandboxCapacity | Exception = _CAPACITY,
    reserved: bool = False,
) -> admission.SandboxQueueContext:
    provider = MockProvider(provider_pool_id, events, capacity=capacity, on_check=on_check, snapshot=snapshot)
    context = admission.SandboxQueueContext(
        provider=cast(SandboxProvider, provider),
        pool_id=store.queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=0,
    )
    return context.for_queue(store.reserved_queue_pool_id(context.pool_id)) if reserved else context


def _reservation_count(engine: Engine, pool_id: str) -> int:
    with Session(engine) as session:
        return store.active_reservations(session, pool_id).count


async def _build_lock_is_free(engine: Engine, task: Task) -> bool:
    lock = store.task_build_lock(engine, task.id)
    if not await lock.acquire():
        return False
    await lock.release()
    return True


async def _pool_lock_is_free(engine: Engine, pool_id: str) -> bool:
    async with store.queue_pool_lock(engine, pool_id) as acquired:
        return acquired


def test_queue_context_requires_managed_provider() -> None:
    provider = Mock(admission_pool_id=None)

    with pytest.raises(ValueError, match="does not support queued admission"):
        admission.create_queue_context(
            engine=cast(Engine, Mock()),
            provider=cast(SandboxProvider, provider),
        )


@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_http_admission_waits_for_real_postgres_row_lock_without_blocking_loop(
    operation: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        store.queue_pool_id(f"daytona:{uuid4()}"),
        [("locked-admission", TaskStatus.STOPPED, _ATTEMPT)],
    )
    executor_authority(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add(benchmark)
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    identity = RequestIdentity(org=org)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_starter, lambda: identity)
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["new-task"])

    async def close(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def copy_agent(*_args: Any, **_kwargs: Any) -> None:
        return None

    enqueued: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        enqueued.append(dispatch.id)

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "close", close)
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    lock_entered = threading.Event()
    if operation == "retry":
        original_lock = tracker_main.lock_executor_admission

        def observed_recovery_lock(session: Session) -> object:
            session.connection().execute(text("SET LOCAL lock_timeout = '5s'"))
            lock_entered.set()
            return original_lock(session)

        monkeypatch.setattr(tracker_main, "lock_executor_admission", observed_recovery_lock)
        url = f"/retry-or-resume-benchmark/{benchmark.id}"
        body: dict[str, Any] = {}
    else:
        original_select_active_release = dispatch_control.select_active_release

        def observed_start_lock(session: Session, *, for_update: bool = False) -> ExecutorRelease:
            if for_update:
                session.connection().execute(text("SET LOCAL lock_timeout = '5s'"))
                lock_entered.set()
            return original_select_active_release(session, for_update=for_update)

        monkeypatch.setattr(dispatch_control, "select_active_release", observed_start_lock)
        url = "/start-benchmark"
        body = StartBenchmarkRequest(
            benchmark_name=f"postgres-lock-{uuid4()}",
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            task_ids=["new-task"],
            properties=aws_runtime.resources,
            sandbox_provider="daytona",
        ).model_dump(mode="json")
        postgres_session.add(OrgProviderSecret(org_id=org.id, provider="daytona", secret_name="provider"))
        postgres_session.commit()

    request: asyncio.Task[httpx.Response] | None = None
    with Session(postgres_engine) as blocker:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test"
        ) as client:
            try:
                blocker.exec(select(ExecutorAdmission).with_for_update()).one()
                request = asyncio.create_task(client.post(url, json=body))
                assert await asyncio.to_thread(lock_entered.wait, 5)
                assert not request.done()
                health = await asyncio.wait_for(client.get("/health"), timeout=2)
                assert health.status_code == 200
                blocker.rollback()
                response = await asyncio.wait_for(request, timeout=5)
            finally:
                blocker.rollback()
                if request is not None:
                    await asyncio.wait_for(
                        asyncio.gather(request, return_exceptions=True),
                        timeout=5,
                    )

    assert response.status_code == 200, response.text
    assert len(enqueued) == 1


@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_http_admission_generates_payload_key_without_holding_admission_lock(
    operation: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        store.queue_pool_id(f"daytona:{uuid4()}"),
        [("payload-key", TaskStatus.STOPPED, _ATTEMPT)],
    )
    executor_authority(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add(benchmark)
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    identity = RequestIdentity(org=org)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_starter, lambda: identity)
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["new-task"])

    async def close(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def copy_agent(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def enqueue(_dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "close", close)
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    original_generate_payload_key = tracker_main.generate_payload_key
    admission_lockable_during_key_generation: list[bool] = []

    def generate_payload_key(dispatch_id: UUID) -> Any:
        # Another replica must be able to admit work while this request waits on KMS.
        with Session(postgres_engine) as other_replica:
            other_replica.exec(select(ExecutorAdmission).with_for_update(nowait=True)).one()
            admission_lockable_during_key_generation.append(True)
        return original_generate_payload_key(dispatch_id)

    monkeypatch.setattr(tracker_main, "generate_payload_key", generate_payload_key)
    if operation == "retry":
        url = f"/retry-or-resume-benchmark/{benchmark.id}"
        body: dict[str, Any] = {}
    else:
        url = "/start-benchmark"
        body = StartBenchmarkRequest(
            benchmark_name=f"payload-key-{uuid4()}",
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            task_ids=["new-task"],
            properties=aws_runtime.resources,
            sandbox_provider="daytona",
        ).model_dump(mode="json")
        postgres_session.add(OrgProviderSecret(org_id=org.id, provider="daytona", secret_name="daytona"))
        postgres_session.commit()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test") as client:
        response = await asyncio.wait_for(client.post(url, json=body), timeout=10)

    assert response.status_code == 200, response.text
    assert admission_lockable_during_key_generation == [True]


async def test_targeted_snapshot_reaches_admission_and_creation_unchanged(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    source = TargetedSnapshotSource(snapshot="snapshot", target="us-west-3")
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("targeted", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    creation_state = SandboxCreationState(cleanup_pending=False)
    async with AsyncExitStack() as stack:
        sandbox = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=task.id,
            expected_started_at=task.started_at,
            authority=authority,
            source=source,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state),
        )

        assert sandbox is not None
        provider = cast(MockProvider, context.provider)
        assert provider.sources == [source]
        assert provider.sources[0] is source
        assert events == ["capacity", "create"]


async def _enter(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    events: list[str],
    authority: Any,
    *,
    source: SandboxSource = _SOURCE,
    resources: Resources = _RESOURCES,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    creation_state = SandboxCreationState(cleanup_pending=False)
    return await admission.enter_queued_sandbox(
        stack=stack,
        context=context,
        task_row_id=task.id,
        expected_started_at=task.started_at,
        authority=authority,
        source=source,
        resources=resources,
        creation_state=creation_state,
        create=_sandbox(events, creation_state=creation_state, on_create=on_create, on_cleanup=on_cleanup),
    )


async def _enter_provider_sandbox(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    authority: ExecutionAuthority,
    creation_state: SandboxCreationState,
    creation_semaphore: Semaphore,
) -> Sandbox | None:
    def create() -> AbstractAsyncContextManager[Sandbox]:
        return create_sandbox(
            provider=context.provider,
            sandbox_name="scheduler-sandbox",
            source=_SOURCE,
            resources=_RESOURCES,
            creation_semaphore=creation_semaphore,
            creation_state=creation_state,
            unique_name=False,
        )

    return await admission.enter_queued_sandbox(
        stack=stack,
        context=context,
        task_row_id=task.id,
        expected_started_at=task.started_at,
        authority=authority,
        source=_SOURCE,
        resources=_RESOURCES,
        creation_state=creation_state,
        create=create,
    )


def _assert_reserved_demand(engine: Engine, pool_id: str, count: int) -> None:
    with Session(engine) as session:
        assert store.active_reservations(session, pool_id) == store.ActiveReservations(
            count=count,
            vcpu=count,
            memory=count * 2,
            disk=count * 3,
        )


async def test_recover_queued_pool_resets_abandoned_build(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [])
    _, _, (task,) = _run(
        postgres_session,
        context.pool_id,
        [("abandoned", TaskStatus.BUILDING, _ATTEMPT)],
    )

    await admission.recover_queued_pool(context)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING


@pytest.mark.parametrize("reserved", [False, True], ids=["legacy", "reserved"])
async def test_completed_run_finalizes_while_another_run_holds_pool_lock_during_creation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
    reserved: bool,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events, reserved=reserved)
    org, completing_run, (evaluating,) = _run(
        postgres_session, context.pool_id, [("evaluating", TaskStatus.EVALUATING, _ATTEMPT)]
    )
    evaluating.eval_resume_state = {"cursor": "evaluation"}
    _, building_run, (building,) = _run(postgres_session, context.pool_id, [("building", TaskStatus.PENDING, _ATTEMPT)])
    completing_authority = executor_authority(completing_run, session=postgres_session)
    building_authority = executor_authority(building_run, session=postgres_session)
    evaluation_started = asyncio.Event()
    creation_started = asyncio.Event()
    release_creation = asyncio.Event()

    async def finish_evaluation(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, float]]:
        evaluation_started.set()
        await creation_started.wait()
        _update_task(postgres_engine, task_row, TaskStatus.FINISHED, task_row.started_at)
        return {task_row.task_id: {"score": 1.0}}

    async def hold_creation() -> None:
        creation_started.set()
        await release_creation.wait()

    monkeypatch.setattr(run_orchestration, "engine", postgres_engine)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr(task_execution.TaskMonitor, "_TRACK_INTERVAL", 0.01)
    monkeypatch.setattr(run_orchestration, "process_task", finish_evaluation)
    coordinator = asyncio.create_task(
        run_orchestration._run_queued_tasks(
            benchmark_id=completing_run.id,
            task_rows=[(evaluating.task_id, evaluating)],
            start_benchmark_request=completing_run.managed_start_benchmark_request(),
            benchmark_service=Mock(spec=BenchmarkServiceClient),
            runtime=Mock(spec=RuntimeServices),
            org=org,
            sandbox_provider_config=Mock(spec=SandboxProviderConfig),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            queue_context=context,
            record_cancellation=lambda _attempts: None,
            authority=completing_authority,
        )
    )
    async with AsyncExitStack() as stack:
        entering: asyncio.Task[Sandbox | None] | None = None
        try:
            await asyncio.wait_for(evaluation_started.wait(), timeout=2)
            entering = asyncio.create_task(
                _enter(
                    stack,
                    context,
                    building,
                    events,
                    building_authority,
                    source=SnapshotSource(snapshot="snapshot"),
                    on_create=hold_creation,
                )
            )
            await asyncio.wait_for(creation_started.wait(), timeout=2)
            await asyncio.wait_for(coordinator, timeout=2)

            with Session(postgres_engine) as session:
                completed = session.get(Benchmark, completing_run.id)
                assert completed is not None
                run_orchestration.set_benchmark_final_status(completed, session, org, authority=completing_authority)
                assert completed.status == BenchmarkStatus.FINISHED

            assert _task(postgres_engine, building).status == TaskStatus.BUILDING
            assert not entering.done()
            assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
        finally:
            release_creation.set()
            coordinator.cancel()
            await asyncio.gather(coordinator, return_exceptions=True)
            if entering is not None:
                await asyncio.wait_for(entering, timeout=2)

    assert await _pool_lock_is_free(postgres_engine, context.pool_id)


async def test_admission_ignores_task_that_is_no_longer_waiting(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(
        postgres_session,
        context.pool_id,
        [("stopped", TaskStatus.STOPPED, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, task, events, authority)

    assert sandbox is None
    assert events == []


async def test_pool_locks_are_isolated_and_reusable(postgres_engine: Engine) -> None:
    first_pool = store.queue_pool_id(f"daytona:{uuid4()}")
    second_pool = store.queue_pool_id(f"daytona:{uuid4()}")

    async with store.queue_pool_lock(postgres_engine, first_pool) as first:
        async with store.queue_pool_lock(postgres_engine, first_pool) as duplicate:
            pass
        async with store.queue_pool_lock(postgres_engine, second_pool) as independent:
            pass
    async with store.queue_pool_lock(postgres_engine, first_pool) as reused:
        pass

    assert (first, duplicate, independent, reused) == (True, False, True, True)


async def test_queue_pool_lock_contends_with_legacy_key(postgres_engine: Engine) -> None:
    pool_id = store.queue_pool_id(f"daytona:{uuid4()}")
    legacy_lock_key = int.from_bytes(sha256(pool_id.encode()).digest()[:8], byteorder="big", signed=True)

    with postgres_engine.connect() as legacy_connection:
        legacy_acquired = bool(
            legacy_connection.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": legacy_lock_key},
            ).scalar_one()
        )
        legacy_connection.commit()
        assert legacy_acquired
        try:
            async with store.queue_pool_lock(postgres_engine, pool_id) as new_version_acquired:
                pass
            assert new_version_acquired is False
        finally:
            legacy_released = bool(
                legacy_connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": legacy_lock_key},
                ).scalar_one()
            )
            legacy_connection.commit()
            assert legacy_released


async def test_cancelled_evaluation_releases_and_reuses_task_lock(postgres_engine: Engine) -> None:
    task_row_id = uuid4()
    acquired = asyncio.Event()
    hold = asyncio.Event()

    async def owner() -> None:
        async with store.task_evaluation_lock(postgres_engine, task_row_id) as owns_lock:
            assert owns_lock
            acquired.set()
            await hold.wait()

    owner_task = asyncio.create_task(owner())
    await acquired.wait()
    owner_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner_task

    async with store.task_evaluation_lock(postgres_engine, task_row_id) as reused:
        pass
    assert reused is True


async def test_admission_reuses_the_advisory_lock_connection(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("single-connection", TaskStatus.PENDING, _ATTEMPT)],
    )
    single_connection_engine = create_engine(
        postgres_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    events: list[str] = []
    authority = executor_authority(benchmark, session=postgres_session)
    try:
        async with AsyncExitStack() as stack:
            assert await _enter(
                stack,
                _context(single_connection_engine, provider_pool_id, events),
                task,
                events,
                authority,
            )
    finally:
        single_connection_engine.dispose()

    assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
    assert events == ["capacity", "create", "cleanup"]


async def test_revocation_before_claim_creates_no_sandbox(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("revoked-before-claim", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events, on_check=revoke),
            task,
            events,
            authority,
        )

    assert sandbox is None
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert events == ["capacity"]


async def test_revocation_during_creation_cleans_up_without_starting(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("revoked-during-creation", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events),
            task,
            events,
            authority,
            on_create=revoke,
        )

    assert sandbox is None
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert events == ["capacity", "create", "cleanup"]


async def test_priority_fifo_pool_isolation_and_build_recovery(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    other_pool_id = store.queue_pool_id(f"daytona:{uuid4()}")
    _, _, (low_priority,) = _run(
        postgres_session,
        pool_id,
        [("low", TaskStatus.PENDING, _ATTEMPT - timedelta(minutes=1))],
        priority=4,
    )
    _, _, (head,) = _run(
        postgres_session,
        pool_id,
        [("head", TaskStatus.PENDING, _ATTEMPT)],
        priority=0,
    )
    _, _, (next_fifo,) = _run(
        postgres_session,
        pool_id,
        [("next", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1))],
        priority=0,
    )
    _, _, (other_pool,) = _run(
        postgres_session,
        other_pool_id,
        [("other", TaskStatus.PENDING, _ATTEMPT)],
    )

    assert not store.eligible_task_is(postgres_session, pool_id, low_priority.id, low_priority.started_at)
    assert store.eligible_task_is(postgres_session, pool_id, head.id, head.started_at)
    assert not store.claim_eligible_task(postgres_session, pool_id, next_fifo.id, next_fifo.started_at)
    assert store.eligible_task_is(postgres_session, other_pool_id, other_pool.id, other_pool.started_at)
    assert not store.claim_eligible_task(
        postgres_session,
        pool_id,
        head.id,
        head.started_at + timedelta(microseconds=1),
    )
    assert store.claim_eligible_task(postgres_session, pool_id, head.id, head.started_at)
    postgres_session.commit()

    await admission.recover_queued_pool(_context(postgres_engine, provider_pool_id, []))

    recovered = _task(postgres_engine, head)
    assert recovered.status == TaskStatus.PENDING
    assert recovered.started_at > head.started_at


async def test_admission_rechecks_dynamic_concurrency_capacity_and_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (active, waiting) = _run(
        postgres_session,
        pool_id,
        [
            ("active", TaskStatus.IN_PROGRESS, _ATTEMPT),
            ("waiting", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    lock_checks: list[bool] = []
    observed_states: list[TaskStatus] = []

    authority = executor_authority(benchmark, session=postgres_session)

    async def observe_lock() -> None:
        async with store.queue_pool_lock(postgres_engine, pool_id) as acquired:
            lock_checks.append(acquired)
        observed_states.append(_task(postgres_engine, waiting).status)

    async def poll(_seconds: float) -> None:
        _set_concurrency(postgres_engine, benchmark, 2)

    sleep = AsyncMock(side_effect=poll)
    monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", sleep)
    context = _context(
        postgres_engine,
        provider_pool_id,
        events,
        capacity=(False, True),
        on_check=observe_lock,
    )
    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, waiting, events, authority, on_create=observe_lock)
        assert sandbox
        assert _task(postgres_engine, waiting).status == TaskStatus.IN_PROGRESS

    assert _task(postgres_engine, active).status == TaskStatus.IN_PROGRESS
    assert sleep.await_count == 2
    assert lock_checks == [False, False, False]
    assert observed_states == [TaskStatus.PENDING, TaskStatus.PENDING, TaskStatus.BUILDING]
    assert events == ["capacity", "capacity", "create", "cleanup"]


async def test_cancellation_releases_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(
        postgres_session,
        pool_id,
        [("cancelled", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    capacity_started = asyncio.Event()
    hold_capacity = asyncio.Event()

    async def block_capacity() -> None:
        capacity_started.set()
        await hold_capacity.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(postgres_engine, provider_pool_id, [], on_check=block_capacity),
                task,
                [],
                authority,
            )
        )
        await capacity_started.wait()
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering

    async with store.queue_pool_lock(postgres_engine, pool_id) as acquired:
        pass
    assert acquired is True


async def test_stale_creation_is_cleaned_before_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (capacity_task, creation_task) = _run(
        postgres_session,
        pool_id,
        [
            ("stale-capacity", TaskStatus.PENDING, _ATTEMPT),
            ("stale-creation", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    capacity_events: list[str] = []

    async def supersede_during_capacity() -> None:
        _update_task(
            postgres_engine,
            capacity_task,
            TaskStatus.STOPPED,
            capacity_task.started_at + timedelta(microseconds=1),
        )

    async with AsyncExitStack() as stack:
        assert (
            await _enter(
                stack,
                _context(postgres_engine, provider_pool_id, capacity_events, on_check=supersede_during_capacity),
                capacity_task,
                capacity_events,
                authority,
            )
            is None
        )

    events: list[str] = []
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def supersede_attempt() -> None:
        _update_task(
            postgres_engine,
            creation_task,
            TaskStatus.PENDING,
            creation_task.started_at + timedelta(microseconds=1),
        )

    async def block_cleanup() -> None:
        cleanup_started.set()
        await finish_cleanup.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(postgres_engine, provider_pool_id, events),
                creation_task,
                events,
                authority,
                on_create=supersede_attempt,
                on_cleanup=block_cleanup,
            )
        )
        await cleanup_started.wait()
        entering.cancel()
        await asyncio.sleep(0)
        assert not entering.done()
        finish_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await entering

    assert capacity_events == ["capacity"]
    assert _task(postgres_engine, creation_task).status == TaskStatus.PENDING
    assert events == ["capacity", "create", "cleanup"]


async def test_held_evaluation_lock_rejects_recovery_without_mutation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("owned-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "active-job"}
    postgres_session.add(task)
    postgres_session.commit()
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()
    original_started_at = task.started_at
    dispatch_count = postgres_session.exec(
        select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    enqueue = AsyncMock()
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async with store.task_evaluation_lock(postgres_engine, task.id) as acquired:
        assert acquired
        with pytest.raises(HTTPException) as error:
            await tracker_main.retry_or_resume_benchmark(
                benchmark.id,
                Request({"type": "http", "headers": []}),
                retry=False,
                retry_mode=RetryMode.AUTO,
                concurrency=None,
                task_ids=[],
                service_headers={},
                secrets={},
                benchmark_url=None,
                session=postgres_session,
                org=org,
                issued_keys=[],
            )

    assert error.value.status_code == 409
    assert error.value.detail == "Run has an evaluation that is already owned by an active executor"
    enqueue.assert_not_awaited()
    postgres_session.expire_all()
    persisted_task = postgres_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert persisted_task.started_at == original_started_at
    assert (
        postgres_session.exec(
            select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
        ).one()
        == dispatch_count
    )


async def test_two_recovery_handoffs_leave_one_evaluation_owner(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("raced-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "raced-job"}
    postgres_session.add(task)
    postgres_session.commit()
    executor_authority(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    first_enqueued = asyncio.Event()
    release_first = asyncio.Event()
    enqueued_dispatch_ids: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, *, session: Session, **_kwargs: object) -> None:
        persisted_dispatch = session.get(ExecutorDispatch, dispatch.id)
        assert persisted_dispatch is not None
        persisted_dispatch.status = ExecutorDispatchStatus.RUNNING
        session.commit()
        enqueued_dispatch_ids.append(dispatch.id)
        if len(enqueued_dispatch_ids) == 1:
            first_enqueued.set()
            await release_first.wait()

    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def recover() -> None:
        with Session(postgres_engine) as recovery_session:
            response = await tracker_main.retry_or_resume_benchmark(
                benchmark.id,
                Request({"type": "http", "headers": []}),
                retry=False,
                retry_mode=RetryMode.AUTO,
                concurrency=None,
                task_ids=[],
                service_headers={},
                secrets={},
                benchmark_url=None,
                session=recovery_session,
                org=org,
                issued_keys=[],
            )
            assert response.status == "success"

    first_recovery = asyncio.create_task(recover())
    await asyncio.wait_for(first_enqueued.wait(), timeout=5)
    await asyncio.wait_for(recover(), timeout=5)
    release_first.set()
    await asyncio.wait_for(first_recovery, timeout=5)

    assert len(enqueued_dispatch_ids) == 2
    first_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[0])
    winning_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[1])
    assert first_dispatch is not None
    assert winning_dispatch is not None
    postgres_session.expire_all()
    winning_task = postgres_session.get(Task, task.id)
    assert winning_task is not None
    assert winning_task.started_at == winning_dispatch.created_at

    with Session(postgres_engine) as stale_session:
        stale_task = stale_session.get(Task, task.id)
        assert stale_task is not None
        stale_session.expunge(stale_task)
    stale_task.started_at = first_dispatch.created_at
    service = AsyncMock(spec=BenchmarkServiceClient)
    service.resume_evaluation.return_value = {"score": 1.0}
    _use_real_sandbox_recovery(service)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    request = benchmark.managed_start_benchmark_request()

    async def run(task_row: Task, authority: ExecutionAuthority) -> dict[str, dict[str, Any] | None]:
        return await task_execution.process_task(
            task_row,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )

    stale_result = await run(stale_task, ExecutionAuthority(benchmark.id, first_dispatch.id))
    winner_result = await run(winning_task, ExecutionAuthority(benchmark.id, winning_dispatch.id))

    assert stale_result == {task.task_id: None}
    assert winner_result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED


async def test_resumed_evaluation_uses_lock_connection_for_callback_and_finalization(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("resume-on-one-connection", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "initial-job"}
    postgres_session.add(task)
    postgres_session.commit()
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()

    service = AsyncMock(spec=BenchmarkServiceClient)

    async def resume_evaluation(*_args: object, on_eval_resume_state: Any, **_kwargs: object) -> dict[str, float]:
        on_eval_resume_state({"job_id": "updated-job"})
        return {"score": 1.0}

    service.resume_evaluation.side_effect = resume_evaluation
    _use_real_sandbox_recovery(service)
    single_connection_engine = create_engine(
        postgres_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    monkeypatch.setattr(task_execution, "engine", single_connection_engine)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    request = benchmark.managed_start_benchmark_request()
    try:
        result = await task_execution.process_task(
            task,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )
    finally:
        single_connection_engine.dispose()

    assert result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    persisted_task = _task(postgres_engine, task)
    assert persisted_task.status == TaskStatus.FINISHED
    assert persisted_task.eval_resume_state == {"job_id": "updated-job"}


async def test_setup_retry_reenters_fifo_before_competitor(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    org, benchmark, (retrying, competitor) = _run(
        postgres_session,
        pool_id,
        [
            ("retrying", TaskStatus.PENDING, _ATTEMPT),
            ("competitor", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    admission_states: list[tuple[TaskStatus, TaskStatus]] = []

    async def observe_admission() -> None:
        admission_states.append(
            (
                _task(postgres_engine, retrying).status,
                _task(postgres_engine, competitor).status,
            )
        )

    names: list[str] = []

    def create_sandbox(*_args: object, **kwargs: object) -> AbstractAsyncContextManager[Sandbox]:
        names.append(cast(str, kwargs["sandbox_name"]))
        return _sandbox(events, creation_state=cast(SandboxCreationState, kwargs["creation_state"]), name=names[-1])()

    service = AsyncMock(spec=BenchmarkServiceClient)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE,
        problem_path="/tmp/problem.txt",
        cwd="/testbed",
        resources=_RESOURCES,
    )
    service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
    _use_real_sandbox_recovery(service)
    monkeypatch.setattr(task_execution, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    monkeypatch.setattr(task_execution, "create_sandbox", create_sandbox)
    monkeypatch.setattr(
        task_execution,
        "upload_agent_artifacts",
        AsyncMock(side_effect=[SandboxSetupError("retry"), None]),
    )
    monkeypatch.setattr(task_execution, "run_agent", AsyncMock(return_value=(None, 0.0)))
    context = _context(postgres_engine, provider_pool_id, events, on_check=observe_admission)
    authority = executor_authority(benchmark, session=postgres_session)

    await task_execution.process_task(
        retrying,
        benchmark.managed_start_benchmark_request(),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        retrying.task_id,
        runtime_services,
        org,
        sandbox_provider_config=DaytonaProviderConfig(
            DAYTONA_API_KEY="test-api-key",
            DAYTONA_API_URL="http://localhost:8001",
            DAYTONA_TARGET="us",
        ),
        sandbox_provider=context.provider,
        creation_semaphore=Semaphore(1),
        authority=authority,
        queue_context=context,
    )

    assert admission_states == [
        (TaskStatus.PENDING, TaskStatus.PENDING),
        (TaskStatus.PENDING, TaskStatus.PENDING),
    ]
    assert _task(postgres_engine, competitor).status == TaskStatus.PENDING
    assert len(names) == 2
    assert names[0] == names[1]
    assert retrying.id.hex in names[0]


def _stop_run(engine: Engine, benchmark: Benchmark) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.status = BenchmarkStatus.STOPPING
        session.commit()


def _reserve(session: Session, pool_id: str, task: Task) -> None:
    assert store.claim_eligible_task_with_reservation(session, pool_id, task.id, task.started_at, _RESOURCES)
    session.commit()


def _release_reservation(engine: Engine, task: Task) -> None:
    with Session(engine) as session:
        store.release_reservation(session, task.id, task.started_at)
        session.commit()


async def test_reserved_image_builds_provision_concurrently_outside_the_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [], reserved=True)
    _, benchmark, (first, second) = _run(
        postgres_session,
        context.pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    creating = 0
    both_creating = asyncio.Event()
    finish_creating = asyncio.Event()

    async def hold_create() -> None:
        nonlocal creating
        creating += 1
        if creating == 2:
            both_creating.set()
        await finish_creating.wait()

    events: list[str] = []
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        entering = asyncio.gather(
            _enter(first_stack, context, first, events, authority, on_create=hold_create),
            _enter(second_stack, context, second, events, authority, on_create=hold_create),
        )
        try:
            await asyncio.wait_for(both_creating.wait(), timeout=10)

            assert _reservation_count(postgres_engine, context.pool_id) == 2
            assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.BUILDING}
            assert await _pool_lock_is_free(postgres_engine, context.pool_id)
            assert not await _build_lock_is_free(postgres_engine, first)
            assert not await _build_lock_is_free(postgres_engine, second)
        finally:
            finish_creating.set()
        sandboxes = await asyncio.wait_for(entering, timeout=10)

        assert all(sandboxes)
        assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}
        assert _reservation_count(postgres_engine, context.pool_id) == 0
        assert await _build_lock_is_free(postgres_engine, first)
        assert await _build_lock_is_free(postgres_engine, second)

    assert events.count("create") == 2
    assert events.index("cleanup") > max(index for index, event in enumerate(events) if event == "create")


@pytest.mark.parametrize(
    "snapshot",
    [
        _CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=_RESOURCES.vcpu, used=0)}),
        _CAPACITY.model_copy(update={"memory": ResourceCapacity(total=_RESOURCES.memory, used=0)}),
        _CAPACITY.model_copy(update={"disk": ResourceCapacity(total=_RESOURCES.disk, used=0)}),
    ],
    ids=["cpu", "memory", "disk"],
)
async def test_reservations_bound_parallel_builds(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
    snapshot: SandboxCapacity,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    provider_events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, provider_events, reserved=True, snapshot=snapshot)
    _, benchmark, (first, second) = _run(
        postgres_session,
        context.pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    first_creating = asyncio.Event()
    finish_first = asyncio.Event()
    polled_states: list[tuple[TaskStatus, TaskStatus, int]] = []

    async def hold_first_create() -> None:
        first_creating.set()
        await finish_first.wait()

    async def poll(_seconds: float) -> None:
        polled_states.append(
            (
                _task(postgres_engine, first).status,
                _task(postgres_engine, second).status,
                _reservation_count(postgres_engine, context.pool_id),
            )
        )
        finish_first.set()

    monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", AsyncMock(side_effect=poll))
    first_events: list[str] = []
    second_events: list[str] = []
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        first_entering = asyncio.create_task(
            _enter(first_stack, context, first, first_events, authority, on_create=hold_first_create)
        )
        try:
            await asyncio.wait_for(first_creating.wait(), timeout=10)
            second_sandbox = await asyncio.wait_for(
                _enter(second_stack, context, second, second_events, authority), timeout=10
            )
        finally:
            finish_first.set()
            first_sandbox = await asyncio.wait_for(first_entering, timeout=10)

        assert first_sandbox is not None
        assert second_sandbox is not None

    assert polled_states
    assert polled_states[0] == (TaskStatus.BUILDING, TaskStatus.PENDING, 1)
    assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert first_events == second_events == ["create", "cleanup"]
    assert provider_events.index("read_capacity") < provider_events.index("capacity")


async def test_failed_reserved_create_recovers_after_provider_cleanup(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events, reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("failing", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async def fail_create() -> None:
        raise SandboxSetupError("provider rejected the create")

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxSetupError):
            await _enter(stack, context, task, events, authority, on_create=fail_create)

    assert events == ["read_capacity", "capacity", "create"]
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert _reservation_count(postgres_engine, context.pool_id) == 1
    assert await _build_lock_is_free(postgres_engine, task)

    await admission.recover_queued_pool(context)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    recovered = _task(postgres_engine, task)

    async with AsyncExitStack() as stack:
        assert await _enter(stack, context, recovered, events, authority) is not None
        assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS

    assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.parametrize("provider_outcome", ["deleted", "missing", "lookup_failed", "delete_failed", "wrong_name"])
async def test_abandoned_reservation_requires_confirmed_provider_cleanup(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch, provider_outcome: str
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [], reserved=True)
    _, _, (task,) = _run(postgres_session, context.pool_id, [("abandoned", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, context.pool_id, task)
    expected_name = f"queued-{task.id.hex}-{int(_ATTEMPT.replace(tzinfo=UTC).timestamp() * 1_000_000):x}"

    async def lookup(name: str) -> Sandbox:
        assert name == expected_name
        assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
        assert not await _build_lock_is_free(postgres_engine, task)
        assert _reservation_count(postgres_engine, context.pool_id) == 1
        if provider_outcome == "missing":
            raise SandboxNotFoundError("Absent")

        if provider_outcome == "lookup_failed":
            raise SandboxError("Lookup failed")

        sandbox = Mock(id="orphan", labels={})
        sandbox.name = "wrong" if provider_outcome == "wrong_name" else name
        return cast(Sandbox, sandbox)

    async def remove(sandbox_id: str) -> None:
        assert sandbox_id == "orphan"
        assert not await _build_lock_is_free(postgres_engine, task)
        assert _reservation_count(postgres_engine, context.pool_id) == 1
        if provider_outcome == "delete_failed":
            raise SandboxError("Deletion failed")

    deletion = AsyncMock(side_effect=remove)
    monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=lookup))
    monkeypatch.setattr(context.provider, "delete_sandbox", deletion)

    await admission.recover_queued_pool(context)

    expected_reservations = 0 if provider_outcome in {"deleted", "missing"} else 1
    assert _reservation_count(postgres_engine, context.pool_id) == expected_reservations
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert await _build_lock_is_free(postgres_engine, task)
    if provider_outcome not in {"deleted", "delete_failed"}:
        deletion.assert_not_awaited()

    if expected_reservations:
        monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=SandboxNotFoundError("Absent")))
        await admission.recover_queued_pool(context)
        assert _reservation_count(postgres_engine, context.pool_id) == 0


async def test_cancellation_while_leaving_the_pool_lock_releases_the_build_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events, reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    build_locks: list[store.PostgresAdvisoryLock] = []

    def captured_build_lock(engine: Engine, task_row_id: UUID) -> store.PostgresAdvisoryLock:
        lock = store.task_build_lock(engine, task_row_id)
        build_locks.append(lock)
        return lock

    def release_while_locked(session: Session, task_row_id: UUID, expected_started_at: datetime) -> None:
        assert build_locks[-1].held
        store.release_reservation(session, task_row_id, expected_started_at)

    monkeypatch.setattr(admission, "task_build_lock", captured_build_lock)
    monkeypatch.setattr(admission, "release_reservation", release_while_locked)

    class CancelledWhileReleasing(store.PostgresAdvisoryLock):
        async def release(self) -> None:
            await super().release()
            if _reservation_count(postgres_engine, context.pool_id) == 1:
                raise asyncio.CancelledError

    def cancelled_pool_lock(engine: Engine, pool_id: str) -> store.PostgresAdvisoryLock:
        return CancelledWhileReleasing(engine, resource_id=store.queue_pool_lock_id(pool_id))

    monkeypatch.setattr(admission, "queue_pool_lock", cancelled_pool_lock)

    async with AsyncExitStack() as stack:
        with pytest.raises(asyncio.CancelledError):
            await _enter(stack, context, task, events, authority)

    assert events == ["read_capacity", "capacity"]
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert await _build_lock_is_free(postgres_engine, task)
    assert await _pool_lock_is_free(postgres_engine, context.pool_id)

    monkeypatch.setattr(admission, "queue_pool_lock", store.queue_pool_lock)
    await admission.recover_queued_pool(context)

    fresh_task = _task(postgres_engine, task)
    assert fresh_task.status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0

    async with AsyncExitStack() as stack:
        assert await _enter(stack, context, fresh_task, events, authority) is not None
        assert _task(postgres_engine, fresh_task).status == TaskStatus.IN_PROGRESS

    assert events == ["read_capacity", "capacity", "read_capacity", "capacity", "create", "cleanup"]
    assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.parametrize(
    "outcome",
    [
        "acknowledged",
        "already_gone",
        "unknown_create",
        "provider_delete_error",
        "unexpected_delete_error",
        "second_cancel_create",
        "second_cancel_delete",
    ],
)
async def test_reserved_cancellation_releases_only_acknowledged_cleanup(
    outcome: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events, reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    creating = asyncio.Event()
    finish_creation = asyncio.Event()
    deleting = asyncio.Event()
    finish_deletion = asyncio.Event()
    acknowledged = outcome in ("acknowledged", "already_gone")
    expected_count = 0 if acknowledged else 1
    create_error = SandboxError("creation unknown")
    delete_error = SandboxError("deletion rejected")
    lock_releases: list[int] = []

    build_locks: list[store.PostgresAdvisoryLock] = []
    original_release = store.PostgresAdvisoryLock.release

    async def observe_release(lock: store.PostgresAdvisoryLock) -> None:
        if lock in build_locks:
            assert lock.held
            # A separate connection sees the release commit before the lock is dropped.
            _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
            lock_releases.append(expected_count)
        await original_release(lock)

    def build_lock(engine: Engine, task_row_id: UUID) -> store.PostgresAdvisoryLock:
        lock = store.task_build_lock(engine, task_row_id)
        build_locks.append(lock)
        return lock

    monkeypatch.setattr(store.PostgresAdvisoryLock, "release", observe_release)
    monkeypatch.setattr(admission, "task_build_lock", build_lock)

    provider = cast(MockProvider, context.provider)
    provider_create = provider.create_sandbox
    provider_delete = provider.delete_sandbox

    async def hold_create(request: SandboxCreateRequest) -> Sandbox:
        assert creation_state.cleanup_pending
        sandbox = await provider_create(request)
        creating.set()
        await finish_creation.wait()
        if outcome == "unknown_create":
            raise create_error
        return sandbox

    async def hold_delete(sandbox_id: str) -> None:
        deleting.set()
        await finish_deletion.wait()
        if outcome == "already_gone":
            raise SandboxNotFoundError("gone")
        if outcome == "provider_delete_error":
            raise delete_error
        if outcome == "unexpected_delete_error":
            raise RuntimeError("deletion unknown")
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "create_sandbox", hold_create)
    monkeypatch.setattr(provider, "delete_sandbox", hold_delete)
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1))
        )
        try:
            await asyncio.wait_for(creating.wait(), timeout=10)
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            assert not await _build_lock_is_free(postgres_engine, task)
            assert await _pool_lock_is_free(postgres_engine, context.pool_id)
            _stop_run(postgres_engine, benchmark)
            entering.cancel()
            # Observe cancellation delivery before completing or cancelling provider creation.
            delivered = asyncio.Event()
            asyncio.get_running_loop().call_soon(delivered.set)
            await delivered.wait()
            if outcome == "second_cancel_create":
                entering.cancel()
            else:
                finish_creation.set()
                if outcome != "unknown_create":
                    await asyncio.wait_for(deleting.wait(), timeout=10)
                    assert creation_state.cleanup_pending
                    _assert_reserved_demand(postgres_engine, context.pool_id, 1)
                    assert not await _build_lock_is_free(postgres_engine, task)
                    assert not entering.done()
                    if outcome == "second_cancel_delete":
                        entering.cancel()
                    else:
                        finish_deletion.set()
            error_type = (
                SandboxError if outcome in ("unknown_create", "provider_delete_error") else asyncio.CancelledError
            )
            with pytest.raises(error_type) as exc_info:
                await asyncio.wait_for(entering, timeout=10)
            if outcome == "unknown_create":
                assert exc_info.value is create_error
            elif outcome == "provider_delete_error":
                assert exc_info.value is delete_error
        finally:
            finish_creation.set()
            finish_deletion.set()
            if not entering.done():
                entering.cancel()
                await asyncio.gather(entering, return_exceptions=True)

    assert creation_state.cleanup_pending is (not acknowledged)
    _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
    assert lock_releases == [expected_count]
    assert await _build_lock_is_free(postgres_engine, task)
    assert await _pool_lock_is_free(postgres_engine, context.pool_id)
    with Session(postgres_engine) as session:
        reservations = session.exec(select(store.SandboxBuildReservation)).all()
        assert len(reservations) == expected_count
        if reservations:
            assert reservations[0].task_row_id == task.id
            assert reservations[0].attempt_started_at == task.started_at

    if acknowledged:
        new_attempt = task.started_at + timedelta(microseconds=1)
        _update_task(postgres_engine, task, TaskStatus.PENDING, new_attempt)
        with Session(postgres_engine) as session:
            run = session.get(Benchmark, benchmark.id)
            assert run is not None
            run.status = BenchmarkStatus.IN_PROGRESS
            session.commit()
            assert store.eligible_task_is(session, context.pool_id, task.id, new_attempt)
            assert not store.eligible_task_is(session, context.pool_id, task.id, task.started_at)


@pytest.mark.parametrize(
    "reserved,delete_fails",
    [
        pytest.param(True, False, id="reserved-acknowledged"),
        pytest.param(True, True, id="reserved-provider-delete-error"),
        pytest.param(False, False, id="legacy-acknowledged"),
    ],
)
async def test_promotion_database_error_cleans_up_before_releasing_ownership(
    reserved: bool,
    delete_fails: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [], reserved=reserved)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("promotion-error", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    expected_count = 1 if delete_fails else 0
    cleanup_ownership: list[bool] = []
    lock_releases: list[int] = []
    build_locks: list[store.PostgresAdvisoryLock] = []
    original_release = store.PostgresAdvisoryLock.release

    async def observe_release(lock: store.PostgresAdvisoryLock) -> None:
        if reserved and lock in build_locks:
            assert lock.held
            # A separate connection observes committed demand before the build lock drops.
            lock_releases.append(_reservation_count(postgres_engine, context.pool_id))
        await original_release(lock)

    def build_lock(engine: Engine, task_row_id: UUID) -> store.PostgresAdvisoryLock:
        lock = store.task_build_lock(engine, task_row_id)
        build_locks.append(lock)
        return lock

    monkeypatch.setattr(store.PostgresAdvisoryLock, "release", observe_release)
    monkeypatch.setattr(admission, "task_build_lock", build_lock)
    provider = cast(MockProvider, context.provider)
    provider_delete = provider.delete_sandbox

    async def observe_delete(sandbox_id: str) -> None:
        assert creation_state.cleanup_pending
        if reserved:
            cleanup_ownership.append(not await _build_lock_is_free(postgres_engine, task))
        else:
            cleanup_ownership.append(not await _pool_lock_is_free(postgres_engine, context.pool_id))
        if delete_fails:
            raise SandboxError("deletion rejected")
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "delete_sandbox", observe_delete)
    function_name = f"scheduler_promotion_error_{task.id.hex}"
    trigger_name = f"promotion_error_{task.id.hex}"
    promotion_error = "scheduler promotion failed after create"
    try:
        with postgres_engine.begin() as connection:
            connection.execute(
                text(
                    f"""CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE EXCEPTION '{promotion_error}';
                    END;
                    $$"""
                )
            )
            connection.execute(
                text(
                    f"""CREATE TRIGGER {trigger_name} BEFORE UPDATE OF status ON task
                    FOR EACH ROW WHEN (
                        OLD.id = '{task.id}'::uuid
                        AND OLD.status = 'BUILDING' AND NEW.status = 'IN_PROGRESS'
                    ) EXECUTE FUNCTION {function_name}()"""
                )
            )

        async with AsyncExitStack() as stack:
            with pytest.raises(DBAPIError, match=promotion_error):
                await asyncio.wait_for(
                    _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1)),
                    timeout=10,
                )

            assert lock_releases == ([expected_count] if reserved else [])
            assert cleanup_ownership == [True]
            assert creation_state.cleanup_pending is delete_fails
            _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)

        assert await _build_lock_is_free(postgres_engine, task)
        assert await _pool_lock_is_free(postgres_engine, context.pool_id)
        assert _task(postgres_engine, task).status == TaskStatus.BUILDING
        if delete_fails:
            monkeypatch.setattr(provider, "get_sandbox", AsyncMock(side_effect=SandboxError("provider unavailable")))

        await admission.recover_queued_pool(context)
        recovered = _task(postgres_engine, task)
        assert recovered.status == TaskStatus.PENDING
        _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
        with Session(postgres_engine) as session:
            assert store.eligible_task_is(session, context.pool_id, task.id, recovered.started_at) is (not delete_fails)
    finally:
        with postgres_engine.begin() as connection:
            connection.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name} ON task"))
            connection.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))


async def test_reserved_cancellation_before_provider_entry_releases_demand(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events, reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    waiting = asyncio.Event()

    class ObservedSemaphore(Semaphore):
        async def acquire(self) -> Literal[True]:
            waiting.set()
            return await super().acquire()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, authority, creation_state, ObservedSemaphore(0))
        )
        await asyncio.wait_for(waiting.wait(), timeout=10)
        _assert_reserved_demand(postgres_engine, context.pool_id, 1)
        assert not await _build_lock_is_free(postgres_engine, task)
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
    assert not creation_state.cleanup_pending
    assert events == ["read_capacity", "capacity"]
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)
    assert await _build_lock_is_free(postgres_engine, task)
    assert await _pool_lock_is_free(postgres_engine, context.pool_id)


@pytest.mark.parametrize("interruption", ["superseded", "stopped", "revoked"])
async def test_reserved_promotion_requires_exact_attempt_live_run_and_authority(
    interruption: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events, reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("fenced", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async def interrupt() -> None:
        if interruption == "superseded":
            _update_task(postgres_engine, task, TaskStatus.PENDING, task.started_at + timedelta(microseconds=1))
        elif interruption == "stopped":
            _stop_run(postgres_engine, benchmark)
        else:
            _revoke_dispatch(postgres_engine, authority)

    cleaning = asyncio.Event()
    finish_cleanup = asyncio.Event()

    provider = cast(MockProvider, context.provider)
    provider_create = provider.create_sandbox
    provider_delete = provider.delete_sandbox

    async def hold_create(request: SandboxCreateRequest) -> Sandbox:
        sandbox = await provider_create(request)
        await interrupt()
        return sandbox

    async def hold_cleanup(sandbox_id: str) -> None:
        cleaning.set()
        await finish_cleanup.wait()
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "create_sandbox", hold_create)
    monkeypatch.setattr(provider, "delete_sandbox", hold_cleanup)
    creation_state = SandboxCreationState(cleanup_pending=False)
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1))
        )
        try:
            await asyncio.wait_for(cleaning.wait(), timeout=10)
            assert creation_state.cleanup_pending
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            assert not await _build_lock_is_free(postgres_engine, task)
            assert await _pool_lock_is_free(postgres_engine, context.pool_id)
        finally:
            finish_cleanup.set()
        sandbox = await asyncio.wait_for(entering, timeout=10)

    assert sandbox is None
    assert not creation_state.cleanup_pending
    assert events == ["read_capacity", "capacity", "create", "cleanup"]
    assert await _build_lock_is_free(postgres_engine, task)
    expected_status = TaskStatus.PENDING if interruption == "superseded" else TaskStatus.BUILDING
    assert _task(postgres_engine, task).status == expected_status
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)

    await admission.recover_queued_pool(context)

    recovered_status = TaskStatus.BUILDING if interruption == "stopped" else TaskStatus.PENDING
    assert _task(postgres_engine, task).status == recovered_status
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)


async def test_legacy_queue_cleans_reserved_capacity_without_requeueing_reserved_tasks(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    legacy = _context(postgres_engine, provider_pool_id, [])
    reserved = _context(postgres_engine, provider_pool_id, [], reserved=True)
    assert reserved.pool_id != legacy.pool_id
    assert store.queue_pool_lock_id(reserved.pool_id) == legacy.pool_id
    assert store.reserved_queue_pool_id(reserved.pool_id) == reserved.pool_id
    assert legacy.serves_queue(reserved.pool_id) and reserved.serves_queue(legacy.pool_id)
    assert not legacy.serves_queue(store.queue_pool_id(f"daytona:{uuid4()}"))

    async with store.queue_pool_lock(postgres_engine, legacy.pool_id) as legacy_held:
        assert legacy_held
        assert not await _pool_lock_is_free(postgres_engine, reserved.pool_id)

    _, _, (legacy_task,) = _run(postgres_session, legacy.pool_id, [("legacy", TaskStatus.BUILDING, _ATTEMPT)])
    _, _, (reserved_task,) = _run(postgres_session, reserved.pool_id, [("reserved", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, reserved.pool_id, reserved_task)

    await admission.recover_queued_pool(legacy)

    assert _task(postgres_engine, legacy_task).status == TaskStatus.PENDING
    assert _task(postgres_engine, reserved_task).status == TaskStatus.BUILDING
    assert _reservation_count(postgres_engine, reserved.pool_id) == 0

    await admission.recover_queued_pool(reserved)

    assert _task(postgres_engine, reserved_task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, reserved.pool_id) == 0


@pytest.mark.parametrize(
    ("interruption", "expected_status", "expected_reservations"),
    [
        ("promoted", TaskStatus.IN_PROGRESS, 0),
        ("stopped", TaskStatus.BUILDING, 0),
    ],
)
async def test_recovery_preserves_builds_changed_after_candidate_selection(
    interruption: str,
    expected_status: TaskStatus,
    expected_reservations: int,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [], reserved=True)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("racing", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, context.pool_id, task)
    original_try_lock = store._try_task_build_transaction_lock
    interleaved = False

    def interleave_before_lock(session: Session, task_row_id: UUID) -> bool:
        nonlocal interleaved
        # Recovery has already materialized the BUILDING candidate before this hook.
        if not interleaved:
            interleaved = True
            with Session(postgres_engine) as concurrent_session:
                if interruption == "promoted":
                    assert admission._start_claimed_task(
                        concurrent_session, task_row_id=task_row_id, expected_started_at=_ATTEMPT
                    )
                else:
                    row = concurrent_session.get(Benchmark, benchmark.id)
                    assert row
                    row.status = BenchmarkStatus.STOPPED
                concurrent_session.commit()
        return original_try_lock(session, task_row_id)

    monkeypatch.setattr(store, "_try_task_build_transaction_lock", interleave_before_lock)

    await admission.recover_queued_pool(context)

    recovered = _task(postgres_engine, task)
    assert interleaved
    assert recovered.status == expected_status
    assert recovered.started_at == _ATTEMPT
    assert _reservation_count(postgres_engine, context.pool_id) == expected_reservations


@pytest.mark.parametrize(
    ("source", "resources", "snapshot"),
    [
        (SnapshotSource(snapshot="snapshot"), _RESOURCES, _CAPACITY),
        (TargetedSnapshotSource(snapshot="snapshot", target="us-west-3"), _RESOURCES, _CAPACITY),
        (_SOURCE, Resources(vcpu=1, memory=2, disk=3, gpu=1), _CAPACITY),
        (_SOURCE, _RESOURCES, SandboxError("capacity unavailable")),
    ],
    ids=["snapshot", "targeted_snapshot", "gpu_image", "capacity_read_failed"],
)
async def test_reserved_queue_keeps_the_full_lock_for_inexact_or_unreadable_demand(
    source: SandboxSource,
    resources: Resources,
    snapshot: SandboxCapacity | Exception,
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events, reserved=True, snapshot=snapshot)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("full-lock", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    observed: list[tuple[bool, int, bool]] = []

    async def observe() -> None:
        observed.append(
            (
                await _pool_lock_is_free(postgres_engine, context.pool_id),
                _reservation_count(postgres_engine, context.pool_id),
                await _build_lock_is_free(postgres_engine, task),
            )
        )

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack, context, task, events, authority, source=source, resources=resources, on_create=observe
        )

        assert sandbox is not None
        assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS

    assert observed == [(False, 0, True)]
    expected_events = ["read_capacity", "capacity"] if isinstance(snapshot, Exception) else ["capacity"]
    assert events == [*expected_events, "create", "cleanup"]


@pytest.mark.parametrize("fits", [True, False])
async def test_full_lock_build_waits_for_all_reservations_to_drain(
    fits: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    snapshot = (
        _CAPACITY if fits else _CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=_RESOURCES.vcpu, used=0)})
    )
    context = _context(postgres_engine, provider_pool_id, events, reserved=True, snapshot=snapshot)
    _, benchmark, (reserved_task, snapshot_task) = _run(
        postgres_session,
        context.pool_id,
        [
            ("reserved", TaskStatus.PENDING, _ATTEMPT),
            ("snapshot", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    _reserve(postgres_session, context.pool_id, reserved_task)
    creator = store.task_build_lock(postgres_engine, reserved_task.id)
    assert await creator.acquire()
    polled: list[tuple[TaskStatus, int]] = []

    async def promote_reserved_build(_seconds: float) -> None:
        polled.append(
            (_task(postgres_engine, snapshot_task).status, _reservation_count(postgres_engine, context.pool_id))
        )
        _release_reservation(postgres_engine, reserved_task)
        await creator.release()

    monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", AsyncMock(side_effect=promote_reserved_build))

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack, context, snapshot_task, events, authority, source=SnapshotSource(snapshot="snapshot")
        )

        assert sandbox is not None
        assert _task(postgres_engine, snapshot_task).status == TaskStatus.IN_PROGRESS

    assert polled == [(TaskStatus.PENDING, 1)]
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert events == ["read_capacity", "capacity", "create", "cleanup"]
    await creator.release()


@pytest.fixture
async def runtime_services(aws_runtime: AWSRuntime) -> AsyncGenerator[RuntimeServices, None]:
    """Open real AWS adapters while tests replace their external calls."""
    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
    )
    yield runtime
