"""ExecutorHost dispatch-store integration against disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session

from services.executor_host.supervisor import (  # pyright: ignore[reportMissingImports]
    ArtifactDispatch,
    PostgresExecutorDispatchStore,
)
from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkStatus,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    TaskStatus,
)
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release


@pytest.mark.asyncio
async def test_postgres_store_fences_claim_finish_and_terminalize_with_sibling(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    org = Org(id=uuid4(), name=f"executor-host-store-{uuid4()}")
    benchmark = make_benchmark(
        name="executor-host-store",
        org_id=org.id,
        contract=AgentContractRequest(name="store-agent", install_cmd="true", run_cmd="true"),
        status=BenchmarkStatus.IN_PROGRESS,
    )
    release = ExecutorRelease(
        id="executor-host-store-release",
        artifact_uri="s3://artifacts/executor-host-store.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )

    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    postgres_session.add(benchmark)
    postgres_session.flush()
    task = make_task(benchmark, "task-0", status=TaskStatus.IN_PROGRESS)
    newer_task = make_task(benchmark, "newer-task", status=TaskStatus.IN_PROGRESS)
    postgres_session.add_all([task, newer_task])
    postgres_session.flush()
    first_dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    sibling_dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    postgres_session.add_all([first_dispatch, sibling_dispatch])
    newer_task.started_at = sibling_dispatch.created_at + timedelta(seconds=1)
    postgres_session.commit()

    url = postgres_engine.url
    assert url.host is not None
    assert url.port is not None
    assert url.database is not None
    assert url.username is not None
    assert url.password is not None
    store = PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    )
    artifact = ArtifactDispatch.from_payload(
        {
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        }
    )

    first_authority = await store.claim(str(first_dispatch.id), str(benchmark.id), artifact)
    assert first_authority is not None
    assert await store.claim(str(first_dispatch.id), str(benchmark.id), artifact) is None
    sibling_authority = await store.claim(str(sibling_dispatch.id), str(benchmark.id), artifact)
    assert sibling_authority is not None
    assert await store.is_current(first_authority)
    assert await store.is_current(sibling_authority)

    assert await store.finish(first_authority)
    assert not await store.is_current(first_authority)
    assert await store.is_current(sibling_authority)
    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(type(benchmark), benchmark.id)
    persisted_task = postgres_session.get(type(task), task.id)
    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.IN_PROGRESS

    assert await store.terminalize(sibling_authority, [task.task_id, newer_task.task_id])
    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(type(benchmark), benchmark.id)
    persisted_task = postgres_session.get(type(task), task.id)
    persisted_newer_task = postgres_session.get(type(newer_task), newer_task.id)
    persisted_first_dispatch = postgres_session.get(type(first_dispatch), first_dispatch.id)
    persisted_sibling_dispatch = postgres_session.get(type(sibling_dispatch), sibling_dispatch.id)

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.ERROR
    assert persisted_benchmark.error_message == "Executor host failed"
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.ERROR
    assert persisted_newer_task is not None
    assert persisted_newer_task.status == TaskStatus.IN_PROGRESS
    assert persisted_first_dispatch is not None
    assert persisted_first_dispatch.status == ExecutorDispatchStatus.FINISHED
    assert persisted_sibling_dispatch is not None
    assert persisted_sibling_dispatch.status == ExecutorDispatchStatus.FAILED
