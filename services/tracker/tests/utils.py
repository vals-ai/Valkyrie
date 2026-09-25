from collections.abc import AsyncGenerator, AsyncIterator, Iterable, Callable, Generator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, TypeVar, cast
from uuid import UUID, uuid4

import httpx
from pydantic import SecretStr
from sqlmodel import Session, select

from tracker.database.models import ExecutorDispatch, ExecutorDispatchAccess, Task
from tracker.database.session import get_session
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient

_Item = TypeVar("_Item")


# Match the default organization seeded by the database fixture.
TEST_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")


def random_task_id() -> str:
    """Return a task ID that will not collide across live test runs."""
    return f"test-task-{uuid4().hex[:5]}"


async def async_iterator(items: Iterable[_Item]) -> AsyncIterator[_Item]:
    """Yield test values through an async iterator."""
    for item in items:
        yield item


@asynccontextmanager
async def executor_api(
    authority: ExecutionAuthority, task_ids: list[str] | None = None
) -> AsyncGenerator[ExecutorClient]:
    """Run the real executor API against the test's configured database dependency."""
    # The app imports tests.utils through fixtures during collection.
    from main import app

    sessions = cast(Callable[[], Generator[Session, None, None]], app.dependency_overrides[get_session])()
    try:
        session = next(sessions)
        dispatch = session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        if dispatch.assigned_task_ids is None:
            dispatch.assigned_task_ids = (
                task_ids
                if task_ids is not None
                else list(session.exec(select(Task.task_id).where(Task.benchmark == authority.benchmark_id)).all())
            )
        if dispatch.lease_expires_at is None:
            dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        token = str(dispatch.id)
        access = session.get(ExecutorDispatchAccess, dispatch.id)
        if access is None:
            access = ExecutorDispatchAccess(
                dispatch_id=dispatch.id, token_digest=sha256(token.encode()).hexdigest(), claimant_id=uuid4()
            )
        else:
            access.token_digest = sha256(token.encode()).hexdigest()
            if access.claimant_id is None:
                access.claimant_id = uuid4()
        session.add(access)
        session.add(dispatch)
        session.commit()
        assert access.claimant_id is not None
        claimant_id = access.claimant_id
    finally:
        sessions.close()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as http:
        yield ExecutorClient(ExecutorTransport(http, SecretStr(token)), authority.dispatch_id, claimant_id)


async def process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None = None,
    benchmark_id_str: str | None = None,
    verified_task_ids: list[str] | None = None,
    execution_context_json: dict[str, Any] | None = None,
    *,
    executor_dispatch_id: str,
) -> None:
    """Exercise the API-backed coordinator with the existing run fixtures."""
    from tracker.aws.runtime import AWSResources
    from tracker.aws.services import CloudRuntimeFactory
    from tracker.executor.run_execution import process_benchmark_v1
    from tracker.utils.run_orchestration import parse_queued_execution

    execution = parse_queued_execution(
        start_benchmark_request_json, benchmark_id_str, verified_task_ids, execution_context_json
    )
    authority = ExecutionAuthority(execution.benchmark_id, UUID(executor_dispatch_id))
    async with executor_api(authority, execution.verified_task_ids) as api:
        state = await api.run_state([])
        resources = AWSResources(**state.run.resources.model_dump()) if state.run.resources is not None else None
        runtime = await CloudRuntimeFactory.create_execution_runtime(
            execution.request,
            state.run.org_id,
            state.run.benchmark_id,
            properties=resources,
            context_version=execution.context_version,
        )
        await process_benchmark_v1(
            api, execution.request, runtime, execution.verified_task_ids, authority.dispatch_id, run=state.run
        )
