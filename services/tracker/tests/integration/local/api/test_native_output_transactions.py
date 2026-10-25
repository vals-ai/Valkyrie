"""PostgreSQL transaction boundaries for native-output HTTP reads."""

import asyncio
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from unittest.mock import AsyncMock, MagicMock

import pytest
from benchmark_service import ImageSource
from benchmark_service.sandbox.types import ExecResult
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.api.dependencies import get_run_runtime
from tracker.database.models import ActiveNativeOutput, DEFAULT_ORG_NAME, Org, Task, TaskStatus
from tracker.database.session import get_session


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def postgres_engine(postgres_container: PostgresContainer) -> Generator[Engine, None, None]:
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.mark.parametrize("endpoint", ["files", "file"])
def test_native_output_provider_wait_does_not_block_task_ddl_or_stale_generation(
    client: TestClient,
    local_app: FastAPI,
    postgres_engine: Engine,
    tmp_path: Path,
    endpoint: str,
) -> None:
    started_at = datetime(2026, 7, 1, tzinfo=UTC)
    output_file = tmp_path / "trajectory.json"
    output_file.write_bytes(b"old bytes")
    descriptor = ActiveNativeOutput(
        attempt_started_at=started_at,
        sandbox_id="sandbox-old",
        runtime_source=ImageSource(image="agent-image"),
        output_path=str(tmp_path),
    )
    with Session(postgres_engine, expire_on_commit=False) as seed:
        seed.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME, storage_key="test-bucket"))
        seed.commit()
        benchmark = make_benchmark(session=seed)
        task = make_task(benchmark, "native-task", status=TaskStatus.IN_PROGRESS, started_at=started_at)
        task.active_native_output = descriptor
        seed.add(task)
        seed.commit()
        benchmark_id, task_id = benchmark.id, task.id

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine, expire_on_commit=False) as session:
            yield session

    local_app.dependency_overrides[get_session] = request_session

    async def execute(command: str, *, timeout: float) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "sh", "-c", command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        output, error = await asyncio.wait_for(process.communicate(), timeout=timeout)
        assert process.returncode is not None
        return ExecResult(exit_code=process.returncode, output=(output + error).decode())

    entered = Event()
    release = Event()

    async def stall(*_args: object) -> object:
        entered.set()
        await asyncio.to_thread(release.wait)
        return sandbox if endpoint == "files" else b"old bytes"

    sandbox = MagicMock()
    sandbox.exec = AsyncMock(side_effect=execute)
    sandbox.download_file = AsyncMock(side_effect=stall if endpoint == "file" else None)
    provider = MagicMock()
    provider.get_sandbox = AsyncMock(side_effect=stall if endpoint == "files" else None)
    if endpoint == "file":
        provider.get_sandbox.return_value = sandbox
    config = MagicMock()
    config.create_provider.return_value.__aenter__.return_value = provider
    runtime = MagicMock()
    runtime.get_sandbox_provider_config = AsyncMock(return_value=config)
    local_app.dependency_overrides[get_run_runtime] = lambda: runtime

    url = f"/benchmarks/{benchmark_id}/tasks/native-task/native-output"

    def request() -> Response:
        if endpoint == "files":
            return client.get(f"{url}/files", headers={"X-Api-Key": "fake"})
        return client.get(
            f"{url}/file",
            params={"path": output_file.name, "generation": descriptor.generation},
            headers={"X-Api-Key": "fake"},
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(request)
        try:
            assert entered.wait(timeout=10), "Native provider did not reach its wait"
            with postgres_engine.begin() as connection:
                connection.exec_driver_sql("SET LOCAL lock_timeout = '2s'")
                connection.exec_driver_sql("ALTER TABLE task ADD COLUMN native_preview_lock_probe integer")
            with Session(postgres_engine) as writer:
                current = writer.get(Task, task_id)
                assert current is not None
                current.active_native_output = descriptor.model_copy(update={"sandbox_id": "sandbox-next"})
                writer.add(current)
                writer.commit()
        finally:
            release.set()
        response = future.result(timeout=10)

    assert response.status_code == 409
    assert b"old bytes" not in response.content
