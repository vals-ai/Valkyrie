import os
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from daytona import AsyncDaytona, DaytonaConfig
from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from tracker.benchmark_service import BenchmarkService
from tracker.database.models import *  # noqa: F403 # type: ignore[attr-defined]

_ = load_dotenv()


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, Any, None]:
    """Spin up a postgres container for integration tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def postgres_engine(postgres_container: PostgresContainer):
    """Create an engine connected to the test postgres container."""
    engine = create_engine(postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture
def postgres_session(postgres_engine) -> Generator[Session, Any, None]:
    """Create a session for the test postgres database."""
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture(scope="function")
async def benchmark_service() -> AsyncGenerator[BenchmarkService, None]:
    service_ip = os.getenv("BENCHMARK_SERVICE_URL")
    if not service_ip:
        raise ValueError("BENCHMARK_SERVICE_URL is not set")

    service = BenchmarkService(name="swebench", url=service_ip)

    yield service

    if service.daytona_client:
        await service.daytona_client.close()


@pytest.fixture
async def daytona_client(benchmark_service: BenchmarkService) -> AsyncGenerator[AsyncDaytona, None]:
    daytona_config = DaytonaConfig(
        api_key=benchmark_service.environment_keys["DAYTONA_API_KEY"],
        api_url=benchmark_service.environment_keys["DAYTONA_API_URL"],
        target=benchmark_service.environment_keys["DAYTONA_TARGET"],
    )

    async with AsyncDaytona(config=daytona_config) as client:
        yield client
