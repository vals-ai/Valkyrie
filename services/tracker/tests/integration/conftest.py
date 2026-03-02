import os
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from daytona import AsyncDaytona
from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from benchmark_service.client import BenchmarkServiceClient
from tracker.types import AWSCredentials
from tracker.utils import create_benchmark_service_client
from tracker.database.models import *  # noqa: F403 # type: ignore[attr-defined]

_ = load_dotenv()


@pytest.fixture(scope="session")
def test_aws() -> AWSCredentials:
    """AWS credentials sourced from the environment for integration tests."""
    return AWSCredentials(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        aws_default_region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


@pytest.fixture(scope="session")
def test_daytona_secret() -> str:
    """Daytona secret name sourced from the environment for integration tests."""
    return os.environ.get("DAYTONA_SECRET_NAME", "test-daytona-secret")


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
async def benchmark_service(
    test_aws: AWSCredentials, test_daytona_secret: str
) -> AsyncGenerator[BenchmarkServiceClient, None]:
    service_url = os.getenv("BENCHMARK_SERVICE_URL")
    if not service_url:
        from tracker.config import benchmark_service_url

        service_url = benchmark_service_url("swebench")

    service = create_benchmark_service_client(url=service_url, daytona_secret_name=test_daytona_secret, aws=test_aws)

    yield service

    await service.close()


@pytest.fixture
async def daytona_client(benchmark_service: BenchmarkServiceClient) -> AsyncGenerator[AsyncDaytona, None]:
    yield benchmark_service.daytona_client
