import os
import uuid
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from benchmark_service.client import BenchmarkServiceClient
from daytona import AsyncDaytona
from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.config import create_benchmark_service_url
from tracker.database.models import *  # noqa: F403 # type: ignore[attr-defined]
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.sandbox import TrackerResources
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import create_benchmark_service_client, fetch_harness_config

_ = load_dotenv()


@pytest.fixture(autouse=True)
def setup_app_dependencies(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wire FastAPI dependency overrides and tracker engine for every integration test."""

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    monkeypatch.setattr("tracker.utils.engine", database_session.bind)
    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(app.dependency_overrides, fetch_harness_config, lambda: harness_config)

    # Ensure the default org exists in the test database and override the dependency
    existing = database_session.get(Org, TEST_ORG_ID)
    if not existing:
        database_session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
        database_session.commit()
    vals_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: vals_org)


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
def postgres_session(postgres_engine: Any) -> Generator[Session, Any, None]:
    """Create a session for the test postgres database."""
    with Session(postgres_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture(scope="session")
def daytona_secret_name():
    daytona_secret_name = os.getenv("TEST_DAYTONA_SECRET_NAME")
    if not daytona_secret_name:
        raise ValueError("Required environment variable 'TEST_DAYTONA_SECRET_NAME' is not set to run tests")

    return daytona_secret_name


@pytest.fixture(scope="session")
def aws_credentials():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if not aws_access_key_id:
        raise ValueError("Required environment variable 'AWS_ACCESS_KEY_ID' is not set to run tests")

    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_secret_access_key:
        raise ValueError("Required environment variable 'AWS_SECRET_ACCESS_KEY' is not set to run tests")

    aws_default_region = os.getenv("AWS_DEFAULT_REGION")
    if not aws_default_region:
        raise ValueError("Required environment variable 'AWS_DEFAULT_REGION' is not set to run tests")

    aws_credentials = AWSCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_default_region=aws_default_region,
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )

    return aws_credentials


@pytest.fixture(scope="session")
def harness_config(daytona_secret_name: str, aws_credentials: AWSCredentials) -> HarnessConfig:
    aws_s3_bucket = os.getenv("TEST_AWS_S3_BUCKET")

    if not aws_s3_bucket:
        raise ValueError("Required environment variable 'TEST_AWS_S3_BUCKET' is not set to run tests")

    log_group = os.getenv("TEST_LOG_GROUP")

    if not log_group:
        raise ValueError("Required environment variable 'TEST_LOG_GROUP' is not set to run tests")

    log_retention_policy = int(os.getenv("TEST_LOG_RETENTION") or 1)

    return HarnessConfig(
        daytona_secret_name=daytona_secret_name,
        aws=aws_credentials,
        log_group=log_group,
        log_retention_policy=log_retention_policy,
        s3_bucket=aws_s3_bucket,
    )


@pytest.fixture(scope="session")
def service_headers() -> dict[str, str]:
    auth_key = os.getenv("BENCHMARK_SERVICE_AUTH_KEY")
    return {"x-descope-api-key": auth_key} if auth_key else {}


@pytest.fixture
def creation_semaphore() -> Semaphore:
    return Semaphore(10)


@pytest.fixture(scope="function")
async def benchmark_service(
    daytona_secret_name: str, aws_credentials: AWSCredentials, service_headers: dict[str, str]
) -> AsyncGenerator[BenchmarkServiceClient, None]:
    service = create_benchmark_service_client(
        url=create_benchmark_service_url("swebench"),
        daytona_secret_name=daytona_secret_name,
        aws=aws_credentials,
        service_headers=service_headers,
    )

    try:
        yield service
    finally:
        await service.close()


@pytest.fixture
async def daytona_client(benchmark_service: BenchmarkServiceClient) -> AsyncGenerator[AsyncDaytona, None]:
    yield benchmark_service.daytona_client


@pytest.fixture
def random_sandbox_name():
    return f"test-sandbox-{uuid.uuid4().hex[:5]}"


@pytest.fixture
def test_image():
    return "python:3.11-slim"


@pytest.fixture
def test_resources():
    return TrackerResources(vcpu=1, memory=2, disk=5)
