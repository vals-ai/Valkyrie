import importlib
import os
import uuid
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import patch

import pytest
from benchmark_service import Resources, SandboxProvider
from benchmark_service.client import BenchmarkServiceClient
from dotenv import load_dotenv
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from testcontainers.postgres import PostgresContainer

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.config import create_benchmark_service_url
from tracker.database.models import *  # noqa: F403 # type: ignore[attr-defined]
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import create_benchmark_service_client, fetch_harness_config, fetch_sandbox_provider_config

_ = load_dotenv()

# Used for the app's fetch_harness_config override so endpoint tests run without real
# AWS env vars. Tests that exercise AWS directly use the session-scoped fixtures below.
FAKE_HARNESS_CONFIG = HarnessConfig(
    aws=AWSCredentials(
        aws_access_key_id="test-aws-access-key-id",
        aws_secret_access_key="test-aws-secret-access-key",
        aws_default_region="us-east-1",
    ),
    s3_bucket="test-bucket",
    log_group="test-log-group",
    log_retention_policy=30,
    sandbox_provider_secret_name="test-daytona-secret",
)


@pytest.fixture(autouse=True)
def setup_app_dependencies(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wire FastAPI dependency overrides and tracker engine for every integration test."""

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    monkeypatch.setattr("tracker.utils.engine", database_session.bind)
    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(app.dependency_overrides, fetch_harness_config, lambda: FAKE_HARNESS_CONFIG)

    # Ensure the default org exists in the test database and override the dependency
    existing = database_session.get(Org, TEST_ORG_ID)
    if not existing:
        database_session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
        database_session.commit()
    vals_org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: vals_org)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> Generator[TestClient, None, None]:
    """TestClient with AUTH_REQUIRED=true and a mocked Descope session validator.

    Reloads config/auth/main so AUTH_REQUIRED takes effect, then routes the app at the
    test database and a fake harness config.
    """
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_fake")

    import tracker.config as config_mod

    importlib.reload(config_mod)
    import tracker.auth as auth_mod

    importlib.reload(auth_mod)
    import main as main_mod

    importlib.reload(main_mod)

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    main_mod.app.dependency_overrides[get_session] = get_test_session
    main_mod.app.dependency_overrides[fetch_harness_config] = lambda: FAKE_HARNESS_CONFIG
    monkeypatch.setattr("tracker.database.session.engine", database_session.bind)

    with patch.object(auth_mod, "_descope_client") as mock_client:
        mock_client.validate_session.return_value = {
            "tenants": {"default": {}},
            "userId": "U_caller",
            "user": {"email": "caller@example.com"},
        }
        yield TestClient(main_mod.app)

    main_mod.app.dependency_overrides.clear()


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
        sandbox_provider_secret_name=daytona_secret_name,
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
async def benchmark_service(service_headers: dict[str, str]) -> AsyncGenerator[BenchmarkServiceClient, None]:
    service = create_benchmark_service_client(
        url=create_benchmark_service_url("swebench"),
        service_headers=service_headers,
    )

    try:
        yield service
    finally:
        await service.close()


@pytest.fixture
async def sandbox_provider(
    benchmark_service: BenchmarkServiceClient,
    daytona_secret_name: str,
    aws_credentials: AWSCredentials,
) -> AsyncGenerator[SandboxProvider, None]:
    provider_config = fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona")
    yield benchmark_service.get_sandbox_provider(provider_config)


@pytest.fixture
def random_sandbox_name():
    return f"test-sandbox-{uuid.uuid4().hex[:5]}"


@pytest.fixture
def test_image():
    return "python:3.11-slim"


@pytest.fixture
def test_resources():
    return Resources(vcpu=1, memory=2, disk=5)
