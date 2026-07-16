"""Shared fixtures for tracker integration tests."""

import os
import uuid
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Generator
import pytest
from benchmark_service import Resources, SandboxProvider
from benchmark_service.client import BenchmarkServiceClient
from dotenv import load_dotenv
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.config import create_benchmark_service_url
from tracker.database.models import DEFAULT_ORG_NAME, AgentContractRequest, Org
from tests.integration_agent_artifacts import (
    create_s3_client,
    delete_test_agent_artifact,
    integration_test_agent_name,
    seed_test_agent_artifact,
)
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import create_benchmark_service_client, fetch_sandbox_provider_config

_ = load_dotenv()


@pytest.fixture
def tracker_database(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connect tracker background work to the per-test SQLite database."""
    monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
    monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)

    existing = database_session.get(Org, TEST_ORG_ID)
    if not existing:
        database_session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
        database_session.commit()


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
def test_agent_name(worker_id: str) -> str:
    return integration_test_agent_name(worker_id)


@pytest.fixture(scope="session")
def seeded_test_agent_artifact(test_agent_name: str, harness_config: HarnessConfig) -> Generator[None, None, None]:
    s3_client = create_s3_client(harness_config.aws)
    key = seed_test_agent_artifact(s3_client, harness_config.s3_bucket, test_agent_name)

    yield

    delete_test_agent_artifact(s3_client, harness_config.s3_bucket, key)


@pytest.fixture
def contract(test_agent_name: str, seeded_test_agent_artifact: None) -> AgentContractRequest:
    return AgentContractRequest(
        name=test_agent_name,
        install_cmd="echo installing dependencies...",
        run_cmd="echo running agent...",
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
