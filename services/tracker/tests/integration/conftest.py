"""Shared fixtures for tracker integration tests."""

import os
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Generator
from uuid import uuid4

import pytest
from benchmark_service import Resources, SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from dotenv import load_dotenv
from sqlmodel import Session

from tests.integration.seed_agent_artifacts import (
    create_s3_client,
    delete_test_agent_artifact,
    integration_test_agent_name,
    seed_test_agent_artifact,
)
from tests.utils import TEST_ORG_ID
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider
from tracker.aws.s3 import get_contract_s3_key
from tracker.config import create_benchmark_service_url
from tracker.database.models import DEFAULT_ORG_NAME, AgentContractRequest, Org
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import create_benchmark_service_client, fetch_sandbox_provider_config

_ = load_dotenv()


@pytest.fixture
def tracker_database(
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> Session:
    """Connect tracker background work to the per-test SQLite database."""
    monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
    monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)

    existing = database_session.get(Org, TEST_ORG_ID)
    if not existing:
        database_session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME))
        database_session.commit()

    return database_session


@pytest.fixture(scope="session")
def daytona_secret_name() -> str:
    """Require the Daytona secret name used by live sandbox tests."""
    daytona_secret_name = os.getenv("TEST_DAYTONA_SECRET_NAME")
    if not daytona_secret_name:
        pytest.fail("TEST_DAYTONA_SECRET_NAME must be set to run live integration tests.")

    return daytona_secret_name


@pytest.fixture(scope="session")
def live_aws_credentials() -> AWSCredentials:
    """Require and return the AWS credentials used by live integration tests."""
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if not aws_access_key_id:
        pytest.fail("AWS_ACCESS_KEY_ID must be set to run live integration tests.")

    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_secret_access_key:
        pytest.fail("AWS_SECRET_ACCESS_KEY must be set to run live integration tests.")

    aws_default_region = os.getenv("AWS_DEFAULT_REGION")
    if not aws_default_region:
        pytest.fail("AWS_DEFAULT_REGION must be set to run live integration tests.")

    return AWSCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_default_region=aws_default_region,
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )


@pytest.fixture(scope="session")
def harness_config(daytona_secret_name: str, live_aws_credentials: AWSCredentials) -> HarnessConfig:
    """Require and assemble the harness configuration for live integration tests."""
    aws_s3_bucket = os.getenv("TEST_AWS_S3_BUCKET")

    if not aws_s3_bucket:
        pytest.fail("TEST_AWS_S3_BUCKET must be set to run live integration tests.")

    log_group = os.getenv("TEST_LOG_GROUP")

    if not log_group:
        pytest.fail("TEST_LOG_GROUP must be set to run live integration tests.")

    log_retention_policy = int(os.getenv("TEST_LOG_RETENTION") or 1)

    return HarnessConfig(
        sandbox_provider_secret_name=daytona_secret_name,
        aws=live_aws_credentials,
        log_group=log_group,
        log_retention_policy=log_retention_policy,
        s3_bucket=aws_s3_bucket,
    )


@pytest.fixture(scope="session")
def test_agent_name(worker_id: str) -> str:
    """Return a collision-free agent name for the current pytest worker."""
    return integration_test_agent_name(worker_id)


@pytest.fixture(scope="session")
def seeded_test_agent_artifact(test_agent_name: str, harness_config: HarnessConfig) -> Generator[str, None, None]:
    """Seed the live S3 agent artifact and always delete it after the session."""
    s3_client = create_s3_client(harness_config.aws)
    key = get_contract_s3_key(test_agent_name)

    try:
        seed_test_agent_artifact(s3_client, harness_config.s3_bucket, test_agent_name)
        yield test_agent_name
    finally:
        try:
            delete_test_agent_artifact(s3_client, harness_config.s3_bucket, key)
        finally:
            s3_client.close()


@pytest.fixture
def contract(seeded_test_agent_artifact: str) -> AgentContractRequest:
    """Return the contract whose artifact is seeded for live integration tests."""
    return AgentContractRequest(
        name=seeded_test_agent_artifact,
        install_cmd="echo installing dependencies...",
        run_cmd="echo running agent...",
    )


@pytest.fixture(scope="session")
def service_headers() -> dict[str, str]:
    """Return benchmark-service authentication headers when configured."""
    auth_key = os.getenv("BENCHMARK_SERVICE_AUTH_KEY")
    return {"x-descope-api-key": auth_key} if auth_key else {}


@pytest.fixture
def creation_semaphore() -> Semaphore:
    """Limit each live test worker to five concurrent sandbox creations."""
    return Semaphore(5)


@pytest.fixture(scope="function")
async def benchmark_service(service_headers: dict[str, str]) -> AsyncGenerator[BenchmarkServiceClient, None]:
    """Provide a live benchmark-service client and always close it."""
    service = create_benchmark_service_client(
        url=create_benchmark_service_url("swebench"),
        service_headers=service_headers,
    )

    try:
        yield service
    finally:
        await service.close()


@pytest.fixture
def sandbox_provider_config(
    daytona_secret_name: str,
    live_aws_credentials: AWSCredentials,
) -> SandboxProviderConfig:
    """Return the real provider configuration used by live service calls."""
    return fetch_sandbox_provider_config(
        daytona_secret_name,
        ExplicitCredentialsAWSClientProvider(live_aws_credentials),
        "daytona",
    )


@pytest.fixture
def sandbox_provider(
    benchmark_service: BenchmarkServiceClient,
    sandbox_provider_config: SandboxProviderConfig,
) -> SandboxProvider:
    """Provide the real configured sandbox provider for live tests."""
    return benchmark_service.get_sandbox_provider(sandbox_provider_config)


@pytest.fixture
def random_sandbox_name() -> str:
    """Return a collision-free sandbox name for a live test."""
    return f"test-sandbox-{uuid4().hex[:5]}"


@pytest.fixture
def test_image() -> str:
    """Return the small public image used by live sandbox tests."""
    return "python:3.11-slim"


@pytest.fixture
def test_resources() -> Resources:
    """Return the minimal resource request used by live sandbox tests."""
    return Resources(vcpu=1, memory=2, disk=5)
