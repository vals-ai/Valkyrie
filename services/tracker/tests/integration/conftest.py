"""Shared fixtures for tracker integration tests."""

import os
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Generator
from uuid import uuid4
from typing import Any, cast

import pytest
import boto3
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
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.s3 import get_contract_s3_key
from tracker.aws.secrets import SecretsManagerStore
from tracker.config import create_benchmark_service_url
from tracker.database.models import DEFAULT_ORG_NAME, AgentContractRequest, Org
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
def live_aws_runtime() -> AWSRuntime:
    """Resolve live resources while AWS authentication stays in the SDK default chain."""
    names = ("AWS_DEFAULT_REGION", "TEST_AWS_S3_BUCKET", "TEST_LOG_GROUP")
    settings = {name: os.getenv(name) for name in names}
    for name, value in settings.items():
        if not value:
            pytest.fail(f"{name} must be set to run live integration tests.")

    region = settings["AWS_DEFAULT_REGION"]
    bucket = settings["TEST_AWS_S3_BUCKET"]
    log_group = settings["TEST_LOG_GROUP"]
    assert region is not None and bucket is not None and log_group is not None
    identity_client = cast(Any, boto3.client)("sts", region_name=region)
    try:
        account_id = cast(str, identity_client.get_caller_identity()["Account"])
    finally:
        identity_client.close()

    return AWSRuntime(
        resources=AWSResources(
            region=region,
            s3_bucket=bucket,
            log_group=log_group,
            log_retention_days=int(os.getenv("TEST_LOG_RETENTION") or 1),
        ),
        clients=DefaultChainAWSClientProvider(region),
        expected_bucket_owner=account_id,
    )


@pytest.fixture(scope="session")
def test_agent_name(worker_id: str) -> str:
    """Return a collision-free agent name for the current pytest worker."""
    return integration_test_agent_name(worker_id)


@pytest.fixture(scope="session")
def seeded_test_agent_artifact(test_agent_name: str, live_aws_runtime: AWSRuntime) -> Generator[str, None, None]:
    """Seed the live S3 agent artifact and always delete it after the session."""
    s3_client = create_s3_client(live_aws_runtime)
    key = get_contract_s3_key(test_agent_name, TEST_ORG_ID)

    try:
        seed_test_agent_artifact(s3_client, live_aws_runtime.resources.s3_bucket, test_agent_name, TEST_ORG_ID)
        yield test_agent_name
    finally:
        try:
            delete_test_agent_artifact(s3_client, live_aws_runtime.resources.s3_bucket, key)
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
async def sandbox_provider_config(
    daytona_secret_name: str,
    live_aws_runtime: AWSRuntime,
) -> SandboxProviderConfig:
    """Return the real provider configuration used by live service calls."""
    return await fetch_sandbox_provider_config(
        daytona_secret_name,
        SecretsManagerStore(live_aws_runtime.clients),
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
