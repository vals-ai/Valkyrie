"""Shared live-integration fixtures."""

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import DEFAULT_ORG_NAME, Org
from tracker.database.session import get_session
from tracker import config
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AWSBenchmarkArguments, AgentContractRequest, Benchmark
from tests.factories import make_benchmark


@pytest.fixture
def api_headers() -> dict[str, str]:
    """Use application-key headers with the local app's overridden identity dependency."""
    return {"X-Api-Key": "test-api-key"}


@pytest.fixture
def live_deployment(live_aws_runtime: AWSRuntime, daytona_secret_name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the local tracker process for the live test account and resources."""
    resources = live_aws_runtime.resources
    for name, value in {
        "AWS_MANAGED_SUBMISSIONS_ENABLED": True,
        "AWS_DEPLOYMENT_ROLE_ORG_IDS": str(TEST_ORG_ID),
        "AWS_DEPLOYMENT_ACCOUNT_ID": live_aws_runtime.expected_bucket_owner,
        "AWS_DEPLOYMENT_REGION": resources.region,
        "AWS_DEPLOYMENT_S3_BUCKET": resources.s3_bucket,
        "AWS_DEPLOYMENT_LOG_GROUP": resources.log_group,
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": str(resources.log_retention_days),
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER": "daytona",
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME": daytona_secret_name,
    }.items():
        monkeypatch.setattr(config, name, value)


@pytest.fixture
def example_benchmark_object(
    contract: AgentContractRequest, live_aws_runtime: AWSRuntime, daytona_secret_name: str, live_deployment: None
) -> Benchmark:
    """Provide a managed run using the configured live resources and provider."""
    benchmark = make_benchmark(contract=contract, concurrency=5)
    benchmark.aws_managed = True
    assert isinstance(benchmark.arguments, AWSBenchmarkArguments)
    benchmark.arguments.properties = live_aws_runtime.resources
    benchmark.arguments.sandbox_provider = "daytona"
    benchmark.arguments.sandbox_provider_secret_name = daytona_secret_name
    return benchmark


@pytest.fixture
def live_api_client(
    tracker_database: Session,
    live_deployment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[TestClient, None, None]:
    """Route the local API through managed live resources and default-chain AWS clients."""

    def get_test_session() -> Generator[Session, None, None]:
        yield tracker_database

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_org,
        lambda: Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME),
    )

    with TestClient(app) as client:
        yield client
