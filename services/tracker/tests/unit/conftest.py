import os
from typing import Any
from unittest.mock import MagicMock

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import HealthCheckResponse, SetupTaskResponse, VerifyTaskIdsResponse
from sqlmodel import Session

from tests.conftest import TEST_ORG_ID
from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.database.session import get_session
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_harness_config

# Sets default aws credentials in the environment for moto to work
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

# Needs to be after the environment variable setup
from main import app


@pytest.fixture
def harness_config() -> HarnessConfig:
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-aws-access-key-id",
            aws_secret_access_key="test-aws-secret-access-key",
            aws_default_region="test-aws-default-region",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=30,
        daytona_secret_name="test-daytona-secret",
    )


@pytest.fixture(autouse=True)
def unit_test_environment(monkeypatch: pytest.MonkeyPatch):
    """Mocks AWS Secrets Manager to return test Daytona credentials"""

    def _mock_fetch_aws_secret(secret_name: str, aws: AWSCredentials) -> dict[str, str]:
        return {
            "DAYTONA_API_KEY": "test_key",
            "DAYTONA_API_URL": "http://test.url",
            "DAYTONA_TARGET": "test_target",
        }

    monkeypatch.setattr("tracker.utils.fetch_aws_secret", _mock_fetch_aws_secret)


@pytest.fixture(autouse=True)
def mock_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocks all s3 related functionality"""

    def _mock_download_from_s3(_s3_key: str) -> bytes:
        return b"mock-contract-content"

    def _mock_get_contract_s3_key(contract_name: str) -> str:
        return f"contracts/{contract_name}.zip"

    def _mock_upload_to_s3(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr("tracker.s3.download_from_s3", _mock_download_from_s3)
    monkeypatch.setattr("tracker.s3.get_contract_s3_key", _mock_get_contract_s3_key)
    monkeypatch.setattr("tracker.utils.upload_to_s3", _mock_upload_to_s3)


@pytest.fixture(autouse=True)
def override_database_session(database_session: Session) -> None:
    """Overrides the database that the fastapi client uses with the in memory database session"""

    def get_test_session():
        yield database_session

    app.dependency_overrides[get_session] = get_test_session


@pytest.fixture(autouse=True)
def override_org() -> None:
    """Override get_current_org to return a test org."""
    test_org = Org(id=TEST_ORG_ID, name="default")
    app.dependency_overrides[get_current_org] = lambda: test_org


@pytest.fixture(autouse=True)
def override_harness_config(harness_config: HarnessConfig) -> None:
    """Overrides the harness config dependency so endpoints don't require X-Harness-* headers"""

    def get_test_harness_config():
        return harness_config

    app.dependency_overrides[fetch_harness_config] = get_test_harness_config


@pytest.fixture(autouse=True)
def mock_benchmark_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock frequently used benchmark service methods
    """

    async def _mock_health_check(*_args: Any, **_kwargs: Any) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
        return SetupTaskResponse(status="ok")

    async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=[])

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)


@pytest.fixture(autouse=True)
def mock_cloudwatch(monkeypatch: pytest.MonkeyPatch) -> None:
    def _mock_create_benchmark_group(*_args: Any, **_kwargs: Any) -> str:
        return "mock-group"

    def _mock_cloudwatch_stream(*_args: Any, **_kwargs: Any) -> None:
        pass

    def _mock_upload_final_view(*_args: Any, **_kwargs: Any) -> None:
        pass

    def _mock_fetch_aws_secret(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"DAYTONA_API_KEY": "test-key", "DAYTONA_API_URL": "http://localhost:8001", "DAYTONA_TARGET": "us"}

    monkeypatch.setattr("tracker.cloudwatch.create_benchmark_group", _mock_create_benchmark_group)
    monkeypatch.setattr("tracker.cloudwatch.cloudwatch_stream", _mock_cloudwatch_stream)
    monkeypatch.setattr("tracker.utils.create_benchmark_group", _mock_create_benchmark_group)
    monkeypatch.setattr("tracker.utils.cloudwatch_stream", _mock_cloudwatch_stream)
    monkeypatch.setattr("tracker.utils.reset_cloudwatch_stream", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("tracker.utils.upload_final_view", _mock_upload_final_view)
    monkeypatch.setattr("tracker.secrets.fetch_aws_secret", _mock_fetch_aws_secret)
    monkeypatch.setattr("tracker.utils.fetch_aws_secret", _mock_fetch_aws_secret)


@pytest.fixture(autouse=True)
def mock_sandbox_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocks sandbox operations so unit tests never run real sandbox commands"""

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr("tracker.utils.upload_agent_artifacts", _noop)
    monkeypatch.setattr("tracker.utils.run_agent", _noop)


@pytest.fixture(autouse=True)
def mock_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _mock_kiq(*_args: Any, **_kwargs: Any) -> None:
        pass

    mock_kicker = MagicMock()
    mock_kicker.return_value.with_labels.return_value.kiq = _mock_kiq

    monkeypatch.setattr("main.process_benchmark.kicker", mock_kicker)
