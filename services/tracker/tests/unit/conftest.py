import unittest.mock
from typing import Any, Callable
from unittest.mock import MagicMock, create_autospec

import pytest
from sqlmodel import Session

from main import app
from tracker.benchmark_service import BenchmarkService
from tracker.database.session import get_session
from tracker.types import (
    HealthCheckResponse,
    SetupTaskResponse,
    VerifyTaskIdsResponse,
)

_patcher = unittest.mock.patch("boto3.client")
mock_boto3_client = _patcher.start()


def _client(_service_name: str, *_args: Any, **_kwargs: Any) -> MagicMock:
    return MagicMock()


mock_boto3_client.side_effect = _client


@pytest.fixture(autouse=True)
def unit_test_environment(monkeypatch: pytest.MonkeyPatch):
    """Sets default environment variables that are expected to be in the environment"""

    monkeypatch.setenv("DAYTONA_API_KEY", "test_key")
    monkeypatch.setenv("DAYTONA_API_URL", "http://test.url")
    monkeypatch.setenv("DAYTONA_TARGET", "test_target")


@pytest.fixture(autouse=True)
def mock_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocks all s3 related functionality"""

    def _mock_download_from_s3(_s3_key: str) -> bytes:
        return b"mock-contract-content"

    def _mock_get_contract_s3_key(contract_name: str) -> str:
        return f"contracts/{contract_name}.zip"

    monkeypatch.setattr("tracker.s3.download_from_s3", _mock_download_from_s3)
    monkeypatch.setattr("tracker.s3.get_contract_s3_key", _mock_get_contract_s3_key)


@pytest.fixture(autouse=True)
def mock_aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sets default aws credentials that are expected to be in the environment"""

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture(autouse=True)
def override_database_session(database_session: Session) -> None:
    """Overrides the database that the fastapi client uses with the in memory database session"""

    def get_test_session():
        yield database_session

        app.dependency_overrides[get_session] = get_test_session


@pytest.fixture
def mock_benchmark_service(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Any]:
    """
    Mock the benchmark service class
    """
    mock_instance = create_autospec(BenchmarkService, instance=True)

    # Endpoints always expected to succeed
    mock_instance.request_health_check.return_value = HealthCheckResponse(status="ok")
    mock_instance.request_setup_task.return_value = SetupTaskResponse(status="ok")

    # Verify task ids always return the same task ids passed in
    async def _mock_request_verify_task_ids(
        *_args: Any, task_ids: list[str] | None = None, _slice_str: str | None = None, **_kwargs: Any
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids or [])

    mock_instance.request_verify_task_ids.side_effect = _mock_request_verify_task_ids

    benchmark_service_mock: Callable[..., Any] = lambda *args: mock_instance

    # When we call the BenchmarkService class, we will return the mock instance
    monkeypatch.setattr("tracker.benchmark_service.BenchmarkService", benchmark_service_mock)

    return benchmark_service_mock


@pytest.fixture(autouse=True)
def mock_agent_utilities(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock the upload contract and run agent functions"""

    async def _mock_upload_contract(*_args: Any, **_kwargs: Any) -> None:
        pass

    async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr("tracker.sandbox.upload_agent_artifacts", _mock_upload_contract)
    monkeypatch.setattr("tracker.sandbox.run_agent", _mock_run_agent)


@pytest.fixture(autouse=True)
def mock_process_benchmark(monkeypatch: pytest.MonkeyPatch) -> None:
    """When we call upon process_benchmark, it will be ignored"""

    async def _mock_process_benchmark(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr("tracker.utils.process_benchmark", _mock_process_benchmark)
