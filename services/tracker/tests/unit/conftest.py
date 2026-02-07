import os
from typing import Any

import pytest
from sqlmodel import Session

from tracker.database.session import get_session
from tracker.types import (
    HealthCheckResponse,
    SetupTaskResponse,
    VerifyTaskIdsResponse,
)

# Sets default aws credentials in the environment for moto to work
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

# Needs to be after the environment variable setup
from main import app


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
def override_database_session(database_session: Session) -> None:
    """Overrides the database that the fastapi client uses with the in memory database session"""

    def get_test_session():
        yield database_session

    app.dependency_overrides[get_session] = get_test_session


@pytest.fixture(autouse=True)
def mock_benchmark_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock frequently used benchmark service methods
    """

    async def _mock_health_check(*_args: Any, **_kwargs: Any) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _mock_request_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
        return SetupTaskResponse(status="ok")

    async def _mock_request_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=[])

    monkeypatch.setattr("tracker.benchmark_service.BenchmarkService.request_health_check", _mock_health_check)
    monkeypatch.setattr("tracker.benchmark_service.BenchmarkService.request_setup_task", _mock_request_setup_task)
    monkeypatch.setattr(
        "tracker.benchmark_service.BenchmarkService.request_verify_task_ids", _mock_request_verify_task_ids
    )


@pytest.fixture(autouse=True)
def mock_agent_utilities(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock the upload contract and run agent functions"""

    async def _mock_upload_contract(*_args: Any, **_kwargs: Any) -> None:
        pass

    async def _mock_run_agent(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr("tracker.sandbox.upload_agent_artifacts", _mock_upload_contract)
    monkeypatch.setattr("tracker.sandbox.run_agent", _mock_run_agent)
