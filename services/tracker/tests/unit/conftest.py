"""Shared fixtures for tracker unit tests."""

import os
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import (
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskResponse,
    VerifyTaskIdsResponse,
)
import pytest
from sqlmodel import Session, SQLModel, create_engine

from tests.unit.utils.task_execution_support import MockKicker, make_retrieve_task_response
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import Org
from tracker.database.session import get_session
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import TaskMonitor

# Set the default AWS credentials before importing modules that create clients.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

# Import the app after configuring the AWS environment.
from main import app
from tracker.aws.runtime import AWSRuntime


@pytest.fixture
def harness_config(aws_credentials: AWSCredentials) -> HarnessConfig:
    return HarnessConfig(
        aws=aws_credentials,
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=30,
        sandbox_provider_secret_name="test-daytona-secret",
    )


@pytest.fixture
def harness_headers(harness_config: HarnessConfig) -> dict[str, str]:
    """Provide complete access-key request headers."""
    headers = {
        "X-Harness-AWS-Access-Key-Id": harness_config.aws.aws_access_key_id,
        "X-Harness-AWS-Secret-Access-Key": harness_config.aws.aws_secret_access_key,
        "X-Harness-AWS-Default-Region": harness_config.aws.aws_default_region,
        "X-Harness-S3-Bucket": harness_config.s3_bucket,
        "X-Harness-Log-Group": harness_config.log_group,
        "X-Harness-Log-Retention-Policy": str(harness_config.log_retention_policy),
        "X-Harness-Sandbox-Provider-Secret-Name": harness_config.sandbox_provider_secret_name,
    }
    if harness_config.aws.aws_session_token:
        headers["X-Harness-AWS-Session-Token"] = harness_config.aws.aws_session_token
    return headers


@pytest.fixture
def aws_runtime(harness_config: HarnessConfig) -> AWSRuntime:
    return AWSRuntime.from_harness_config(harness_config)


@pytest.fixture
def empty_database_session() -> Generator[Session, None, None]:
    """Provide an isolated database without the default organization seed."""
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)

    try:
        with Session(engine) as session:
            yield session
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture(autouse=True)
def mock_s3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace S3 operations with deterministic in-process behavior."""

    async def _mock_get_bytes(*_args: Any, **_kwargs: Any) -> bytes:
        return b"mock-contract-content"

    def _mock_get_contract_s3_key(contract_name: str) -> str:
        return f"contracts/{contract_name}.zip"

    async def _mock_upload_to_s3(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _mock_copy_agent_to_benchmark(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.get_bytes", _mock_get_bytes)
    monkeypatch.setattr("tracker.aws.s3.get_contract_s3_key", _mock_get_contract_s3_key)
    monkeypatch.setattr("tracker.utils.reporting.upload_to_s3", _mock_upload_to_s3)
    monkeypatch.setattr("main.copy_agent_to_benchmark", _mock_copy_agent_to_benchmark)


@pytest.fixture(autouse=True)
def override_database_session(database_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    """Route FastAPI database dependencies through the in-memory test session."""

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)


@pytest.fixture(autouse=True)
def override_org(monkeypatch: pytest.MonkeyPatch) -> None:
    """Override get_current_org to return a test org."""
    test_org = Org(id=TEST_ORG_ID, name="default")
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: test_org)


@pytest.fixture(autouse=True)
def override_starter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Override get_current_starter to return a test identity.

    Tests that need a different identity (e.g. hosted-mode with custom claims) can
    monkeypatch app.dependency_overrides[get_current_starter] inside the test body.
    """
    test_org = Org(id=TEST_ORG_ID, name="default")
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_starter,
        lambda: RequestIdentity(
            org=test_org,
            access_key_id=None,
            email=None,
            name=None,
        ),
    )


@pytest.fixture(autouse=True)
def mock_benchmark_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace benchmark-service calls used by tracker unit tests."""

    async def _mock_health_check(*_args: Any, **_kwargs: Any) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
        return SetupTaskResponse(status="ok")

    async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=[])

    def _mock_get_sandbox_provider(*_args: Any, **_kwargs: Any) -> Mock:
        return Mock()

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", _mock_get_sandbox_provider)
    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)


@pytest.fixture(autouse=True)
def mock_cloudwatch(monkeypatch: pytest.MonkeyPatch) -> None:
    def _mock_create_benchmark_log_group(*_args: Any, **_kwargs: Any) -> str:
        return "mock-group"

    def _mock_write_benchmark_log_event(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _mock_upload_final_view(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("tracker.aws.cloudwatch_logs.create_benchmark_log_group", _mock_create_benchmark_log_group)
    monkeypatch.setattr("tracker.aws.cloudwatch_logs.write_benchmark_log_event", _mock_write_benchmark_log_event)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_benchmark_log_group", _mock_create_benchmark_log_group)
    monkeypatch.setattr("tracker.utils.task_execution.write_benchmark_log_event", _mock_write_benchmark_log_event)
    monkeypatch.setattr("tracker.utils.run_orchestration.upload_final_view", _mock_upload_final_view)


@pytest.fixture(autouse=True)
def mock_secret_store(monkeypatch: pytest.MonkeyPatch) -> None:
    def get(_self: object, _name: str) -> dict[str, str]:
        return {"DAYTONA_API_KEY": "test-key", "DAYTONA_API_URL": "http://localhost:8001", "DAYTONA_TARGET": "us"}

    monkeypatch.setattr("tracker.aws.secrets.SecretsManagerStore.get", get)


@pytest.fixture(autouse=True)
def mock_sandbox_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocks sandbox operations so unit tests never run real sandbox commands"""

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _noop_run_agent(*_args: Any, **_kwargs: Any) -> tuple[None, float]:
        return None, 0.0

    monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", _noop)
    monkeypatch.setattr("tracker.utils.task_execution.run_agent", _noop_run_agent)


@pytest.fixture(autouse=True)
def mock_kicker(monkeypatch: pytest.MonkeyPatch) -> MockKicker:
    """Record queued benchmark work without starting the broker."""
    kicker = MockKicker()
    monkeypatch.setattr("main.process_benchmark.kicker", lambda: kicker)
    return kicker


@pytest.fixture
def process_benchmark_env(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> None:
    """Use deterministic local dependencies for run-orchestration behavior tests."""

    @asynccontextmanager
    async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
        mock_sandbox = AsyncMock()
        mock_sandbox.id = "mock-sandbox-id"
        yield mock_sandbox

    async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return make_retrieve_task_response()

    async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"status": "success", "score": 1.0}

    async def _mock_final_score(*_args: Any, evaluation_results: dict[str, Any], **_kwargs: Any) -> FinalScoreResponse:
        tasks_evaluated = list(evaluation_results.keys())
        return FinalScoreResponse(
            tasks_evaluated=tasks_evaluated,
            final_score=50.0,
            metadata={"resolved_tasks": [], "unresolved_tasks": tasks_evaluated},
        )

    async def _mock_verify_task_ids(*_args: Any, task_ids: list[str], **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
    monkeypatch.setattr("tracker.utils.run_orchestration.engine", database_session.bind)
    monkeypatch.setattr(TaskMonitor, "_TRACK_INTERVAL", 0)
    monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", _mock_create_sandbox)
    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)
    monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
