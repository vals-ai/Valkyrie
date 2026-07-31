"""Fixtures for local CLI-to-tracker integration tests."""

from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
import yaml
from click.testing import CliRunner
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from services.tracker import main as tracker_main
from tracker.auth import get_current_org
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.database.models import (
    DEFAULT_ORG_NAME,
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorAdmission,
    ExecutorRelease,
    ExecutorReleaseStatus,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import AWSCredentials, HarnessConfig
from tracker.utils import fetch_harness_config
from valkyrie.cli.runtime_config import TRACKER_SERVICE_URL_ENV_VAR, VALKYRIE_CONFIG_PATH_ENV_VAR

TEST_ORG_ID = UUID("c15649d2-6ec4-4b4a-974a-cc00ea80bbf7")


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide an isolated Click command runner."""
    return CliRunner()


@pytest.fixture
def database_session() -> Generator[Session, None, None]:
    """Provide a disposable SQLite tracker database."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)

    try:
        with Session(engine, expire_on_commit=False) as session:
            release = ExecutorRelease(
                id="cli-test-release",
                artifact_uri="s3://test-artifacts/cli-test-release.pex",
                artifact_digest="a" * 64,
                protocol_version=SUPPORTED_PROTOCOL_VERSION,
                status=ExecutorReleaseStatus.ACTIVE,
                readiness_verified=True,
            )
            session.add_all([Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME), release])
            session.flush()
            session.add(ExecutorAdmission(release_id=release.id))
            session.commit()
            yield session
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def harness_config() -> HarnessConfig:
    """Provide tracker configuration without cloud access."""
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=1,
        sandbox_provider_secret_name="test-provider-secret",
    )


@pytest.fixture
def local_tracker_app(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[FastAPI, None, None]:
    """Connect the production tracker app to local test dependencies."""

    def get_test_session() -> Generator[Session, None, None]:
        yield database_session

    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    tracker_main.app.dependency_overrides[get_session] = get_test_session
    tracker_main.app.dependency_overrides[get_current_org] = lambda: org
    tracker_main.app.dependency_overrides[fetch_harness_config] = lambda: harness_config
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)

    try:
        yield tracker_main.app
    finally:
        tracker_main.app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def route_cli_to_local_tracker(
    tmp_path: Path,
    local_tracker_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route production tracker clients through the local FastAPI app."""
    config_path = tmp_path / "valkyrie.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "test-bucket",
                "sandbox_providers": {"daytona": "test-provider-secret"},
            }
        )
    )
    monkeypatch.setenv(VALKYRIE_CONFIG_PATH_ENV_VAR, str(config_path))
    monkeypatch.setenv(TRACKER_SERVICE_URL_ENV_VAR, "http://tracker.test")

    def build_client(
        *,
        timeout: int,
        headers: dict[str, str],
    ) -> TestClient:
        del timeout
        return TestClient(local_tracker_app, headers=headers)

    monkeypatch.setattr("valkyrie.cli.tracker_client.httpx.Client", build_client)


@pytest.fixture
def seeded_runs(database_session: Session) -> tuple[Benchmark, Benchmark]:
    """Persist runs and tasks with distinct user-visible states."""
    running = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        label="nightly",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        started_by_email="runner@example.com",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(
                name="cli-agent",
                model="openai/gpt-5",
                install_cmd="install",
                run_cmd="run",
                secrets={"TOKEN": "must-not-leak"},
                kwargs={"private": "must-not-leak"},
            ),
            concurrency=2,
            dataset="verified",
        ),
    )
    finished = Benchmark(
        org_id=TEST_ORG_ID,
        name="terminalbench",
        status=BenchmarkStatus.FINISHED,
        label="release",
        started_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
        started_by_email="reviewer@example.com",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(
                name="review-agent",
                install_cmd="install",
                run_cmd="run",
                secrets={"TOKEN": "finished-secret-must-not-leak"},
                kwargs={"private": "finished-kwarg-must-not-leak"},
            ),
            concurrency=1,
        ),
    )
    completed_task = Task(
        org_id=TEST_ORG_ID,
        benchmark=finished.id,
        task_id="complete",
        status=TaskStatus.FINISHED,
    )
    database_session.add_all(
        [
            running,
            finished,
            Task(org_id=TEST_ORG_ID, benchmark=running.id, task_id="done", status=TaskStatus.FINISHED),
            Task(org_id=TEST_ORG_ID, benchmark=running.id, task_id="active", status=TaskStatus.IN_PROGRESS),
            Task(org_id=TEST_ORG_ID, benchmark=running.id, task_id="pending", status=TaskStatus.PENDING),
            Task(org_id=TEST_ORG_ID, benchmark=running.id, task_id="error", status=TaskStatus.ERROR),
            completed_task,
            EvaluationResult(
                org_id=TEST_ORG_ID,
                task=completed_task.id,
                instance_id="complete",
                result={"score": 1},
            ),
            FinalEvaluation(org_id=TEST_ORG_ID, benchmark=finished.id, final_score=0.75),
        ]
    )
    database_session.commit()
    database_session.expire_all()
    return running, finished
