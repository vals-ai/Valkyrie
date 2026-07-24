from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import DaytonaConfig, DaytonaConnectionError, DaytonaNotFoundError, GpuType, SandboxState
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlmodel import Session

from tests.factories import make_benchmark, make_error_result, make_evaluation_result, make_task
from tests.utils import TEST_ORG_ID
from tracker.api import run_evidence
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import RunLogEvent, RunLogEventsPage, task_log_attempt_id
from tracker.database.models import Benchmark, Org, TaskAttempt
from tracker.database.session import get_session
from tracker.types import HarnessConfig
from tracker.utils.harness_config import try_fetch_harness_config

_APP = FastAPI()
_APP.include_router(run_evidence.router)
_CLIENT = TestClient(_APP)
_ORG = Org(id=TEST_ORG_ID, name="default")
_NOW = datetime(2026, 7, 23, tzinfo=ZoneInfo("UTC"))


class _FakeDaytona:
    def __init__(self, result: object) -> None:
        self.result = result

    async def __aenter__(self) -> "_FakeDaytona":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def get(self, _instance_id: str) -> object:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _use_database(database_session: Session, harness_config: HarnessConfig) -> None:
    def get_test_session():
        yield database_session

    _APP.dependency_overrides[get_session] = get_test_session
    _APP.dependency_overrides[get_current_org] = lambda: _ORG
    _APP.dependency_overrides[try_fetch_harness_config] = lambda: harness_config


def _persist_attempt(
    database_session: Session,
    *,
    provider: str = "daytona",
    attempt_id: str = "deadbeef",
) -> tuple[Benchmark, str, str]:
    benchmark = make_benchmark()
    benchmark.aws_managed = True
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "sandbox_provider": provider,
            "sandbox_provider_secret_name": "provider-secret",
        }
    )
    task = make_task(benchmark, "provider/model:fast")
    evaluation = make_evaluation_result(task, "sandbox-123", {"score": 1}, _NOW)
    evaluation.attempt_id = attempt_id
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        attempt_id=attempt_id,
        started_at=_NOW,
        sandbox_provider=provider,
        sandbox_instance_id="sandbox-123",
    )
    database_session.add_all([benchmark, task, attempt, evaluation])
    database_session.commit()
    return benchmark, task.task_id, attempt_id


def _runtime_resolution() -> SimpleNamespace:
    runtime = SimpleNamespace(
        clients=Mock(),
        resources=SimpleNamespace(log_group="/valkyrie/benchmarks-dev"),
    )
    return SimpleNamespace(runtime=runtime)


def test_run_evidence_openapi_is_discriminated_and_bounded() -> None:
    schema = _APP.openapi()
    operation = schema["paths"]["/benchmarks/{benchmark_id}/tasks/{task_id}/attempts/{attempt_id}/sandbox"]["get"]
    response = operation["responses"]["200"]["content"]["application/json"]["schema"]

    assert response["discriminator"]["propertyName"] == "status"
    assert set(response["discriminator"]["mapping"]) == {
        "deleted",
        "live",
        "not_recorded",
        "unavailable",
        "unsupported",
    }
    run_operation = schema["paths"]["/benchmarks/{benchmark_id}/logs/events"]["get"]
    limit = next(parameter for parameter in run_operation["parameters"] if parameter["name"] == "limit")
    cursor = next(parameter for parameter in run_operation["parameters"] if parameter["name"] == "cursor")
    assert limit["schema"]["maximum"] == 1_000
    assert cursor["schema"]["anyOf"][0]["maxLength"] == 4_096


def test_run_logs_return_exact_identifiers_and_active_state(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark = make_benchmark()
    benchmark.aws_managed = True
    database_session.add(benchmark)
    database_session.commit()
    monkeypatch.setattr(run_evidence, "resolve_run_aws_runtime", Mock(return_value=_runtime_resolution()))
    monkeypatch.setattr(
        run_evidence,
        "get_run_log_events",
        Mock(
            return_value=RunLogEventsPage(
                events=[
                    RunLogEvent(
                        event_id="event-1",
                        task_id="provider/model:fast",
                        attempt_id="deadbeef",
                        timestamp_ms=10,
                        ingestion_time_ms=11,
                        message="streamed",
                    )
                ],
                next_cursor="opaque-next",
                at_tail=True,
            )
        ),
    )

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/logs/events?limit=25")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "events": [
            {
                "event_id": "event-1",
                "task_id": "provider/model:fast",
                "attempt_id": "deadbeef",
                "timestamp_ms": 10,
                "ingestion_time_ms": 11,
                "message": "streamed",
            }
        ],
        "next_cursor": "opaque-next",
        "at_tail": True,
        "is_active": True,
    }


def test_daytona_attempt_returns_only_safe_live_metadata(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark, task_id, attempt_id = _persist_attempt(database_session)
    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY="secret-key",
        DAYTONA_API_URL="https://daytona.example/api",
        DAYTONA_TARGET="us",
    )
    sandbox = SimpleNamespace(
        name="task-a1b2c3",
        state=SandboxState.STARTED,
        target="us",
        cpu=4,
        memory=8,
        disk=30,
        gpu=1,
        gpu_type=GpuType.H100,
        created_at="2026-07-23T01:00:00Z",
        updated_at="2026-07-23T02:00:00Z",
        last_activity_at="2026-07-23T02:01:00Z",
    )
    runtime_resolution = _runtime_resolution()
    monkeypatch.setattr(run_evidence, "resolve_run_aws_runtime", Mock(return_value=runtime_resolution))
    fetch_provider = Mock(return_value=provider_config)
    monkeypatch.setattr(run_evidence, "fetch_sandbox_provider_config", fetch_provider)

    def create_daytona(_config: DaytonaConfig) -> _FakeDaytona:
        return _FakeDaytona(sandbox)

    monkeypatch.setattr(run_evidence, "AsyncDaytona", create_daytona)

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/tasks/{task_id}/attempts/{attempt_id}/sandbox")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "live",
        "provider": "daytona",
        "instance_id": "sandbox-123",
        "name": "task-a1b2c3",
        "state": "started",
        "region": "us",
        "resources": {
            "cpu_cores": 4.0,
            "memory_gib": 8.0,
            "disk_gib": 30.0,
            "gpu_count": 1.0,
            "gpu_type": "H100",
        },
        "created_at": "2026-07-23T01:00:00Z",
        "updated_at": "2026-07-23T02:00:00Z",
        "last_activity_at": "2026-07-23T02:01:00Z",
    }
    fetch_provider.assert_called_once_with(
        "provider-secret",
        runtime_resolution.runtime.clients,
        "daytona",
    )
    assert "secret" not in response.text


def test_daytona_attempt_distinguishes_deleted_and_unavailable(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark, task_id, attempt_id = _persist_attempt(database_session)
    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY="secret-key",
        DAYTONA_API_URL="https://daytona.example/api",
        DAYTONA_TARGET="us",
    )
    monkeypatch.setattr(run_evidence, "resolve_run_aws_runtime", Mock(return_value=_runtime_resolution()))
    monkeypatch.setattr(run_evidence, "fetch_sandbox_provider_config", Mock(return_value=provider_config))
    daytona_result = [DaytonaNotFoundError("missing"), DaytonaConnectionError("offline")]

    def create_daytona(_config: DaytonaConfig) -> _FakeDaytona:
        return _FakeDaytona(daytona_result.pop(0))

    monkeypatch.setattr(run_evidence, "AsyncDaytona", create_daytona)
    path = f"/benchmarks/{benchmark.id}/tasks/{task_id}/attempts/{attempt_id}/sandbox"

    deleted = _CLIENT.get(path)
    unavailable = _CLIENT.get(path)

    assert deleted.json() == {
        "status": "deleted",
        "provider": "daytona",
        "instance_id": "sandbox-123",
        "snapshot": None,
    }
    assert unavailable.json() == {
        "status": "unavailable",
        "provider": "daytona",
        "instance_id": "sandbox-123",
        "message": "Live sandbox metadata is temporarily unavailable.",
        "snapshot": None,
    }


def test_current_attempt_exposes_sandbox_before_result(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark = make_benchmark()
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "sandbox_provider": "daytona",
            "sandbox_provider_secret_name": "provider-secret",
        }
    )
    task = make_task(benchmark, "active-task", started_at=_NOW)
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        attempt_id=task_log_attempt_id(task.started_at),
        started_at=task.started_at,
        sandbox_provider="daytona",
        sandbox_instance_id="sandbox-live",
        sandbox_snapshot={
            "name": "active-task",
            "state": "started",
            "region": "us",
            "resources": {
                "cpu_cores": 4,
                "memory_gib": 8,
                "disk_gib": 30,
                "gpu_count": 0,
                "gpu_type": None,
            },
            "recorded_at": "2026-07-23T00:00:00Z",
        },
    )
    database_session.add_all([benchmark, task, attempt])
    database_session.commit()
    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY="secret-key",
        DAYTONA_API_URL="https://daytona.example/api",
        DAYTONA_TARGET="us",
    )
    monkeypatch.setattr(
        run_evidence,
        "resolve_run_aws_runtime",
        Mock(return_value=_runtime_resolution()),
    )
    monkeypatch.setattr(
        run_evidence,
        "fetch_sandbox_provider_config",
        Mock(return_value=provider_config),
    )
    monkeypatch.setattr(
        run_evidence,
        "AsyncDaytona",
        lambda _config: _FakeDaytona(DaytonaNotFoundError("missing")),
    )

    response = _CLIENT.get(
        f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts/{task_log_attempt_id(task.started_at)}/sandbox"
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "provider": "daytona",
        "instance_id": "sandbox-live",
        "snapshot": {
            "name": "active-task",
            "state": "started",
            "region": "us",
            "resources": {
                "cpu_cores": 4.0,
                "memory_gib": 8.0,
                "disk_gib": 30.0,
                "gpu_count": 0.0,
                "gpu_type": None,
            },
            "recorded_at": "2026-07-23T00:00:00Z",
        },
    }


def test_historical_stopped_attempt_keeps_its_sandbox(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark = make_benchmark()
    benchmark.arguments = benchmark.arguments.model_copy(
        update={"sandbox_provider": "daytona", "sandbox_provider_secret_name": "provider-secret"}
    )
    task = make_task(benchmark, "retried-task", started_at=_NOW)
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        attempt_id="deadbeef",
        started_at=datetime(2026, 7, 22, tzinfo=ZoneInfo("UTC")),
        sandbox_provider="daytona",
        sandbox_instance_id="deleted-sandbox",
    )
    database_session.add_all([benchmark, task, attempt])
    database_session.commit()
    monkeypatch.setattr(
        run_evidence,
        "resolve_run_aws_runtime",
        Mock(return_value=_runtime_resolution()),
    )
    monkeypatch.setattr(
        run_evidence,
        "fetch_sandbox_provider_config",
        Mock(
            return_value=DaytonaProviderConfig(
                DAYTONA_API_KEY="secret-key",
                DAYTONA_API_URL="https://daytona.example/api",
                DAYTONA_TARGET="us",
            )
        ),
    )
    monkeypatch.setattr(
        run_evidence,
        "AsyncDaytona",
        lambda _config: _FakeDaytona(DaytonaNotFoundError("missing")),
    )

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/attempts/{attempt.attempt_id}/sandbox")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "provider": "daytona",
        "instance_id": "deleted-sandbox",
        "snapshot": None,
    }


def test_non_daytona_attempt_does_not_load_provider_credentials(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark, task_id, attempt_id = _persist_attempt(database_session, provider="modal")
    resolve_runtime = Mock()
    fetch_provider = Mock()
    monkeypatch.setattr(run_evidence, "resolve_run_aws_runtime", resolve_runtime)
    monkeypatch.setattr(run_evidence, "fetch_sandbox_provider_config", fetch_provider)

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/tasks/{task_id}/attempts/{attempt_id}/sandbox")

    assert response.json() == {
        "status": "unsupported",
        "provider": "modal",
        "instance_id": "sandbox-123",
        "message": "Live sandbox metadata is not supported for this provider.",
        "snapshot": None,
    }
    resolve_runtime.assert_not_called()
    fetch_provider.assert_not_called()


def test_persisted_attempt_without_instance_id_is_explicitly_not_recorded(
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_database(database_session, harness_config)
    benchmark = make_benchmark()
    benchmark.arguments = benchmark.arguments.model_copy(
        update={"sandbox_provider": "daytona", "sandbox_provider_secret_name": "provider-secret"}
    )
    error_task = make_task(benchmark, "error-task")
    resumed_task = make_task(benchmark, "resumed-task")
    error = make_error_result(error_task, "failed", _NOW)
    error.attempt_id = "bad"
    resumed = make_evaluation_result(resumed_task, "temporary", {"score": 0}, _NOW)
    resumed.attempt_id = "fade"
    resumed.instance_id = None
    database_session.add_all([benchmark, error_task, resumed_task, error, resumed])
    database_session.commit()
    resolve_runtime = Mock()
    monkeypatch.setattr(run_evidence, "resolve_run_aws_runtime", resolve_runtime)

    error_response = _CLIENT.get(
        f"/benchmarks/{benchmark.id}/tasks/{error_task.task_id}/attempts/{error.attempt_id}/sandbox"
    )
    resumed_response = _CLIENT.get(
        f"/benchmarks/{benchmark.id}/tasks/{resumed_task.task_id}/attempts/{resumed.attempt_id}/sandbox"
    )

    expected = {
        "status": "not_recorded",
        "provider": "daytona",
        "instance_id": None,
        "message": "No sandbox ID was recorded for this attempt.",
    }
    assert error_response.json() == expected
    assert resumed_response.json() == expected
    resolve_runtime.assert_not_called()


def test_sandbox_attempt_is_scoped_to_the_requested_task(
    database_session: Session,
    harness_config: HarnessConfig,
) -> None:
    _use_database(database_session, harness_config)
    benchmark, _, attempt_id = _persist_attempt(database_session)

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/tasks/different-task/attempts/{attempt_id}/sandbox")

    assert response.status_code == 404
