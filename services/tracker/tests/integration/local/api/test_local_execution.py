"""Run a local Tracker against hosted sandbox providers configured in its secrets file.

Run: uv run pytest tests/integration/local/api/test_local_execution.py
"""

from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from fastapi.testclient import TestClient
from sqlmodel import Session, select

import main as main_module
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    BenchmarkStatus,
    DEFAULT_ORG_NAME,
    AgentContractRequest,
    Benchmark,
    ExecutorAdmission,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
)
from tracker.aws.secrets import SecretsManagerStore
from tracker.exceptions import SecretsError
from tracker.executor.dependencies import get_execution_runtime
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.runtime.services import RuntimeServices
from tracker.secret_access import tenant_secret_prefix
from tracker.types import StartBenchmarkRequest

ORG = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
PROVIDER_SETTINGS = {
    "daytona": "DAYTONA_API_KEY=daytona-key\nDAYTONA_API_URL=https://daytona.test/api\nDAYTONA_TARGET=us\n",
    "modal": "MODAL_TOKEN_ID=modal-id\nMODAL_TOKEN_SECRET=modal-secret\n",
}


@pytest.fixture
def local_tracker(tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Serve the local Tracker with an active executor release and no runner launch."""
    release = ExecutorRelease(
        id="local-release",
        artifact_uri="s3://artifacts/local-release.pex",
        artifact_digest="digest-local-release",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
        readiness_verified=True,
    )
    admission = tracker_database.get(ExecutorAdmission, 1)
    assert admission is not None
    tracker_database.add(release)
    tracker_database.flush()
    admission.release_id = release.id
    tracker_database.add(admission)
    tracker_database.commit()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    overrides = main_module.app.dependency_overrides
    monkeypatch.setitem(overrides, main_module.get_session, lambda: tracker_database)
    monkeypatch.setitem(overrides, main_module.get_current_org, lambda: ORG)
    monkeypatch.setitem(overrides, main_module.get_current_starter, lambda: RequestIdentity(org=ORG))
    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "close", noop)
    monkeypatch.setattr(main_module, "copy_agent_to_benchmark", noop)
    monkeypatch.setattr(main_module, "launch_dispatch", noop)
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", False)
    return TestClient(main_module.app, base_url="http://localhost")


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, settings: str | None) -> None:
    secrets_file = None
    if settings is not None:
        secrets_file = tmp_path / "credentials.env"
        secrets_file.write_text(settings)
    resources = LocalResources(data_root=tmp_path / "data", secrets_file=secrets_file)
    monkeypatch.setattr(local_config, "resources", resources)


def _start(client: TestClient, provider: str, secrets: dict[str, str] | None = None) -> httpx.Response:
    contract = AgentContractRequest(name="agent", install_cmd="true", run_cmd="true", secrets=secrets or {})
    request = StartBenchmarkRequest(contract=contract, benchmark_name="hello", sandbox_provider=provider)
    return client.post("/start-benchmark", json=request.model_dump(mode="json"))


def _benchmark(session: Session, response: httpx.Response) -> Benchmark:
    assert response.status_code == 200, response.text
    session.expire_all()
    benchmark = session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    return benchmark


@pytest.mark.parametrize("provider", ["daytona", "modal"])
async def test_executor_builds_saved_provider_from_secrets_file(
    provider: str,
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run keeps its provider and the executor rebuilds it from the file without persisting credentials."""
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS[provider] + "MODEL_KEY=model-value\n")

    benchmark = _benchmark(tracker_database, _start(local_tracker, provider, {"MODEL_KEY": "model-key"}))
    runtime = await get_execution_runtime(benchmark.local_start_benchmark_request(service_headers={}), benchmark, ORG)

    assert benchmark.arguments.environment == "local"
    assert benchmark.arguments.sandbox_provider == provider
    assert (await runtime.get_sandbox_provider_config()).type == provider
    assert await runtime.secrets.get("model-key") == {"MODEL_KEY": "model-value"}
    stored = benchmark.model_dump_json()
    assert all(value not in stored for value in ("daytona-key", "modal-secret", "model-value"))


@pytest.mark.parametrize("queue_enabled", [False, True])
def test_start_rejects_incomplete_provider_settings(
    queue_enabled: bool,
    local_tracker: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Start names the missing provider settings without echoing configured values."""
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", queue_enabled)
    _configure(monkeypatch, tmp_path, "DAYTONA_API_KEY=daytona-key\n")

    response = _start(local_tracker, "daytona")

    assert response.status_code == 400
    assert "DAYTONA_API_URL: Field required" in response.json()["detail"]
    assert "daytona-key" not in response.text


def test_queue_admission_reads_provider_from_secrets_file(
    local_tracker: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With the sandbox queue on, admission builds the Daytona provider from the local file."""
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", True)
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])

    response = _start(local_tracker, "daytona")

    assert response.status_code == 200, response.text


def test_start_requires_secrets_file_for_hosted_provider(
    local_tracker: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, None)

    response = _start(local_tracker, "modal")

    assert response.status_code == 400
    assert "Local modal execution requires a secrets_file" in response.json()["detail"]


@pytest.mark.parametrize("name", ["DAYTONA_API_KEY", "MODAL_TOKEN_SECRET"])
async def test_contract_cannot_read_provider_settings(
    name: str,
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dispatch refuses contract secrets that would expose the operator's provider credentials."""
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"] + PROVIDER_SETTINGS["modal"])
    benchmark = _benchmark(tracker_database, _start(local_tracker, "daytona", {name: "provider"}))

    with pytest.raises(SecretsError, match=f"cannot read sandbox provider settings: {name}"):
        await get_execution_runtime(benchmark.local_start_benchmark_request(service_headers={}), benchmark, ORG)


async def test_force_stop_rebuilds_saved_provider(
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force-stop signals the run's own provider, configured from the original secrets file."""
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["modal"])
    benchmark = _benchmark(tracker_database, _start(local_tracker, "modal"))
    runtimes: list[RuntimeServices] = []

    async def force_stop_sandboxes(
        _benchmark: Benchmark, runtime: RuntimeServices, *_args: Any, **_kwargs: Any
    ) -> None:
        runtimes.append(runtime)

    monkeypatch.setattr(main_module, "force_stop_sandboxes", force_stop_sandboxes)
    (tmp_path / "other").mkdir()
    _configure(monkeypatch, tmp_path / "other", PROVIDER_SETTINGS["daytona"])

    response = local_tracker.post(f"/stop-benchmark/{benchmark.id}?force=true")

    assert response.status_code == 200, response.text
    assert (await runtimes[0].get_sandbox_provider_config()).type == "modal"


def _use_secrets_manager(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Serve named secrets from a stubbed Secrets Manager with the Daytona provider set to `provider-secret`."""
    monkeypatch.setattr(
        local_config, "resources", LocalResources(data_root=tmp_path / "data", secrets_manager_region="us-east-1")
    )

    def provider_secrets(*_args: object) -> dict[str, str]:
        return {"daytona": "provider-secret"}

    async def get(_store: SecretsManagerStore, _name: str) -> dict[str, str]:
        return {"DAYTONA_API_KEY": "daytona-key", "DAYTONA_API_URL": "https://daytona.test/api", "DAYTONA_TARGET": "us"}

    monkeypatch.setattr(main_module, "provider_secrets", provider_secrets)
    monkeypatch.setattr(SecretsManagerStore, "get", get)


async def test_secrets_manager_run_rebuilds_saved_provider_for_force_stop(
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Secrets Manager-backed run keeps its provider secret, and force-stop rebuilds the same runtime."""
    _use_secrets_manager(monkeypatch, tmp_path)
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")
    benchmark = _benchmark(tracker_database, _start(local_tracker, "daytona"))
    runtimes: list[RuntimeServices] = []

    async def force_stop_sandboxes(
        _benchmark: Benchmark, runtime: RuntimeServices, *_args: Any, **_kwargs: Any
    ) -> None:
        runtimes.append(runtime)

    monkeypatch.setattr(main_module, "force_stop_sandboxes", force_stop_sandboxes)

    response = local_tracker.post(f"/stop-benchmark/{benchmark.id}?force=true")

    assert response.status_code == 200, response.text
    assert benchmark.arguments.sandbox_provider_secret_name == f"{tenant_secret_prefix(TEST_ORG_ID)}provider-secret"
    assert isinstance(runtimes[0].secrets, SecretsManagerStore)
    assert (await runtimes[0].get_sandbox_provider_config()).type == "daytona"


@pytest.mark.parametrize(
    ("secrets_manager", "fields", "detail"),
    [
        (False, {"lambda_function": "callback-fn"}, "Lambda callbacks require S3 storage"),
        (False, {"sandbox_provider": "docker"}, "Docker sandbox provider requires a local benchmark service"),
        (True, {"lambda_function": "callback-fn"}, "Lambda callbacks require S3 storage"),
        (True, {}, "Sandbox access requires a provider secret name"),
        (True, {}, "daytona.DAYTONA_TARGET: Field required"),
    ],
    ids=["lambda", "docker-hosted-service", "lambda-secrets-manager", "no-provider-secret", "invalid-provider-secret"],
)
def test_start_rejects_incompatible_composition(
    secrets_manager: bool,
    fields: dict[str, str],
    detail: str,
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combinations the local runtime cannot serve are rejected before a run is created."""
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    if secrets_manager:
        _use_secrets_manager(monkeypatch, tmp_path)

        if "DAYTONA_TARGET" in detail:

            async def get(_store: SecretsManagerStore, _name: str) -> dict[str, str]:
                return {"DAYTONA_API_KEY": "daytona-key", "DAYTONA_API_URL": "https://daytona.test/api"}

            monkeypatch.setattr(SecretsManagerStore, "get", get)
        else:

            def no_provider_secrets(*_args: object) -> dict[str, str]:
                return {}

            monkeypatch.setattr(main_module, "provider_secrets", no_provider_secrets)
    contract = AgentContractRequest(name="agent", install_cmd="true", run_cmd="true")
    request = {"benchmark_name": "hello", "sandbox_provider": "daytona", **fields}

    response = local_tracker.post("/start-benchmark", json={**request, "contract": contract.model_dump(mode="json")})

    assert response.status_code == 400, response.text
    assert detail in response.json()["detail"]
    assert not tracker_database.exec(select(Benchmark)).all()


def test_filesystem_runs_reject_resume_callbacks_and_s3_export(
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resuming with a Lambda callback and exporting to S3 both need S3 storage."""
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    benchmark = _benchmark(tracker_database, _start(local_tracker, "daytona"))
    benchmark.status = BenchmarkStatus.STOPPED
    tracker_database.add(benchmark)
    tracker_database.commit()

    resume = local_tracker.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"task_ids": ["task-1"], "lambda_function": "callback-fn"},
    )
    export = local_tracker.get("/retrieve-results", params={"benchmark_id": str(benchmark.id), "s3": True})

    assert (resume.status_code, resume.json()["detail"]) == (400, "Lambda callbacks require S3 storage")
    assert (export.status_code, export.json()["detail"]) == (400, "S3 export requires S3 storage")


@pytest.mark.parametrize(
    ("secret_name", "expected_status"),
    [("ModelKey", 200), ("valkyrie/tenants/7f1c2a1e-0000-4000-8000-000000000000/ModelKey", 404)],
    ids=["own", "other-org"],
)
def test_secrets_manager_resume_limits_secret_names_to_the_organization(
    secret_name: str,
    expected_status: int,
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resuming a Secrets Manager-backed run checks secret ownership like an AWS run."""
    _use_secrets_manager(monkeypatch, tmp_path)
    benchmark = _benchmark(tracker_database, _start(local_tracker, "daytona"))
    benchmark.status = BenchmarkStatus.STOPPED
    tracker_database.add(benchmark)
    tracker_database.commit()
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")

    response = local_tracker.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"task_ids": ["task-1"], "secrets": {"MODEL_KEY": secret_name}},
    )

    assert response.status_code == expected_status, response.text


def test_secrets_manager_start_authorizes_uploaded_agent_secrets(
    local_tracker: TestClient,
    tracker_database: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Secret names from an uploaded agent are resolved for the organization."""
    _use_secrets_manager(monkeypatch, tmp_path)
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")

    async def uploaded_contract(*_args: object) -> AgentContractRequest:
        return AgentContractRequest(name="agent", install_cmd="true", run_cmd="true", secrets={"MODEL_KEY": "AgentKey"})

    monkeypatch.setattr(main_module, "_resolve_contract_from_s3", uploaded_contract)
    response = local_tracker.post(
        "/start-benchmark",
        json={"benchmark_name": "hello", "sandbox_provider": "daytona", "contract": {"name": "agent"}},
    )

    benchmark = _benchmark(tracker_database, response)
    assert benchmark.arguments.contract.secrets == {"MODEL_KEY": f"{tenant_secret_prefix(TEST_ORG_ID)}AgentKey"}
