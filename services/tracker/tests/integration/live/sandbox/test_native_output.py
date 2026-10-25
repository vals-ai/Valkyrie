"""Live native-output HTTP checks against real Daytona and Modal sandboxes.

From services/tracker, with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_DEFAULT_REGION (and AWS_SESSION_TOKEN when needed) configured:

    uv run pytest tests/integration/live/sandbox/test_native_output.py -q -k aws-daytona
    uv run pytest tests/integration/live/sandbox/test_native_output.py -q -k modal
    uv run pytest tests/integration/live/sandbox/test_native_output.py -q -k local-daytona

Set TEST_DAYTONA_SECRET_NAME or TEST_MODAL_SECRET_NAME to the corresponding
AWS Secrets Manager provider-config secret. Omit -k to run all three cases.
The GitHub live-test workflow needs Actions secrets with those same names and
its existing bench-account role must be able to read both provider secrets.
Each case creates one billable sandbox and deletes it in finally. No S3 objects,
agent artifacts, deployed Tracker, or database migration are needed. Collection
with --collect-only does not fetch secrets or create cloud resources.

Persistence/auth are local. AWS cases inject provider config; local/Daytona
reconstructs the saved runtime and reads its provider secret from Secrets Manager.
Provider creation, reconnection, command execution, file upload, and file
download use the real provider APIs.
Executor publication/cancellation is covered separately by local tests.
"""

import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from benchmark_service import ImageSource, Resources, SandboxCreateRequest, SandboxProviderConfig
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.factories import make_benchmark, make_task
from tracker.api.dependencies import get_run_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.aws.secrets import SecretsManagerStore
from tracker.database.models import ActiveNativeOutput, AgentContractRequest, LocalBenchmarkArguments, TaskStatus
from tracker.local.resources import LocalResources
from tracker.runtime.services import RuntimeServices
from tracker.sandbox import SANDBOX_AUTO_STOP_INTERVAL, SANDBOX_CREATE_TIMEOUT
from tracker.utils import fetch_sandbox_provider_config


@pytest.fixture(
    params=[("aws", "daytona"), ("aws", "modal"), ("local", "daytona")],
    ids=["aws-daytona", "aws-modal", "local-daytona"],
)
async def native_provider_case(
    request: pytest.FixtureRequest,
    live_aws_runtime: AWSRuntime,
) -> tuple[str, str, SandboxProviderConfig]:
    environment, provider = request.param
    variable = f"TEST_{provider.upper()}_SECRET_NAME"
    secret_name = os.getenv(variable)
    if not secret_name:
        pytest.fail(f"{variable} must name a provider-config secret for this live test.")

    config = await fetch_sandbox_provider_config(
        secret_name,
        SecretsManagerStore(live_aws_runtime.clients),
        provider,
    )
    return environment, secret_name, config


async def test_native_endpoints_read_current_provider_files(
    native_provider_case: tuple[str, str, SandboxProviderConfig],
    live_api_client: TestClient,
    live_aws_runtime: AWSRuntime,
    tracker_database: Session,
    tmp_path: Path,
    random_sandbox_name: str,
    test_image: str,
    test_resources: Resources,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, secret_name, native_provider_config = native_provider_case
    root = "/tmp/native output"
    file = f"{root}/nested/trajectory.bin"
    source = ImageSource(image=test_image)
    benchmark = make_benchmark(
        contract=AgentContractRequest(name="native-live", install_cmd="true", run_cmd="true", final_output=root),
        session=tracker_database,
    )
    if environment == "local":
        benchmark.arguments = LocalBenchmarkArguments.model_validate(
            {
                **benchmark.arguments.model_dump(),
                "environment": "local",
                "properties": LocalResources(
                    data_root=tmp_path, secrets_manager_region=live_aws_runtime.resources.region
                ),
                "sandbox_provider": native_provider_config.type,
                "sandbox_provider_secret_name": secret_name,
            }
        )
    else:
        benchmark.arguments = benchmark.arguments.model_copy(update={"sandbox_provider": native_provider_config.type})
    task = make_task(benchmark, "native-live", status=TaskStatus.IN_PROGRESS, started_at=datetime.now(UTC))
    tracker_database.add(benchmark)
    tracker_database.add(task)
    tracker_database.commit()

    if environment == "aws":
        runtime = MagicMock(spec=RuntimeServices)
        runtime.get_sandbox_provider_config = AsyncMock(return_value=native_provider_config)
        monkeypatch.setitem(app.dependency_overrides, get_run_runtime, lambda: runtime)
    url = f"/benchmarks/{benchmark.id}/tasks/{task.task_id}/native-output"

    async with native_provider_config.create_provider() as provider:
        sandbox = await provider.create_sandbox(
            SandboxCreateRequest(
                name=random_sandbox_name,
                source=source,
                resources=test_resources,
                labels={},
                env_vars={},
                auto_stop_interval=SANDBOX_AUTO_STOP_INTERVAL,
                create_timeout=SANDBOX_CREATE_TIMEOUT,
            )
        )
        try:
            result = await sandbox.exec(f"mkdir -p '{root}/nested'")
            assert result.exit_code == 0, result.output

            for output_path, relative_path in [(root, "nested/trajectory.bin"), (file, "trajectory.bin")]:
                content = b"native\x00bytes\nnot JSON\xff"
                await sandbox.upload_file(file, content)
                task.active_native_output = ActiveNativeOutput(
                    attempt_started_at=task.started_at,
                    sandbox_id=sandbox.id,
                    runtime_source=source,
                    output_path=output_path,
                )
                tracker_database.add(task)
                tracker_database.commit()

                # Each HTTP request creates a fresh provider and reconnects using the persisted ID.
                listing = live_api_client.get(f"{url}/files")
                assert listing.status_code == 200, listing.text
                assert listing.json()["files"] == [{"path": relative_path, "size_bytes": len(content)}]
                generation = listing.json()["generation"]
                params = {"path": relative_path, "generation": generation}
                response = live_api_client.get(f"{url}/file", params=params)
                assert response.status_code == 200, response.text
                assert response.content == content
                assert response.headers["x-native-output-generation"] == generation
                assert response.headers["cache-control"] == listing.headers["cache-control"] == "no-store"

                updated = content + b"\nnext agent turn"
                await sandbox.upload_file(file, updated)
                listing = live_api_client.get(f"{url}/files")
                assert listing.status_code == 200, listing.text
                assert listing.json()["files"] == [{"path": relative_path, "size_bytes": len(updated)}]
                assert listing.json()["generation"] == generation
                response = live_api_client.get(f"{url}/file", params=params)
                assert response.status_code == 200, response.text
                assert response.content == updated

                task.active_native_output = None
                tracker_database.add(task)
                tracker_database.commit()
                assert live_api_client.get(f"{url}/files").status_code == 204
                assert live_api_client.get(f"{url}/file", params=params).status_code == 204
        finally:
            await provider.delete_sandbox(sandbox.id)
