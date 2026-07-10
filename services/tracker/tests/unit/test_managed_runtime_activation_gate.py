from typing import Any
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tracker.database.models import AgentContractRequest, Benchmark, Org
from tracker.types import HarnessConfig, StartBenchmarkRequest, TaskRoleAWSConfig

client = TestClient(app)


def test_api_rejects_task_role_starts_before_activation() -> None:
    response = client.post(
        "/start-benchmark",
        json={
            "contract": {
                "name": "agent",
                "install_cmd": "true",
                "run_cmd": "true",
            },
            "benchmark_name": "swebench",
            "harness_config": {
                "aws": {
                    "kind": "task_role",
                    "aws_default_region": "us-east-1",
                },
                "s3_bucket": "managed-bucket",
                "s3_prefix": "orgs/org-id",
                "log_group": "managed-logs/orgs/org-id",
                "log_retention_policy": 30,
                "sandbox_provider_secret_name": "managed-provider",
            },
        },
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Managed runtime is not activated"}


def test_api_rejects_managed_runtime_header_before_activation(
    contract: AgentContractRequest,
) -> None:
    response = client.post(
        "/start-benchmark",
        headers={"X-Valkyrie-Runtime": "managed"},
        json={"contract": contract.model_dump(), "benchmark_name": "swebench"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Managed runtime is not activated"}


def test_legacy_start_remains_writer_free(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queued_request: dict[str, Any] = {}

    class Kicker:
        def with_labels(self, **_kwargs: Any) -> "Kicker":
            return self

        async def kiq(self, **kwargs: Any) -> None:
            queued_request.update(kwargs["start_benchmark_request_json"])

    monkeypatch.setattr("main.process_benchmark.kicker", lambda: Kicker())
    request = StartBenchmarkRequest(
        contract=contract,
        benchmark_name="swebench",
        harness_config=harness_config,
    )

    response = client.post("/start-benchmark", json=request.model_dump())

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.arguments.runtime is None
    assert queued_request["harness_config"]["aws"] == harness_config.aws.model_dump()


async def test_managed_agent_reads_use_org_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.agents as agents_api

    observed: dict[str, str] = {}
    harness_config = HarnessConfig(
        aws=TaskRoleAWSConfig(aws_default_region="us-east-1"),
        s3_bucket="managed-bucket",
        s3_prefix="orgs/org-id",
        log_group="managed-logs/orgs/org-id",
        log_retention_policy=30,
        sandbox_provider_secret_name="managed-provider",
    )

    async def list_agents(**kwargs: object) -> list[tuple[str, None]]:
        observed["list_prefix"] = str(kwargs["s3_prefix"])
        return []

    async def object_exists(key: str, **_kwargs: object) -> bool:
        observed["download_key"] = key
        return True

    async def object_size(*_args: object, **_kwargs: object) -> int:
        return 1

    async def presigned_url(*_args: object, **_kwargs: object) -> str:
        return "https://s3.test/agent.zip"

    monkeypatch.setattr(agents_api, "list_agents", list_agents)
    monkeypatch.setattr(agents_api, "s3_object_exists", object_exists)
    monkeypatch.setattr(agents_api, "get_s3_object_size", object_size)
    monkeypatch.setattr(agents_api, "create_presigned_url", presigned_url)
    org = Org(id=UUID(int=1), name="org")

    await agents_api.list_agents_endpoint(_org=org, harness_config=harness_config)
    await agents_api.get_agent_download_url("agent", _org=org, harness_config=harness_config)

    assert observed == {
        "list_prefix": "orgs/org-id",
        "download_key": "orgs/org-id/agents/agent.zip",
    }
