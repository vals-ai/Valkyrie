import json
from asyncio import Semaphore
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
import jwt
from sqlmodel import Session

import tracker.utils.task_execution as utils_module
from tests.conftest import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import AgentContractRequest, BenchmarkStatus, Org, Task
from tracker.types import HarnessConfig, StartBenchmarkRequest, TaskRoleAWSConfig
from tracker.utils import fetch_sandbox_provider_config, process_task, start_benchmark_request_to_benchmark

_TEST_ORG = Org(id=TEST_ORG_ID, name="default")
_TEST_STARTER = RequestIdentity(
    org=_TEST_ORG,
    access_key_id=None,
    email=None,
    name=None,
)


def _create_task_env(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    run_starter: RequestIdentity | None = None,
) -> tuple[StartBenchmarkRequest, Task, UUID]:
    """Create a benchmark request, benchmark row, and task row for process_task tests."""
    start_benchmark_request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=1,
        task_ids=["task_0"],
        harness_config=harness_config,
    )

    benchmark_row = start_benchmark_request_to_benchmark(start_benchmark_request, run_starter or _TEST_STARTER)
    benchmark_row.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(benchmark_row)
    database_session.commit()

    task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
    database_session.add(task_row)
    database_session.commit()

    return start_benchmark_request, task_row, benchmark_row.id


async def _run_process_task(
    start_benchmark_request: StartBenchmarkRequest,
    task_row: Task,
    benchmark_id: UUID,
    harness_config: HarnessConfig,
) -> dict[str, dict[str, Any] | None]:
    return await process_task(
        task_row=task_row,
        start_benchmark_request=start_benchmark_request,
        benchmark_service=start_benchmark_request.benchmark_service,
        benchmark_id=benchmark_id,
        task_id="task_0",
        harness_config=harness_config,
        org=_TEST_ORG,
        sandbox_provider_config=fetch_sandbox_provider_config(
            harness_config.sandbox_provider_secret_name,
            harness_config.aws,
            start_benchmark_request.sandbox_provider,
        ),
        creation_semaphore=Semaphore(1),
    )


async def test_process_task_injects_tracker_owned_attribution_env(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    contract = contract.model_copy(update={"secrets": {"UNRELATED_SECRET": "secret-name"}})
    run_starter = RequestIdentity(
        org=_TEST_ORG,
        access_key_id="access-key-id",
        email="starter@example.com",
        name="Starter User",
    )
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
        run_starter,
    )
    start_benchmark_request = start_benchmark_request.model_copy(
        update={
            "benchmark_name": "transient-benchmark-name",
            "contract": contract.model_copy(update={"name": "transient-agent-name"}),
        }
    )
    captured_env_vars: list[dict[str, str]] = []

    def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {
            "RUN_ID": "secret-run-id",
            "TASK_ID": "secret-task-id",
            "IDENTITY": '{"source":"secret"}',
            "UNRELATED_SECRET": "secret-value",
            "MODEL_GATEWAY_URL": "https://gateway.example.test",
            "MODEL_GATEWAY_API_KEY": "gateway-key",
        }

    @asynccontextmanager
    async def _capture_create_sandbox(*_args: Any, env_vars: dict[str, str], **_kwargs: Any):
        captured_env_vars.append(env_vars)
        yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

    monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": {"status": "success", "score": 1.0}}
    assert len(captured_env_vars) == 1
    env_vars = captured_env_vars[0]
    assert env_vars["RUN_ID"] == str(benchmark_id)
    assert "QUESTION_ID" not in env_vars
    assert env_vars["TASK_ID"] == "task_0"
    assert json.loads(env_vars["IDENTITY"]) == {
        "benchmark_name": "swebench",
        "agent_name": contract.name,
        "email": "starter@example.com",
    }
    assert env_vars["UNRELATED_SECRET"] == "secret-value"
    assert env_vars["MODEL_GATEWAY_URL"] == "https://gateway.example.test"
    assert env_vars["MODEL_GATEWAY_API_KEY"] == "gateway-key"


async def test_process_task_omits_identity_email_when_unavailable(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
) -> None:
    start_benchmark_request, task_row, benchmark_id = _create_task_env(
        contract,
        database_session,
        harness_config,
    )
    captured_env_vars: list[dict[str, str]] = []

    @asynccontextmanager
    async def _capture_create_sandbox(*_args: Any, env_vars: dict[str, str], **_kwargs: Any):
        captured_env_vars.append(env_vars)
        yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

    def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_no_secrets)
    monkeypatch.setattr(utils_module, "create_sandbox", _capture_create_sandbox)

    result = await _run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

    assert result == {"task_0": {"status": "success", "score": 1.0}}
    assert len(captured_env_vars) == 1
    env_vars = captured_env_vars[0]
    assert json.loads(env_vars["IDENTITY"]) == {
        "benchmark_name": "swebench",
        "agent_name": contract.name,
    }
    assert "MODEL_GATEWAY_URL" not in env_vars
    assert "MODEL_GATEWAY_API_KEY" not in env_vars


@pytest.mark.parametrize(
    ("contract_egress", "expected_egress"),
    [
        ([], []),
        (["https://api.openai.com"], ["https://api.openai.com", "https://gateway.example.test"]),
    ],
)
async def test_managed_task_injects_scoped_gateway_token_and_gateway_egress(
    contract: AgentContractRequest,
    database_session: Session,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
    contract_egress: list[str],
    expected_egress: list[str],
) -> None:
    managed_config = harness_config.model_copy(
        update={
            "aws": TaskRoleAWSConfig(aws_default_region="us-east-1"),
            "s3_prefix": f"orgs/{TEST_ORG_ID.hex}",
        }
    )
    contract = contract.model_copy(update={"egress_allowlist": contract_egress})
    starter = RequestIdentity(
        org=_TEST_ORG,
        access_key_id="personal-key-id",
        email="starter@example.com",
        name=None,
    )
    request, task_row, benchmark_id = _create_task_env(contract, database_session, managed_config, starter)
    captured_env: dict[str, str] = {}
    captured_egress: list[str] = []
    monkeypatch.setattr(utils_module.config, "MODEL_GATEWAY_URL", "https://gateway.example.test/v1")
    signing_key = "0123456789abcdef0123456789abcdef"
    monkeypatch.setattr(utils_module.config, "VALKYRIE_GATEWAY_SIGNING_KEY", signing_key)

    def fail_secret_resolution(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        raise AssertionError("managed tasks must not resolve AWS secrets")

    @asynccontextmanager
    async def capture_sandbox(*_args: Any, env_vars: dict[str, str], **_kwargs: Any):
        captured_env.update(env_vars)
        yield SimpleNamespace(id="sandbox-id", name="sandbox-name")

    async def capture_run_agent(_sandbox: object, agent_contract: AgentContractRequest, *_args: Any, **_kwargs: Any):
        captured_egress.extend(agent_contract.egress_allowlist)
        return None, 0.1

    monkeypatch.setattr(utils_module, "resolve_secrets", fail_secret_resolution)
    monkeypatch.setattr(utils_module, "create_sandbox", capture_sandbox)
    monkeypatch.setattr(utils_module, "run_agent", capture_run_agent)

    await _run_process_task(request, task_row, benchmark_id, managed_config)

    claims = jwt.decode(
        captured_env["MODEL_GATEWAY_API_KEY"],
        signing_key,
        algorithms=["HS256"],
        issuer="valkyrie-tracker",
        audience="model-gateway",
    )
    assert captured_env["MODEL_GATEWAY_URL"] == "https://gateway.example.test/v1"
    assert claims["sub"] == "personal-key-id"
    assert claims["org_id"] == str(TEST_ORG_ID)
    assert claims["run_id"] == str(benchmark_id)
    assert claims["task_id"] == "task_0"
    assert claims["exp"] > claims["iat"]
    assert captured_egress == expected_egress
