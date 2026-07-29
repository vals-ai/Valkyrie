"""Unit tests for task execution environment assembly.

Run: uv run pytest tests/unit/utils/test_task_execution_env.py
"""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from typing import Any

import pytest
from sqlmodel import Session

import tracker.utils.task_execution as utils_module
from tests.unit.utils.task_execution_support import TEST_ORG, create_task_environment, run_process_task
from tracker.auth import RequestIdentity
from tracker.database.models import AgentContractRequest
from tracker.types import HarnessConfig


@asynccontextmanager
async def _capture_sandbox_environment(
    captured_env_vars: list[dict[str, str]],
    *_args: Any,
    env_vars: dict[str, str],
    **_kwargs: Any,
) -> AsyncGenerator[SimpleNamespace, None]:
    captured_env_vars.append(env_vars)
    yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")


async def _capture_agent_environment(
    captured_env_vars: list[dict[str, str]],
    *_args: Any,
    agent_env_vars: dict[str, str],
    **_kwargs: Any,
) -> tuple[None, float]:
    captured_env_vars.append(agent_env_vars)
    return None, 0.0


class TestProcessTaskEnvironment:
    """Tracker-owned environment variables passed to agent tasks."""

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_injects_tracker_owned_attribution_env(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        contract = contract.model_copy(update={"secrets": {"UNRELATED_SECRET": "secret-name"}})
        run_starter = RequestIdentity(
            org=TEST_ORG,
            access_key_id="access-key-id",
            email="starter@example.com",
            name="Starter User",
        )
        start_benchmark_request, task_row, benchmark_id = create_task_environment(
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
        captured_sandbox_env_vars: list[dict[str, str]] = []
        captured_agent_env_vars: list[dict[str, str]] = []

        def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {
                "RUN_ID": "secret-run-id",
                "TASK_ID": "secret-task-id",
                "IDENTITY": '{"source":"secret"}',
                "UNRELATED_SECRET": "secret-value",
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
                "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": "secret-value",
            }

        monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_sandbox_env_vars),
        )
        monkeypatch.setattr(
            utils_module,
            "run_agent",
            partial(_capture_agent_environment, captured_agent_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert captured_sandbox_env_vars == [
            {
                "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": (
                    f"benchmark_id={benchmark_id},task_id=task_0,environment={utils_module.ENVIRONMENT}"
                )
            }
        ]
        assert len(captured_agent_env_vars) == 1
        agent_env_vars = captured_agent_env_vars[0]
        assert agent_env_vars["RUN_ID"] == str(benchmark_id)
        assert "QUESTION_ID" not in agent_env_vars
        assert agent_env_vars["TASK_ID"] == "task_0"
        assert json.loads(agent_env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
            "email": "starter@example.com",
        }
        assert agent_env_vars["UNRELATED_SECRET"] == "secret-value"
        assert agent_env_vars["MODEL_GATEWAY_URL"] == "https://gateway.example.test"
        assert agent_env_vars["MODEL_GATEWAY_API_KEY"] == "gateway-key"
        assert "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS" not in agent_env_vars

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_omits_identity_email_when_unavailable(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured_agent_env_vars: list[dict[str, str]] = []

        def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {}

        monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_no_secrets)
        monkeypatch.setattr(
            utils_module,
            "run_agent",
            partial(_capture_agent_environment, captured_agent_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, harness_config)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_agent_env_vars) == 1
        agent_env_vars = captured_agent_env_vars[0]
        assert json.loads(agent_env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
        }
        assert "MODEL_GATEWAY_URL" not in agent_env_vars
        assert "MODEL_GATEWAY_API_KEY" not in agent_env_vars
