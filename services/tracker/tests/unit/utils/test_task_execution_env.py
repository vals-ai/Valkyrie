"""Unit tests for task execution environment assembly.

Run: uv run pytest tests/unit/utils/test_task_execution_env.py
"""

import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from typing import Any

import pytest
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session

import tracker.utils.task_execution as utils_module
from tests.unit.utils.task_execution_support import (
    TEST_ORG,
    create_task_environment,
    make_retrieve_task_response,
    run_process_task,
)
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import (
    AgentContractRequest,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskStatus,
)
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


class TestProcessTaskEnvironment:
    """Tracker-owned environment variables passed to agent tasks."""

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_injects_tracker_owned_attribution_env(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        contract = contract.model_copy(
            update={
                "model": "provider/model",
                "kwargs": {"variant": "xhigh"},
                "secrets": {"UNRELATED_SECRET": "secret-name"},
                "inference_settings_attested": True,
            }
        )
        run_starter = RequestIdentity(
            org=TEST_ORG,
            access_key_id="access-key-id",
            email="starter@example.com",
            name="Starter User",
        )
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
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
                "VALKYRIE_AGENT_MODEL": "secret-model",
                "VALKYRIE_AGENT_VARIANT": "secret-variant",
                "IDENTITY": '{"source":"secret"}',
                "UNRELATED_SECRET": "secret-value",
                "MODEL_GATEWAY_URL": "https://gateway.example.test",
                "MODEL_GATEWAY_API_KEY": "gateway-key",
            }

        monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert env_vars["RUN_ID"] == str(benchmark_id)
        assert "QUESTION_ID" not in env_vars
        assert env_vars["TASK_ID"] == "task_0"
        assert env_vars["VALKYRIE_AGENT_MODEL"] == "provider/model"
        assert env_vars["VALKYRIE_AGENT_VARIANT"] == "xhigh"
        assert json.loads(env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
            "email": "starter@example.com",
        }
        assert env_vars["UNRELATED_SECRET"] == "secret-value"
        assert env_vars["MODEL_GATEWAY_URL"] == "https://gateway.example.test"
        assert env_vars["MODEL_GATEWAY_API_KEY"] == "gateway-key"

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_withholds_unattested_inference_settings(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        """A caller-supplied contract must not reach setup as trusted settings."""
        contract = contract.model_copy(
            update={
                "model": "caller/model",
                "kwargs": {"variant": "caller-variant"},
                "install_cmd": "echo install",
                "run_cmd": "echo run",
            }
        )
        assert contract.inference_settings_attested is False
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured_env_vars: list[dict[str, str]] = []

        monkeypatch.setattr(utils_module, "resolve_secrets", lambda *_args, **_kwargs: {})
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert "VALKYRIE_AGENT_MODEL" not in env_vars
        assert "VALKYRIE_AGENT_VARIANT" not in env_vars

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_omits_identity_email_when_unavailable(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        contract = contract.model_copy(update={"inference_settings_attested": True})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured_env_vars: list[dict[str, str]] = []

        def _mock_resolve_no_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {}

        monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_no_secrets)
        monkeypatch.setattr(
            utils_module,
            "create_sandbox",
            partial(_capture_sandbox_environment, captured_env_vars),
        )

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert len(captured_env_vars) == 1
        env_vars = captured_env_vars[0]
        assert env_vars["VALKYRIE_AGENT_MODEL"] == ""
        assert env_vars["VALKYRIE_AGENT_VARIANT"] == ""
        assert json.loads(env_vars["IDENTITY"]) == {
            "benchmark_name": "swebench",
            "agent_name": contract.name,
        }
        assert "MODEL_GATEWAY_URL" not in env_vars
        assert "MODEL_GATEWAY_API_KEY" not in env_vars

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_process_task_forwards_native_secret_references_without_resolving_values(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        contract = contract.model_copy(update={"secrets": {"LEGACY_API_KEY": "aws-secret"}})
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        captured: dict[str, dict[str, str]] = {}
        resolved_inputs: list[dict[str, str]] = []

        def _mock_resolve_secrets(secrets: dict[str, str], *_args: Any, **_kwargs: Any) -> dict[str, str]:
            resolved_inputs.append(secrets)
            return {"LEGACY_API_KEY": "legacy-value"}

        @asynccontextmanager
        async def _capture_sandbox(*_args: Any, **kwargs: Any) -> AsyncGenerator[SimpleNamespace, None]:
            captured["env_vars"] = kwargs["env_vars"]
            captured["sandbox_secrets"] = kwargs["sandbox_secrets"]
            yield SimpleNamespace(id="mock-sandbox-id", name="mock-sandbox-name")

        async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> Any:
            response = make_retrieve_task_response()
            response.sandbox_secrets = {"TAVILY_API_KEY": "daytona-tavily"}
            return response

        monkeypatch.setattr(utils_module, "resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(utils_module, "create_sandbox", _capture_sandbox)
        monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)

        result = await run_process_task(start_benchmark_request, task_row, benchmark_id, aws_runtime, authority)

        assert result == {"task_0": {"status": "success", "score": 1.0}}
        assert resolved_inputs == [{"LEGACY_API_KEY": "aws-secret"}]
        assert captured["sandbox_secrets"] == {"TAVILY_API_KEY": "daytona-tavily"}
        assert captured["env_vars"]["LEGACY_API_KEY"] == "legacy-value"
        assert "TAVILY_API_KEY" not in captured["env_vars"]

    @pytest.mark.usefixtures("process_benchmark_env")
    async def test_stopped_task_output_is_fenced_while_sibling_keeps_dispatch_active(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
        aws_runtime: AWSRuntime,
    ) -> None:
        start_benchmark_request, task_row, benchmark_id, authority = create_task_environment(
            contract,
            database_session,
            harness_config,
        )
        sibling = Task(
            org_id=task_row.org_id,
            benchmark=benchmark_id,
            task_id="task_sibling",
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(sibling)
        database_session.commit()
        output_authority_checks: list[bool] = []

        async def stop_before_output(
            *_args: Any,
            execution_is_current: Callable[[], bool],
            **_kwargs: Any,
        ) -> tuple[None, float]:
            selected = database_session.get(Task, task_row.id)
            assert selected is not None
            selected.status = TaskStatus.STOPPED
            database_session.add(selected)
            database_session.commit()

            dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
            assert dispatch is not None
            assert dispatch.status == ExecutorDispatchStatus.RUNNING
            persisted_sibling = database_session.get(Task, sibling.id)
            assert persisted_sibling is not None
            assert persisted_sibling.status == TaskStatus.IN_PROGRESS

            output_authority_checks.append(execution_is_current())
            return None, 0.0

        monkeypatch.setattr(utils_module, "run_agent", stop_before_output)

        result = await run_process_task(
            start_benchmark_request,
            task_row,
            benchmark_id,
            aws_runtime,
            authority,
        )

        assert output_authority_checks == [False]
        assert result == {task_row.task_id: None}
