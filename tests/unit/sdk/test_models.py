"""Behavioral tests for SDK-owned request and response models.

Run: uv run pytest tests/unit/sdk/test_models.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from valkyrie.sdk.models import (
    AWSCredentials,
    AgentContractRequest,
    FailureCategory,
    FailureClassificationState,
    FailureDetail,
    FailureSummary,
    FailureTerminalEffect,
    FetchBenchmarkResponse,
    FetchBenchmarksResponse,
    FinalViewResponse,
    HarnessConfig,
    OutputArtifact,
    SingleTaskResponse,
    StartBenchmarkRequest,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sdk_api"


def load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_agent_contract_normalizes_output_artifacts() -> None:
    contract = AgentContractRequest(
        name="agent",
        output_artifacts=[
            "reports/result.json",
            {"path": "logs/run.txt", "source": "/workspace/logs/*.txt"},
            {
                "path": "artifacts/model.patch",
                "source": "/logs/artifacts/model.patch",
                "required": False,
            },
        ],
    )

    assert contract.output_artifacts[0] == "reports/result.json"
    assert contract.output_artifacts[1] == OutputArtifact(
        path="logs/run.txt",
        source="/workspace/logs/*.txt",
    )
    assert contract.output_artifacts[2] == OutputArtifact(
        path="artifacts/model.patch",
        source="/logs/artifacts/model.patch",
        required=False,
    )

    serialized = contract.model_dump(mode="json")["output_artifacts"]
    assert "required" not in serialized[1]
    assert serialized[2]["required"] is False


@pytest.mark.parametrize("path", ["/absolute", "../escape", "a/../b"])
def test_agent_contract_rejects_unsafe_artifact_paths(path: str) -> None:
    with pytest.raises(ValidationError):
        AgentContractRequest(name="agent", output_artifacts=[path])


@pytest.mark.parametrize(
    "artifacts",
    [
        [
            "artifacts/result.json",
            OutputArtifact(
                path="artifacts//result.json",
                source="/logs/optional.json",
                required=False,
            ),
        ],
        [
            OutputArtifact(
                path="telemetry/result.json",
                source="/logs/first.json",
                required=False,
            ),
            OutputArtifact(
                path="telemetry//result.json",
                source="/logs/second.json",
                required=False,
            ),
        ],
    ],
    ids=["required-optional", "optional-optional"],
)
def test_agent_contract_rejects_duplicate_normalized_output_artifact_paths(
    artifacts: list[str | OutputArtifact],
) -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        AgentContractRequest(name="agent", output_artifacts=artifacts)


@pytest.mark.parametrize("source", ["relative/path", "/../escape", "/*.json"])
def test_output_artifact_rejects_unsafe_sources(source: str) -> None:
    with pytest.raises(ValidationError):
        OutputArtifact(path="result.json", source=source)


def test_aws_credentials_are_frozen_and_secret_is_hidden() -> None:
    credentials = AWSCredentials(
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        aws_default_region="us-west-2",
    )

    assert "secret" not in repr(credentials)
    with pytest.raises(ValidationError):
        credentials.aws_default_region = "us-east-1"


def test_harness_config_serializes_expected_shape() -> None:
    config = HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="key",
            aws_secret_access_key="secret",
            aws_default_region="us-west-2",
        ),
        s3_bucket="bucket",
        log_group="benchmarks",
        log_retention_policy=30,
        sandbox_provider_secret_name="ModalSecret",
    )

    assert config.model_dump(mode="json")["aws"]["aws_secret_access_key"] == "secret"


def test_start_request_matches_canonical_wire_shape() -> None:
    payload = load_fixture("start.json")["request"]
    assert StartBenchmarkRequest.model_validate(payload).model_dump(mode="json") == payload


def test_nested_response_models_ignore_additive_fields() -> None:
    payload = load_fixture("fetch.json")["response"]
    assert isinstance(payload, dict)
    payload["future_top_level"] = True
    details = payload["details"]
    assert isinstance(details, dict)
    details["future_nested"] = {"value": 1}

    response = FetchBenchmarkResponse.model_validate(payload)

    assert response.benchmark_name == "swebench"


def test_non_empty_list_and_final_results_parse() -> None:
    list_response = FetchBenchmarksResponse.model_validate(load_fixture("list.json")["response"])
    result = FinalViewResponse.model_validate(load_fixture("results.json")["inline"])

    assert len(list_response.benchmarks) == 1
    assert result.final_evaluation is not None
    assert result.benchmark_arguments.contract.output_artifacts
    assert result.run_failure is None
    assert result.task_failures is None
    assert result.recovered_failure_count == 0
    assert result.secondary_failure_count == 0


def test_structured_failure_models_parse_versioned_provenance() -> None:
    payload = {
        "id": "10000000-0000-0000-0000-000000000001",
        "schema_version": 1,
        "category": "harness",
        "benchmark_id": "20000000-0000-0000-0000-000000000001",
        "task_id": "30000000-0000-0000-0000-000000000001",
        "task_attempt_id": "40000000-0000-0000-0000-000000000001",
        "retry_sequence": 2,
        "occurred_at": "2026-07-08T12:00:00Z",
        "producer": "benchmark_service",
        "operation": "evaluate",
        "error_type": "ConnectionClosedError",
        "message": "benchmark service connection closed",
        "classification_state": "classified",
        "cause_code": "websocket_closed",
        "terminal_effect": "recovered",
    }

    summary = FailureSummary.model_validate(payload)
    detail = FailureDetail.model_validate({**payload, "safe_details": {"websocket_close_code": 1011}})

    assert summary.category is FailureCategory.HARNESS
    assert summary.classification_state is FailureClassificationState.CLASSIFIED
    assert summary.terminal_effect is FailureTerminalEffect.RECOVERED
    assert summary.model_dump(mode="json")["occurred_at"] == "2026-07-08T12:00:00+00:00"
    assert detail.safe_details == {"websocket_close_code": 1011}


def test_single_task_parses_bounded_failure_detail_history() -> None:
    failure = {
        "id": "10000000-0000-0000-0000-000000000001",
        "schema_version": 1,
        "category": "valkyrie",
        "benchmark_id": "20000000-0000-0000-0000-000000000001",
        "task_id": "30000000-0000-0000-0000-000000000001",
        "task_attempt_id": "40000000-0000-0000-0000-000000000001",
        "retry_sequence": None,
        "occurred_at": "2026-07-08T12:05:00Z",
        "producer": "tracker",
        "operation": "process_task",
        "error_type": "RuntimeError",
        "message": "task failed",
        "classification_state": "unclassified",
        "cause_code": None,
        "terminal_effect": "terminal",
        "safe_details": None,
    }
    response = SingleTaskResponse.model_validate(
        {
            "id": "30000000-0000-0000-0000-000000000001",
            "task_id": "repo__issue-1",
            "status": "ERROR",
            "started_at": "2026-07-08T12:00:00Z",
            "finished_at": "2026-07-08T12:05:00Z",
            "error_message": "task failed",
            "evaluation_result": None,
            "agent_caused_exit_reason": None,
            "failure": failure,
            "failure_history": [failure],
            "failure_history_truncated": True,
        }
    )

    assert response.failure is not None
    assert response.failure.safe_details is None
    assert response.failure_history[0].terminal_effect is FailureTerminalEffect.TERMINAL
    assert response.failure_history_truncated is True
