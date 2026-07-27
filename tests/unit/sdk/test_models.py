"""Behavioral tests for SDK-owned request and response models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from valkyrie.sdk.models import (
    AWSCredentials,
    AgentContractRequest,
    GetRunResponse,
    HarnessConfig,
    ListRunsResponse,
    OutputArtifact,
    RunResultsResponse,
    StartRunRequest,
    StartRunResponse,
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
        ],
    )

    assert contract.output_artifacts[0] == "reports/result.json"
    assert contract.output_artifacts[1] == OutputArtifact(
        path="logs/run.txt",
        source="/workspace/logs/*.txt",
    )


@pytest.mark.parametrize("path", ["/absolute", "../escape", "a/../b"])
def test_agent_contract_rejects_unsafe_artifact_paths(path: str) -> None:
    with pytest.raises(ValidationError):
        AgentContractRequest(name="agent", output_artifacts=[path])


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
    assert StartRunRequest.model_validate(payload).model_dump(mode="json") == payload


def test_nested_response_models_ignore_additive_fields() -> None:
    payload = load_fixture("fetch.json")["response"]
    assert isinstance(payload, dict)
    payload["future_top_level"] = True
    details = payload["details"]
    assert isinstance(details, dict)
    details["future_nested"] = {"value": 1}

    response = GetRunResponse.model_validate(payload)

    assert response.benchmark_name == "swebench"


def test_non_empty_list_and_final_results_parse() -> None:
    list_response = ListRunsResponse.model_validate(load_fixture("list.json")["response"])
    result = RunResultsResponse.model_validate(load_fixture("results.json")["inline"])

    assert len(list_response.runs) == 1
    assert result.final_evaluation is not None
    assert result.run_arguments.contract.output_artifacts


def test_canonical_run_models_reject_legacy_wire_names() -> None:
    run_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    payload = {
        "benchmark_name": "swebench",
        "agent_name": "agent",
        "run_id": run_id,
        "concurrency": 1,
        "started_at": "2026-07-08T12:00:00Z",
        "task_count": 1,
        "cloudwatch_url": "https://logs.test",
        "s3_bucket_url": "s3://bucket/run",
    }

    legacy_payload = {**payload, "benchmark_id": payload["run_id"]}
    del legacy_payload["run_id"]
    with pytest.raises(ValidationError):
        StartRunResponse.model_validate(legacy_payload)

    with pytest.raises(ValidationError):
        ListRunsResponse.model_validate({"benchmarks": []})
