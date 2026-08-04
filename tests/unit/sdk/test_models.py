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
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksResponse,
    FinalViewResponse,
    GetRunResponse,
    HarnessConfig,
    ListRunsResponse,
    OutputArtifact,
    RunResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StartRunRequest,
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


def test_released_legacy_response_models_keep_legacy_default_dump_keys() -> None:
    start_payload = load_fixture("start.json")["response"]
    start_payload["benchmark_id"] = start_payload.pop("run_id")
    start = StartBenchmarkResponse.model_validate(start_payload)

    fetch_payload = load_fixture("fetch.json")["response"]
    fetch_payload["benchmark_id"] = fetch_payload.pop("run_id")
    fetched = FetchBenchmarkResponse.model_validate(fetch_payload)

    list_payload = load_fixture("list.json")["response"]
    list_payload["benchmarks"] = list_payload.pop("runs")
    for run in list_payload["benchmarks"]:
        run["id"] = run.pop("run_id")
        run["name"] = run.pop("benchmark_name")
    listed = FetchBenchmarksResponse.model_validate(list_payload)

    results_payload = load_fixture("results.json")["inline"]
    results_payload["benchmark_id"] = results_payload.pop("run_id")
    results_payload["benchmark_arguments"] = results_payload.pop("run_arguments")
    results_payload["final_evaluation"]["benchmark"] = results_payload["final_evaluation"].pop("run_id")
    results = FinalViewResponse.model_validate(results_payload)

    metadata = FetchBenchmarkMetadataResponse.model_validate(
        {
            "benchmark_id": start_payload["benchmark_id"],
            "benchmark_name": "swebench",
            "benchmark_arguments": results_payload["benchmark_arguments"],
        }
    )

    assert type(start).__name__ == "StartBenchmarkResponse"
    assert set(start.model_dump()) >= {"benchmark_id"}
    assert "run_id" not in start.model_dump()
    assert set(fetched.model_dump()) >= {"benchmark_id"}
    assert "run_id" not in fetched.model_dump()
    assert set(listed.model_dump()) >= {"benchmarks"}
    assert "runs" not in listed.model_dump()
    assert set(listed.model_dump()["benchmarks"][0]) >= {"id", "name"}
    assert not {"run_id", "benchmark_name"} & set(listed.model_dump()["benchmarks"][0])
    assert set(metadata.model_dump()) >= {"benchmark_id", "benchmark_arguments"}
    assert not {"run_id", "run_arguments"} & set(metadata.model_dump())
    assert set(results.model_dump()) >= {"benchmark_id", "benchmark_arguments"}
    assert not {"run_id", "run_arguments"} & set(results.model_dump())
    assert "benchmark" in results.model_dump()["final_evaluation"]
    assert "run_id" not in results.model_dump()["final_evaluation"]


def test_released_start_request_keeps_legacy_concurrency_coercion() -> None:
    payload = StartRunRequest(
        contract={"name": "agent", "description": "description", "artifacts": []},
        benchmark_name="swebench",
        harness_config={
            "aws": {
                "aws_access_key_id": "access-key",
                "aws_secret_access_key": "secret-key",
                "aws_default_region": "us-west-2",
            },
            "s3_bucket": "bucket",
            "log_group": "logs",
            "log_retention_policy": 7,
            "sandbox_provider_secret_name": "secret",
        },
    ).model_dump()
    payload["concurrency"] = "2"

    assert StartBenchmarkRequest.model_validate(payload).concurrency == 2
