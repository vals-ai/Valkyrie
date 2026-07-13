"""Canonical V1 payloads accepted by the Tracker service models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from services.tracker.main import app
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    FinalEvaluation,
    OutputArtifact,
)
from tracker.types import (
    AWSCredentials,
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    HarnessConfig,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
)
from valkyrie.sdk.models import (
    AWSCredentials as SDKAWSCredentials,
    AgentContractRequest as SDKAgentContractRequest,
    AverageTaskBreakdown as SDKAverageTaskBreakdown,
    BenchmarkArguments as SDKBenchmarkArguments,
    BenchmarkDetails as SDKBenchmarkDetails,
    BenchmarkTableRow as SDKBenchmarkTableRow,
    FetchBenchmarkResponse as SDKFetchBenchmarkResponse,
    FetchBenchmarksRequest as SDKFetchBenchmarksRequest,
    FetchBenchmarksResponse as SDKFetchBenchmarksResponse,
    FinalEvaluation as SDKFinalEvaluation,
    FinalViewResponse as SDKFinalViewResponse,
    HarnessConfig as SDKHarnessConfig,
    OutputArtifact as SDKOutputArtifact,
    RetryOrResumeBenchmarkResponse as SDKRetryResponse,
    S3UploadResultsResponse as SDKS3ResultsResponse,
    StartBenchmarkRequest as SDKStartBenchmarkRequest,
    StartBenchmarkResponse as SDKStartBenchmarkResponse,
    StopBenchmarkResponse as SDKStopBenchmarkResponse,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sdk_api"
ROUTES = (
    ("/start-benchmark", "post", ""),
    ("/fetch-benchmark", "get", "benchmark_id connect"),
    (
        "/fetch-benchmarks",
        "get",
        "agent_name benchmark_name model dataset label status started_by started_after started_before order_by cursor limit offset",
    ),
    ("/retrieve-results", "get", "benchmark_id s3 task_ids"),
    ("/stop-benchmark/{benchmark_id}", "post", "benchmark_id force"),
    ("/retry-or-resume-benchmark/{benchmark_id}", "post", "benchmark_id retry retry_mode concurrency"),
)
RESPONSE_MODELS = {
    "/start-benchmark": "StartBenchmarkResponse",
    "/fetch-benchmarks": "FetchBenchmarksResponse",
    "/stop-benchmark/{benchmark_id}": "StopBenchmarkResponse",
    "/retry-or-resume-benchmark/{benchmark_id}": "RetryOrResumeBenchmarkResponse",
}
MODEL_PAIRS = (
    (OutputArtifact, SDKOutputArtifact),
    (AgentContractRequest, SDKAgentContractRequest),
    (AWSCredentials, SDKAWSCredentials),
    (HarnessConfig, SDKHarnessConfig),
    (StartBenchmarkRequest, SDKStartBenchmarkRequest),
    (BenchmarkDetails, SDKBenchmarkDetails),
    (StartBenchmarkResponse, SDKStartBenchmarkResponse),
    (FetchBenchmarkResponse, SDKFetchBenchmarkResponse),
    (FetchBenchmarksRequest, SDKFetchBenchmarksRequest),
    (BenchmarkTableRow, SDKBenchmarkTableRow),
    (FetchBenchmarksResponse, SDKFetchBenchmarksResponse),
    (BenchmarkArguments, SDKBenchmarkArguments),
    (FinalEvaluation, SDKFinalEvaluation),
    (AverageTaskBreakdown, SDKAverageTaskBreakdown),
    (FinalViewResponse, SDKFinalViewResponse),
    (S3UploadResultsResponse, SDKS3ResultsResponse),
    (StopBenchmarkResponse, SDKStopBenchmarkResponse),
    (RetryOrResumeBenchmarkResponse, SDKRetryResponse),
)


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("name", "key", "tracker_model", "sdk_model"),
    [
        ("start.json", "request", StartBenchmarkRequest, SDKStartBenchmarkRequest),
        ("start.json", "response", StartBenchmarkResponse, SDKStartBenchmarkResponse),
        ("fetch.json", "response", FetchBenchmarkResponse, SDKFetchBenchmarkResponse),
        ("list.json", "request", FetchBenchmarksRequest, SDKFetchBenchmarksRequest),
        ("list.json", "response", FetchBenchmarksResponse, SDKFetchBenchmarksResponse),
        ("results.json", "inline", FinalViewResponse, SDKFinalViewResponse),
        ("results.json", "s3", S3UploadResultsResponse, SDKS3ResultsResponse),
        ("stop.json", "response", StopBenchmarkResponse, SDKStopBenchmarkResponse),
        ("retry_resume.json", "response", RetryOrResumeBenchmarkResponse, SDKRetryResponse),
    ],
)
def test_sdk_and_tracker_accept_canonical_fixture(
    name: str,
    key: str,
    tracker_model: type[BaseModel],
    sdk_model: type[BaseModel],
) -> None:
    payload = load_fixture(name)[key]
    tracker_value = tracker_model.model_validate(payload)
    sdk_value = sdk_model.model_validate(payload)

    assert isinstance(tracker_value, tracker_model)
    assert isinstance(sdk_value, sdk_model)
    assert tracker_value.model_dump(mode="json", warnings=False) == payload
    assert sdk_value.model_dump(mode="json") == payload


@pytest.mark.parametrize(("tracker_model", "sdk_model"), MODEL_PAIRS)
def test_sdk_and_tracker_wire_models_have_the_same_fields(
    tracker_model: type[BaseModel], sdk_model: type[BaseModel]
) -> None:
    assert tracker_model.model_fields.keys() == sdk_model.model_fields.keys()


def test_fetch_stream_fixture_matches_tracker_and_sdk_response_models() -> None:
    event = load_fixture("fetch.json")["sse"]

    assert event["event"] == ""
    tracker_value = FetchBenchmarkResponse.model_validate(event["data"])
    sdk_value = SDKFetchBenchmarkResponse.model_validate(event["data"])
    assert tracker_value.model_dump(mode="json") == event["data"]
    assert sdk_value.model_dump(mode="json") == event["data"]


def test_tracker_routes_match_the_sdk_http_contract() -> None:
    schema = app.openapi()
    for path, method, names in ROUTES:
        assert set(schema["paths"][path]) == {method}
        operation = schema["paths"][path][method]
        actual_parameters = {(parameter["name"], parameter["in"]) for parameter in operation.get("parameters", [])}
        expected_parameters = {(name, "path" if f"{{{name}}}" in path else "query") for name in names.split()}
        assert actual_parameters == expected_parameters

    start = schema["paths"]["/start-benchmark"]["post"]
    assert start["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/StartBenchmarkRequest"
    }
    retry = schema["paths"]["/retry-or-resume-benchmark/{benchmark_id}"]["post"]
    retry_schema_ref = retry["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    retry_schema = schema["components"]["schemas"][retry_schema_ref.rsplit("/", 1)[-1]]
    retry_fixture = load_fixture("retry_resume.json")
    assert set(retry_schema["properties"]) == set(retry_fixture["body"])
    assert {name: (value["type"], value["default"]) for name, value in retry_schema["properties"].items()} == {
        "task_ids": ("array", []),
        "service_headers": ("object", {}),
        "secrets": ("object", {}),
    }

    retry_parameters = {parameter["name"]: parameter for parameter in retry["parameters"]}
    assert retry_parameters["retry"]["schema"]["default"] == retry_fixture["query"]["retry"]
    assert retry_parameters["retry_mode"]["schema"]["default"] == retry_fixture["query"]["retry_mode"]
    assert {option.get("type") for option in retry_parameters["concurrency"]["schema"]["anyOf"]} == {
        "integer",
        "null",
    }
    assert isinstance(retry_fixture["query"]["concurrency"], int)

    fetch = schema["paths"]["/fetch-benchmark"]["get"]
    fetch_parameters = {parameter["name"]: parameter for parameter in fetch["parameters"]}
    assert fetch_parameters["connect"]["required"] is False
    assert fetch_parameters["connect"]["schema"] == {
        "type": "boolean",
        "default": False,
        "title": "Connect",
    }
    assert "default" not in retry_parameters["concurrency"]["schema"]

    for path, method, parameter_name in (
        ("/fetch-benchmark", "get", "benchmark_id"),
        ("/retrieve-results", "get", "benchmark_id"),
        ("/stop-benchmark/{benchmark_id}", "post", "benchmark_id"),
        ("/retry-or-resume-benchmark/{benchmark_id}", "post", "benchmark_id"),
    ):
        operation = schema["paths"][path][method]
        parameter = next(item for item in operation["parameters"] if item["name"] == parameter_name)
        assert parameter["required"] is True
        assert parameter["schema"]["format"] == "uuid"

    for path, model in RESPONSE_MODELS.items():
        method = next(method for route, method, _names in ROUTES if route == path)
        response = schema["paths"][path][method]["responses"]["200"]["content"]["application/json"]["schema"]
        assert response == {"$ref": f"#/components/schemas/{model}"}
    assert schema["paths"]["/fetch-benchmark"]["get"]["responses"]["200"]["content"]["application/json"]["schema"] == {}
    result_schema = schema["paths"]["/retrieve-results"]["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert {item["$ref"].rsplit("/", 1)[-1] for item in result_schema["anyOf"]} == {
        "FinalViewResponse",
        "S3UploadResultsResponse",
    }


def test_final_evaluation_preserves_tracker_runtime_string_ids() -> None:
    payload = load_fixture("results.json")["inline"]
    tracker_evaluation = FinalViewResponse.model_validate(payload).final_evaluation
    sdk_evaluation = SDKFinalViewResponse.model_validate(payload).final_evaluation

    assert tracker_evaluation is not None
    assert sdk_evaluation is not None
    for field in ("id", "org_id", "benchmark"):
        tracker_value = getattr(tracker_evaluation, field)
        sdk_value = getattr(sdk_evaluation, field)
        assert isinstance(tracker_value, str)
        assert type(sdk_value) is type(tracker_value)
    assert sdk_evaluation.model_dump() == tracker_evaluation.model_dump(warnings=False)
