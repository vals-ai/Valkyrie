"""Canonical V1 payloads accepted by the Tracker service models."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from benchmark_service.schemas import VerifyTaskIdsResponse
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
    AgentDownloadURLResponse,
    AgentEntry,
    AgentsResponse,
    AnalyzeBenchmarkRequest,
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkServiceCatalogResponse,
    BenchmarkServiceEntry,
    BenchmarkServiceHealth,
    BenchmarkServicesRequest,
    BenchmarkServicesResponse,
    BenchmarkStatusEntry,
    BenchmarkStatusResponse,
    BenchmarkTableRow,
    FetchBenchmarkResponse,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkTasksRequest,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FinalViewResponse,
    HarnessConfig,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    SingleBenchmarkResponse,
    SingleTaskResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StopBenchmarkResponse,
    TaskArtifactsResponse,
    TasksResponse,
    TaskSummary,
)
from valkyrie.sdk.models import (
    AWSCredentials as SDKAWSCredentials,
    AgentContractRequest as SDKAgentContractRequest,
    AgentDownloadURLResponse as SDKAgentDownloadURLResponse,
    AgentEntry as SDKAgentEntry,
    AgentsResponse as SDKAgentsResponse,
    AnalyzeBenchmarkRequest as SDKAnalyzeBenchmarkRequest,
    AverageTaskBreakdown as SDKAverageTaskBreakdown,
    BenchmarkArguments as SDKBenchmarkArguments,
    BenchmarkDetails as SDKBenchmarkDetails,
    BenchmarkServiceCatalogResponse as SDKBenchmarkServiceCatalogResponse,
    BenchmarkServiceEntry as SDKBenchmarkServiceEntry,
    BenchmarkServiceHealth as SDKBenchmarkServiceHealth,
    BenchmarkServicesRequest as SDKBenchmarkServicesRequest,
    BenchmarkServicesResponse as SDKBenchmarkServicesResponse,
    BenchmarkStatusEntry as SDKBenchmarkStatusEntry,
    BenchmarkStatusResponse as SDKBenchmarkStatusResponse,
    BenchmarkTableRow as SDKBenchmarkTableRow,
    FetchBenchmarkResponse as SDKFetchBenchmarkResponse,
    FetchBenchmarkMetadataResponse as SDKFetchBenchmarkMetadataResponse,
    FetchBenchmarkTasksRequest as SDKFetchBenchmarkTasksRequest,
    FetchBenchmarksRequest as SDKFetchBenchmarksRequest,
    FetchBenchmarksResponse as SDKFetchBenchmarksResponse,
    FinalEvaluation as SDKFinalEvaluation,
    FinalViewResponse as SDKFinalViewResponse,
    HarnessConfig as SDKHarnessConfig,
    OutputArtifact as SDKOutputArtifact,
    RetryOrResumeBenchmarkResponse as SDKRetryResponse,
    S3UploadResultsResponse as SDKS3ResultsResponse,
    SingleBenchmarkResponse as SDKSingleBenchmarkResponse,
    SingleTaskResponse as SDKSingleTaskResponse,
    StartBenchmarkRequest as SDKStartBenchmarkRequest,
    StartBenchmarkResponse as SDKStartBenchmarkResponse,
    StopBenchmarkResponse as SDKStopBenchmarkResponse,
    TaskArtifactsResponse as SDKTaskArtifactsResponse,
    TaskIDsResponse as SDKTaskIDsResponse,
    TasksResponse as SDKTasksResponse,
    TaskSummary as SDKTaskSummary,
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
    ("/benchmarks/status", "get", "ids"),
    ("/benchmarks/{benchmark_id}", "get", "benchmark_id"),
    (
        "/benchmarks/{benchmark_id}/tasks",
        "get",
        "benchmark_id status task_id_search sort sort_dir limit offset",
    ),
    ("/benchmarks/{benchmark_id}/tasks/{task_id}", "get", "benchmark_id task_id"),
    ("/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts", "get", "benchmark_id task_id"),
    ("/agents", "get", ""),
    ("/agents/{name}/download-url", "get", "name"),
    ("/benchmark-services", "get", ""),
    ("/benchmark-services", "post", ""),
    ("/fetch-benchmark-tasks", "post", ""),
    ("/analyze-benchmark/{benchmark_id}", "post", "benchmark_id"),
    ("/check-results-exist", "get", "benchmark_id"),
    ("/fetch-benchmark-metadata/{benchmark_id}", "get", "benchmark_id"),
    ("/fetch-run-outputs/{benchmark_id}", "get", "benchmark_id task_ids"),
)
RESPONSE_MODELS = {
    ("/start-benchmark", "post"): "StartBenchmarkResponse",
    ("/fetch-benchmarks", "get"): "FetchBenchmarksResponse",
    ("/stop-benchmark/{benchmark_id}", "post"): "StopBenchmarkResponse",
    ("/retry-or-resume-benchmark/{benchmark_id}", "post"): "RetryOrResumeBenchmarkResponse",
    ("/benchmarks/status", "get"): "BenchmarkStatusResponse",
    ("/benchmarks/{benchmark_id}", "get"): "SingleBenchmarkResponse",
    ("/benchmarks/{benchmark_id}/tasks", "get"): "TasksResponse",
    ("/benchmarks/{benchmark_id}/tasks/{task_id}", "get"): "SingleTaskResponse",
    ("/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts", "get"): "TaskArtifactsResponse",
    ("/agents", "get"): "AgentsResponse",
    ("/agents/{name}/download-url", "get"): "AgentDownloadURLResponse",
    ("/benchmark-services", "get"): "BenchmarkServiceCatalogResponse",
    ("/benchmark-services", "post"): "BenchmarkServicesResponse",
    ("/fetch-benchmark-tasks", "post"): "VerifyTaskIdsResponse",
    ("/fetch-benchmark-metadata/{benchmark_id}", "get"): "FetchBenchmarkMetadataResponse",
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
    (BenchmarkStatusEntry, SDKBenchmarkStatusEntry),
    (BenchmarkStatusResponse, SDKBenchmarkStatusResponse),
    (SingleBenchmarkResponse, SDKSingleBenchmarkResponse),
    (TaskSummary, SDKTaskSummary),
    (TasksResponse, SDKTasksResponse),
    (SingleTaskResponse, SDKSingleTaskResponse),
    (TaskArtifactsResponse, SDKTaskArtifactsResponse),
    (AgentEntry, SDKAgentEntry),
    (AgentsResponse, SDKAgentsResponse),
    (AgentDownloadURLResponse, SDKAgentDownloadURLResponse),
    (BenchmarkServiceEntry, SDKBenchmarkServiceEntry),
    (BenchmarkServiceHealth, SDKBenchmarkServiceHealth),
    (BenchmarkServicesRequest, SDKBenchmarkServicesRequest),
    (BenchmarkServicesResponse, SDKBenchmarkServicesResponse),
    (BenchmarkServiceCatalogResponse, SDKBenchmarkServiceCatalogResponse),
    (FetchBenchmarkTasksRequest, SDKFetchBenchmarkTasksRequest),
    (VerifyTaskIdsResponse, SDKTaskIDsResponse),
    (AnalyzeBenchmarkRequest, SDKAnalyzeBenchmarkRequest),
    (FetchBenchmarkMetadataResponse, SDKFetchBenchmarkMetadataResponse),
)
INTERNAL_ROUTES = {
    ("/benchmarks/filter-options", "get"),
    ("/health", "get"),
    ("/init", "post"),
}


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
    tracker_schema = tracker_model.model_json_schema()
    sdk_schema = sdk_model.model_json_schema()
    assert set(tracker_schema.get("required", [])) == set(sdk_schema.get("required", []))

    for name in tracker_model.model_fields:
        tracker_property = _normalized_wire_schema(tracker_schema["properties"][name])
        sdk_property = _normalized_wire_schema(sdk_schema["properties"][name])
        assert tracker_property == sdk_property

        tracker_field = tracker_model.model_fields[name]
        sdk_field = sdk_model.model_fields[name]
        if tracker_field.is_required() or (tracker_field.default_factory and sdk_field.default_factory):
            continue
        tracker_default = tracker_field.get_default(call_default_factory=True)
        sdk_default = sdk_field.get_default(call_default_factory=True)
        assert _normalized_default(tracker_default) == _normalized_default(sdk_default)


def _normalized_wire_schema(value: Any) -> Any:
    """Compare wire shapes while ignoring documentation and intentional ID format hints."""
    if isinstance(value, dict):
        return {
            key: _normalized_wire_schema(item)
            for key, item in value.items()
            if key not in {"default", "description", "format", "title"}
        }
    if isinstance(value, list):
        return [_normalized_wire_schema(item) for item in value]
    return value


def _normalized_default(value: Any) -> Any:
    """Compare enum defaults by their wire value."""
    if isinstance(value, Enum):
        return value.value
    return value


def test_fetch_stream_fixture_matches_tracker_and_sdk_response_models() -> None:
    event = load_fixture("fetch.json")["sse"]

    assert event["event"] == ""
    tracker_value = FetchBenchmarkResponse.model_validate(event["data"])
    sdk_value = SDKFetchBenchmarkResponse.model_validate(event["data"])
    assert tracker_value.model_dump(mode="json") == event["data"]
    assert sdk_value.model_dump(mode="json") == event["data"]


def test_tracker_routes_match_the_sdk_http_contract() -> None:
    schema = app.openapi()
    expected_methods: dict[str, set[str]] = {}
    for path, method, _names in ROUTES:
        expected_methods.setdefault(path, set()).add(method)
    for path, methods in expected_methods.items():
        assert set(schema["paths"][path]) == methods

    for path, method, names in ROUTES:
        operation = schema["paths"][path][method]
        actual_parameters = {(parameter["name"], parameter["in"]) for parameter in operation.get("parameters", [])}
        expected_parameters = {(name, "path" if f"{{{name}}}" in path else "query") for name in names.split()}
        assert actual_parameters == expected_parameters

    start = schema["paths"]["/start-benchmark"]["post"]
    assert start["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/StartBenchmarkRequest"
    }
    for path, model in (
        ("/analyze-benchmark/{benchmark_id}", "AnalyzeBenchmarkRequest"),
        ("/benchmark-services", "BenchmarkServicesRequest"),
        ("/fetch-benchmark-tasks", "FetchBenchmarkTasksRequest"),
    ):
        operation = schema["paths"][path]["post"]
        assert operation["requestBody"]["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{model}"
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

    status_parameters = {
        parameter["name"]: parameter for parameter in schema["paths"]["/benchmarks/status"]["get"]["parameters"]
    }
    assert status_parameters["ids"]["schema"]["default"] == ""

    task_parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"]["/benchmarks/{benchmark_id}/tasks"]["get"]["parameters"]
    }
    assert task_parameters["status"]["schema"]["default"] == ""
    assert task_parameters["sort"]["schema"]["enum"] == ["task_id", "started_at", "duration", "status"]
    assert task_parameters["sort"]["schema"]["default"] == "started_at"
    assert task_parameters["sort_dir"]["schema"]["enum"] == ["asc", "desc"]
    assert task_parameters["sort_dir"]["schema"]["default"] == "desc"
    assert task_parameters["limit"]["schema"] == {
        "type": "integer",
        "maximum": 500,
        "minimum": 1,
        "default": 50,
        "title": "Limit",
    }
    assert task_parameters["offset"]["schema"] == {
        "type": "integer",
        "minimum": 0,
        "default": 0,
        "title": "Offset",
    }

    for path, method, parameter_name in (
        ("/fetch-benchmark", "get", "benchmark_id"),
        ("/retrieve-results", "get", "benchmark_id"),
        ("/stop-benchmark/{benchmark_id}", "post", "benchmark_id"),
        ("/retry-or-resume-benchmark/{benchmark_id}", "post", "benchmark_id"),
        ("/analyze-benchmark/{benchmark_id}", "post", "benchmark_id"),
        ("/check-results-exist", "get", "benchmark_id"),
        ("/fetch-benchmark-metadata/{benchmark_id}", "get", "benchmark_id"),
        ("/fetch-run-outputs/{benchmark_id}", "get", "benchmark_id"),
    ):
        operation = schema["paths"][path][method]
        parameter = next(item for item in operation["parameters"] if item["name"] == parameter_name)
        assert parameter["required"] is True
        assert parameter["schema"]["format"] == "uuid"

    for (path, method), model in RESPONSE_MODELS.items():
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


def test_every_tracker_route_is_classified_as_sdk_supported_or_internal() -> None:
    schema = app.openapi()
    tracker_routes = {(path, method) for path, operations in schema["paths"].items() for method in operations}
    sdk_routes = {(path, method) for path, method, _names in ROUTES}

    assert tracker_routes == sdk_routes | INTERNAL_ROUTES


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
