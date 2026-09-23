"""Checks for the committed Tracker API contract."""

import json
from pathlib import Path
from typing import Any

from generate_openapi import build_openapi


def test_openapi_snapshot_matches_generator() -> None:
    snapshot_path = Path(__file__).parents[2] / "openapi.json"

    expected = json.dumps(build_openapi(), indent=2, sort_keys=True) + "\n"
    assert snapshot_path.read_text() == expected


def test_openapi_declares_authentication() -> None:
    schema = build_openapi()

    assert schema["components"]["securitySchemes"] == {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        },
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "x-api-key",
        },
        "ExecutorDispatchAuth": {
            "type": "http",
            "scheme": "bearer",
        },
    }
    assert schema["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert schema["paths"]["/health"]["get"]["security"] == []
    assert schema["paths"]["/init"]["post"]["security"] == [{"ApiKeyAuth": []}]
    assert schema["paths"]["/start-benchmark"]["post"]["security"] == [{"ApiKeyAuth": []}]
    assert schema["paths"]["/start-benchmark-with-storage"]["post"]["security"] == [{"ApiKeyAuth": []}]
    for operation in ("claim", "authority", "heartbeat", "finish", "fail", "run/initialize", "run/state"):
        path = f"/internal/executor/v1/dispatches/{{dispatch_id}}/{operation}"
        assert schema["paths"][path]["post"]["security"] == [{"ExecutorDispatchAuth": []}]
    for operation in ("claim", "write"):
        path = f"/internal/executor/v1/dispatches/{{dispatch_id}}/tasks/{{task_id}}/{operation}"
        assert schema["paths"][path]["post"]["security"] == [{"ExecutorDispatchAuth": []}]


def test_openapi_declares_required_harness_headers() -> None:
    schema = build_openapi()
    expected_parameters = {
        "HarnessAwsAccessKeyId": {
            "name": "X-Harness-AWS-Access-Key-Id",
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        },
        "HarnessAwsSecretAccessKey": {
            "name": "X-Harness-AWS-Secret-Access-Key",
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        },
        "HarnessAwsDefaultRegion": {
            "name": "X-Harness-AWS-Default-Region",
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        },
        "HarnessS3Bucket": {
            "name": "X-Harness-S3-Bucket",
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        },
    }
    expected_references = [{"$ref": f"#/components/parameters/{name}"} for name in expected_parameters]
    affected_operations = (
        schema["paths"]["/agents"]["get"],
        schema["paths"]["/agents/{name}/download-url"]["get"],
        schema["paths"]["/analyze-benchmark/{benchmark_id}"]["post"],
        schema["paths"]["/benchmarks/{benchmark_id}/logs"]["get"],
        schema["paths"]["/benchmarks/{benchmark_id}/logs/stream"]["get"],
        schema["paths"]["/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts"]["get"],
        schema["paths"]["/check-results-exist"]["get"],
        schema["paths"]["/fetch-benchmark"]["get"],
        schema["paths"]["/fetch-benchmark-tasks"]["post"],
        schema["paths"]["/fetch-run-outputs/{benchmark_id}"]["get"],
        schema["paths"]["/retrieve-results"]["get"],
        schema["paths"]["/retry-or-resume-benchmark/{benchmark_id}"]["post"],
        schema["paths"]["/stop-benchmark/{benchmark_id}"]["post"],
    )

    assert schema["components"]["parameters"] == expected_parameters
    for operation in affected_operations:
        assert operation["parameters"][-4:] == expected_references


def test_openapi_states_the_storage_requirement_of_each_start_route() -> None:
    schema = build_openapi()

    def request_model(path: str) -> dict[str, Any]:
        body = schema["paths"][path]["post"]["requestBody"]["content"]["application/json"]["schema"]
        return schema["components"]["schemas"][body["$ref"].rsplit("/", 1)[-1]]

    shared = request_model("/start-benchmark")
    managed = request_model("/start-benchmark-with-storage")

    assert "managed_s3_bucket" not in shared["required"]
    assert "managed_s3_bucket" in managed["required"]


def test_openapi_keeps_scheduler_storage_fields_internal() -> None:
    schemas = build_openapi()["components"]["schemas"]

    benchmark_argument_properties = schemas["BenchmarkArguments"]["properties"]
    assert "priority" not in benchmark_argument_properties
    assert "queue_pool_id" not in benchmark_argument_properties
    assert "priority" in schemas["StartBenchmarkRequest"]["properties"]


def test_openapi_includes_scheduler_overview_contract() -> None:
    operation = build_openapi()["paths"]["/scheduler/overview"]["get"]
    parameters = {parameter["name"]: parameter["schema"] for parameter in operation["parameters"]}

    assert parameters == {
        "waiting_limit": {
            "type": "integer",
            "maximum": 200,
            "minimum": 1,
            "default": 100,
            "title": "Waiting Limit",
        },
        "active_limit": {
            "type": "integer",
            "maximum": 200,
            "minimum": 1,
            "default": 100,
            "title": "Active Limit",
        },
        "waiting_offset": {"type": "integer", "minimum": 0, "default": 0, "title": "Waiting Offset"},
        "active_offset": {"type": "integer", "minimum": 0, "default": 0, "title": "Active Offset"},
        "include_capacity": {
            "type": "boolean",
            "default": False,
            "title": "Include Capacity",
        },
    }
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SchedulerOverviewResponse"
    }

    schemas = build_openapi()["components"]["schemas"]
    pool_schema = schemas["SchedulerPoolResponse"]
    assert pool_schema["required"] == ["pool_id", "waiting"]
    assert set(pool_schema["properties"]) == {"pool_id", "waiting", "provider", "capacity_domains"}
    assert schemas["SchedulerCapacityDomainResponse"]["required"] == ["target_id", "sandbox_class", "capacity"]
    capacity_schema = schemas["SchedulerCapacityResponse"]
    assert capacity_schema["required"] == ["cpu", "memory", "disk"]
    assert set(capacity_schema["properties"]) == {
        "cpu",
        "memory",
        "disk",
        "gpu",
        "allowed_gpu_types",
    }
    assert capacity_schema["properties"]["gpu"] == {
        "anyOf": [
            {"$ref": "#/components/schemas/SchedulerResourceCapacityResponse"},
            {"type": "null"},
        ]
    }
    assert capacity_schema["properties"]["allowed_gpu_types"] == {
        "anyOf": [
            {"type": "array", "items": {"type": "string"}},
            {"type": "null"},
        ],
        "title": "Allowed Gpu Types",
    }
    assert schemas["SchedulerResourceCapacityResponse"]["properties"] == {
        "available": {"type": "number", "minimum": 0.0, "title": "Available"},
        "total": {"type": "number", "minimum": 0.0, "title": "Total"},
    }
