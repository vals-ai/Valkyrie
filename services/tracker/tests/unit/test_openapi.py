"""Checks for the committed Tracker OpenAPI contract."""

import json
from pathlib import Path

from main import app


def test_openapi_snapshot_matches_app() -> None:
    snapshot_path = Path(__file__).parents[2] / "openapi.json"
    snapshot = json.loads(snapshot_path.read_text())

    assert snapshot == app.openapi()


def test_openapi_declares_authentication() -> None:
    schema = app.openapi()
    assert schema["components"]["securitySchemes"] == {
        "DescopeAccessKey": {
            "type": "apiKey",
            "in": "header",
            "name": "x-api-key",
        },
        "DescopeBearer": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        },
    }

    for path, path_item in schema["paths"].items():
        for operation in path_item.values():
            if not isinstance(operation, dict) or "responses" not in operation:
                continue
            if path == "/health":
                assert "security" not in operation
            elif path == "/init":
                assert operation["security"] == [{"DescopeAccessKey": []}]
            else:
                assert operation["security"] == [
                    {"DescopeBearer": []},
                    {"DescopeAccessKey": []},
                ]


def test_mutation_receipt_openapi_contract() -> None:
    schema = app.openapi()
    security = [{"DescopeBearer": []}, {"DescopeAccessKey": []}]
    status = schema["paths"]["/operations/{operation_id}"]["get"]

    assert status["security"] == security
    assert status["responses"]["200"]["content"]["application/json"]["schema"]["discriminator"] == {
        "propertyName": "state",
        "mapping": {
            "failed": "#/components/schemas/FailedMutationOperationResponse",
            "processing": "#/components/schemas/ProcessingMutationOperationResponse",
            "succeeded": "#/components/schemas/SucceededMutationOperationResponse",
            "uncertain": "#/components/schemas/UncertainMutationOperationResponse",
        },
    }

    paths = (
        "/analyze-benchmark/{benchmark_id}",
        "/start-benchmark",
        "/stop-benchmark/{benchmark_id}",
        "/retry-or-resume-benchmark/{benchmark_id}",
    )
    for path in paths:
        operation = schema["paths"][path]["post"]
        header = next(parameter for parameter in operation["parameters"] if parameter["name"] == "Idempotency-Key")

        assert operation["security"] == security
        assert header == {
            "name": "Idempotency-Key",
            "in": "header",
            "required": False,
            "description": (
                "Required and used for bearer-authenticated mutations; ignored for access-key and self-hosted callers."
            ),
            "schema": {
                "anyOf": [
                    {"type": "string", "format": "uuid"},
                    {"type": "null"},
                ],
                "description": (
                    "Required and used for bearer-authenticated mutations; ignored for access-key and self-hosted callers."
                ),
                "title": "Idempotency-Key",
            },
        }


def test_task_attempts_openapi_is_paginated_and_discriminated() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/benchmarks/{benchmark_id}/tasks/{task_id}/attempts"]["get"]
    response = operation["responses"]["200"]["content"]["application/json"]["schema"]
    attempts = schema["components"]["schemas"]["TaskAttemptsResponse"]["properties"]["attempts"]["items"]
    offset = next(parameter for parameter in operation["parameters"] if parameter["name"] == "offset")

    assert response == {"$ref": "#/components/schemas/TaskAttemptsResponse"}
    assert "maximum" not in offset["schema"]
    assert attempts["discriminator"] == {
        "propertyName": "kind",
        "mapping": {
            "error": "#/components/schemas/ErrorTaskAttempt",
            "evaluation": "#/components/schemas/EvaluationTaskAttempt",
            "execution": "#/components/schemas/ExecutionTaskAttempt",
        },
    }
    assert {item["$ref"] for item in attempts["oneOf"]} == {
        "#/components/schemas/ErrorTaskAttempt",
        "#/components/schemas/EvaluationTaskAttempt",
        "#/components/schemas/ExecutionTaskAttempt",
    }
    assert "result" not in schema["components"]["schemas"]["EvaluationTaskAttempt"]["properties"]


def test_task_log_openapi_has_strict_attempt_and_cursor_contracts() -> None:
    schema = app.openapi()
    attempts = schema["paths"]["/benchmarks/{benchmark_id}/tasks/{task_id}/log-attempts"]["get"]
    events = schema["paths"]["/benchmarks/{benchmark_id}/tasks/{task_id}/log-attempts/{attempt_id}/events"]["get"]

    attempts_limit = next(parameter for parameter in attempts["parameters"] if parameter["name"] == "limit")
    attempt_id = next(parameter for parameter in events["parameters"] if parameter["name"] == "attempt_id")
    direction = next(parameter for parameter in events["parameters"] if parameter["name"] == "direction")

    assert attempts_limit["schema"]["maximum"] == 50
    assert attempt_id["schema"]["pattern"] == "^[0-9a-f]+$"
    assert direction["schema"]["enum"] == ["forward", "backward"]
    assert attempts["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/TaskLogAttemptsResponse"
    }
    assert events["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/TaskLogEventsResponse"
    }


def test_task_artifact_openapi_is_paginated_and_range_readable() -> None:
    schema = app.openapi()
    prefix = "/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts"
    index = schema["paths"][f"{prefix}/index"]["get"]
    files = schema["paths"][f"{prefix}/files"]["get"]
    content = schema["paths"][f"{prefix}/content"]["get"]
    archive = schema["paths"][f"{prefix}/archive"]["get"]

    attempt_id = next(parameter for parameter in index["parameters"] if parameter["name"] == "attempt_id")
    limit = next(parameter for parameter in files["parameters"] if parameter["name"] == "limit")
    content_response = content["responses"]["200"]["content"]["application/json"]["schema"]

    assert attempt_id["schema"]["anyOf"][0]["pattern"] == "^[0-9a-f]+$"
    assert limit["schema"]["default"] == 200
    assert limit["schema"]["maximum"] == 500
    assert content_response == {"$ref": "#/components/schemas/TaskArtifactContentResponse"}
    assert "303" in archive["responses"]
