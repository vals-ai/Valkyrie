"""Generate the committed Tracker API contract."""

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from main import app

OPENAPI_PATH = Path(__file__).with_name("openapi.json")
BEARER_OR_API_KEY: list[dict[str, list[str]]] = [{"BearerAuth": []}, {"ApiKeyAuth": []}]
API_KEY_ONLY: list[dict[str, list[str]]] = [{"ApiKeyAuth": []}]
HARNESS_HEADERS = (
    ("HarnessAwsAccessKeyId", "X-Harness-AWS-Access-Key-Id"),
    ("HarnessAwsSecretAccessKey", "X-Harness-AWS-Secret-Access-Key"),
    ("HarnessAwsDefaultRegion", "X-Harness-AWS-Default-Region"),
    ("HarnessS3Bucket", "X-Harness-S3-Bucket"),
)
HARNESS_OPERATIONS = (
    ("/agents", "get"),
    ("/agents/{name}/download-url", "get"),
    ("/analyze-benchmark/{benchmark_id}", "post"),
    ("/benchmarks/{benchmark_id}/tasks/{task_id}/artifacts", "get"),
    ("/check-results-exist", "get"),
    ("/fetch-benchmark", "get"),
    ("/fetch-benchmark-tasks", "post"),
    ("/fetch-run-outputs/{benchmark_id}", "get"),
    ("/retrieve-results", "get"),
    ("/retry-or-resume-benchmark/{benchmark_id}", "post"),
    ("/stop-benchmark/{benchmark_id}", "post"),
)


def build_openapi() -> dict[str, Any]:
    schema = deepcopy(app.openapi())
    schema["components"]["securitySchemes"] = {
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
    }
    schema["components"]["parameters"] = {
        component: {
            "name": header,
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
        }
        for component, header in HARNESS_HEADERS
    }
    for path, method in HARNESS_OPERATIONS:
        operation = schema["paths"][path][method]
        operation["parameters"] = [
            *operation.get("parameters", []),
            *({"$ref": f"#/components/parameters/{component}"} for component, _ in HARNESS_HEADERS),
        ]

    schema["security"] = BEARER_OR_API_KEY
    schema["paths"]["/health"]["get"]["security"] = []
    schema["paths"]["/init"]["post"]["security"] = API_KEY_ONLY
    schema["paths"]["/start-benchmark"]["post"]["security"] = API_KEY_ONLY
    return schema


if __name__ == "__main__":
    OPENAPI_PATH.write_text(json.dumps(build_openapi(), indent=2, sort_keys=True) + "\n")
