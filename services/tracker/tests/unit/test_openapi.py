"""Checks for the committed Tracker API contract."""

import json
from pathlib import Path

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
    }
    assert schema["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]
    assert schema["paths"]["/health"]["get"]["security"] == []
    assert schema["paths"]["/init"]["post"]["security"] == [{"ApiKeyAuth": []}]
    assert schema["paths"]["/start-benchmark"]["post"]["security"] == [{"ApiKeyAuth": []}]
