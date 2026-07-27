"""Checks for the committed Tracker API contract."""

import json
from pathlib import Path

from generate_openapi import API_KEY_ONLY, BEARER_OR_API_KEY, build_openapi


def test_openapi_snapshot_matches_generator() -> None:
    snapshot_path = Path(__file__).parents[2] / "openapi.json"

    assert json.loads(snapshot_path.read_text()) == build_openapi()


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
    assert schema["security"] == BEARER_OR_API_KEY
    assert schema["paths"]["/health"]["get"]["security"] == []
    assert schema["paths"]["/init"]["post"]["security"] == API_KEY_ONLY
    assert schema["paths"]["/start-benchmark"]["post"]["security"] == API_KEY_ONLY
