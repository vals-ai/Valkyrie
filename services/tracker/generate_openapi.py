"""Generate the committed Tracker API contract."""

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from main import app

OPENAPI_PATH = Path(__file__).with_name("openapi.json")
BEARER_OR_API_KEY: list[dict[str, list[str]]] = [{"BearerAuth": []}, {"ApiKeyAuth": []}]
BEARER_ONLY: list[dict[str, list[str]]] = [{"BearerAuth": []}]


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

    schema["security"] = BEARER_OR_API_KEY
    schema["paths"]["/health"]["get"]["security"] = []
    schema["paths"]["/auth/cli"]["get"]["security"] = []
    for path, methods in schema["paths"].items():
        if path.startswith(("/orgs", "/access-keys")):
            for operation in methods.values():
                operation["security"] = BEARER_ONLY
    return schema


if __name__ == "__main__":
    OPENAPI_PATH.write_text(json.dumps(build_openapi(), indent=2, sort_keys=True) + "\n")
