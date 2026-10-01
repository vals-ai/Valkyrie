"""Generate the committed Tracker API contract."""

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from main import app

OPENAPI_PATH = Path(__file__).with_name("openapi.json")
API_KEY_ONLY: list[dict[str, list[str]]] = [{"ApiKeyAuth": []}]


def build_openapi() -> dict[str, Any]:
    schema = deepcopy(app.openapi())
    schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "x-api-key",
        },
    }

    schema["security"] = API_KEY_ONLY
    schema["paths"]["/health"]["get"]["security"] = []
    schema["paths"]["/init"]["post"]["security"] = API_KEY_ONLY
    schema["paths"]["/start-benchmark"]["post"]["security"] = API_KEY_ONLY
    schema["paths"]["/start-benchmark-with-storage"]["post"]["security"] = API_KEY_ONLY
    return schema


if __name__ == "__main__":
    OPENAPI_PATH.write_text(json.dumps(build_openapi(), indent=2, sort_keys=True) + "\n")
