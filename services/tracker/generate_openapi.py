"""Generate the committed Tracker API contract."""

import json
from pathlib import Path

from main import app

OPENAPI_PATH = Path(__file__).with_name("openapi.json")


if __name__ == "__main__":
    OPENAPI_PATH.write_text(json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n")
