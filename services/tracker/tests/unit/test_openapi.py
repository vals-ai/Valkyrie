"""Checks for the committed Tracker API contract."""

import json
from pathlib import Path

from main import app


def test_openapi_snapshot_matches_app() -> None:
    snapshot_path = Path(__file__).parents[2] / "openapi.json"

    assert json.loads(snapshot_path.read_text()) == app.openapi()
