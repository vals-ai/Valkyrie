"""Reject unbounded database pool configuration before connecting.

Run: uv run pytest tests/unit/test_database_config.py
"""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    ("setting", "value"),
    [("DATABASE_POOL_SIZE", "0"), ("DATABASE_POOL_SIZE", "-1"), ("DATABASE_MAX_OVERFLOW", "-1")],
)
def test_rejects_unbounded_pool_settings(setting: str, value: str) -> None:
    environment = {**os.environ, "DATABASE_POOL_SIZE": "5", "DATABASE_MAX_OVERFLOW": "2", setting: value}

    result = subprocess.run(
        [sys.executable, "-c", "import tracker.database.session"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert f"ValueError: {setting} must be" in result.stderr
