"""Real CLI rejects mutation before opening databases or AWS sessions."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "transfer_run_history.py"


@pytest.mark.parametrize("action", ["prepare", "import", "cleanup", "finalize"])
def test_transfer_cli_requires_apply_before_access(tmp_path: Path, action: str):
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"action": action}))
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--request",
            str(request),
            "--report",
            str(tmp_path / "report.json"),
            "--source-database-url-env",
            "SOURCE_TEST_DB",
            "--destination-database-url-env",
            "DESTINATION_TEST_DB",
            "--expected-source-database-target",
            "source",
            "--expected-destination-database-target",
            "destination",
            "--source-aws-profile-env",
            "SOURCE_PROFILE",
            "--destination-aws-profile-env",
            "DESTINATION_PROFILE",
            "--journal-directory",
            str(tmp_path / "journal"),
        ],
        env={**os.environ, "SOURCE_TEST_DB": "postgresql://user:private-password@localhost:1/source"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "--apply" in result.stderr
    assert "private-password" not in result.stderr
    assert not (tmp_path / "journal").exists()
    assert not (tmp_path / "report.json").exists()
