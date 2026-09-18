"""The installed operator defaults to no mutations and rejects unsafe DB targets."""

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "relocate_run_storage.py"


@pytest.mark.parametrize("action", ["prepare", "relocate", "release"])
def test_cli_requires_apply_before_database_or_provider_access(tmp_path: Path, action: str) -> None:
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
            "--database-url-env",
            "RELOCATION_PRIVATE_DATABASE",
            "--expected-database-target",
            "postgresql:localhost:1/unused",
        ],
        env={**os.environ, "RELOCATION_PRIVATE_DATABASE": "postgresql://user:do-not-print@localhost:1/unused"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "--apply" in result.stderr
    assert "do-not-print" not in result.stderr
    assert not (tmp_path / "report.json").exists()


def test_cli_database_target_mismatch_cannot_reach_provider(tmp_path: Path) -> None:
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "action": "inventory",
                "nonce": str(uuid4()),
                "github_owner_id": 42,
                "org_id": str(uuid4()),
                "source_aws_account_id": "123456789012",
                "destination_aws_account_id": "123456789012",
                "region": "us-east-1",
                "environment": "dev",
                "database_target": "postgresql:localhost:1/other",
                "run_ids": [str(uuid4())],
            }
        )
    )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--request",
            str(request),
            "--report",
            str(tmp_path / "report.json"),
            "--database-url-env",
            "RELOCATION_PRIVATE_DATABASE",
            "--expected-database-target",
            "postgresql:localhost:1/unused",
        ],
        env={**os.environ, "RELOCATION_PRIVATE_DATABASE": "postgresql://user:do-not-print@localhost:1/unused"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "do-not-print" not in result.stderr
    assert not (tmp_path / "report.json").exists()
