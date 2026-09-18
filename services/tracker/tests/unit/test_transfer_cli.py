"""Real CLI rejects mutation before opening databases or AWS sessions."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.cli import execute
from tracker.run_transfer.contracts import TransferRequest

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


@pytest.mark.parametrize("failure", ["targets", "backend", "report-input"])
def test_in_process_cli_rejects_untrusted_database_or_report_target_before_connect(
    tmp_path: Path, failure: str
) -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures/tracker-transfer-plan-v1.json"
    request = TransferRequest.model_validate_json(fixture.read_bytes())
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    request_path.write_bytes(fixture.read_bytes())
    source_target = request.plan.source_identity.database_target
    source_url = "postgresql://localhost:1/source"
    if failure == "targets":
        source_target = "wrong"
    elif failure == "backend":
        source_url = "sqlite://"
    else:
        report_path = request_path
    with pytest.raises(LifecycleConflict):
        execute(
            request_path.read_bytes(),
            request_path,
            report_path,
            source_url,
            "postgresql://localhost:1/destination",
            source_target,
            request.plan.destination_identity.database_target,
            "unused-source",
            "unused-destination",
            tmp_path / "journal",
        )
    assert request_path.read_bytes() == fixture.read_bytes()
