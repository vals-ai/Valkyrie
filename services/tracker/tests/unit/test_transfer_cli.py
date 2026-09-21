"""Real CLI rejects mutation before opening databases or AWS sessions."""

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts.transfer_run_history import clause_code, main
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


@pytest.mark.parametrize(
    "fault", ["identity", "database", "accounts", "run_set", "edits", "region", "bucket", "releases"]
)
def test_cli_rejects_ambiguous_transfer_authority_before_opening_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures/tracker-transfer-plan-v1.json"
    payload = json.loads(fixture.read_bytes())
    plan = payload["plan"]
    run = plan["runs"][0]
    if fault == "identity":
        plan["destination_identity"]["github_owner_id"] += 1
    elif fault == "database":
        plan["destination_identity"]["database_target"] = plan["source_identity"]["database_target"]
    elif fault == "accounts":
        for side in ("source_identity", "destination_identity"):
            plan[side]["destination_aws_account_id"] = plan[side]["source_aws_account_id"]
    elif fault == "run_set":
        plan["runs"] *= 2
    elif fault == "edits":
        run["reference_edits"] = [
            {"pointer": "/arguments/dataset", "original_sha256": "a" * 64, "replacement": "private-destination"}
        ] * 2
    elif fault == "region":
        plan["destination_identity"]["region"] = "eu-west-1"
    elif fault == "bucket":
        run["destination"]["original_resources"]["s3_bucket"] = run["source"]["original_resources"]["s3_bucket"]
    else:
        plan["releases"] = [
            {
                "source_id": "a",
                "destination_id": "b",
                "source_artifact_uri": "s3://source/a",
                "destination_artifact_uri": "s3://destination/b",
                "artifact_digest": "c" * 64,
                "protocol_version": "1",
            }
        ] * 2
    engine = Mock()
    monkeypatch.setattr("tracker.run_transfer.cli.create_engine", engine)
    report = tmp_path / "report.json"
    report.write_text("previous verified report")

    with pytest.raises(ValueError):
        execute(
            json.dumps(payload).encode(),
            tmp_path / "request.json",
            report,
            "postgresql://localhost/source",
            "postgresql://localhost/destination",
            plan["source_identity"]["database_target"],
            plan["destination_identity"]["database_target"],
            "source",
            "destination",
            tmp_path / "journal",
        )

    engine.assert_not_called()
    assert report.read_text() == "previous verified report"
    assert not (tmp_path / "journal").exists()


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "Historical log completeness clause scan_quiet_interval failed; transfer remains pending",
            "scan_quiet_interval",
        ),
        (
            "Historical log completeness clause persisted_decision failed; transfer remains pending",
            "persisted_decision",
        ),
        ("clause private-customer-payload failed", "unnamed"),
        ("Exact planned source row content changed", "unnamed"),
    ],
)
def test_the_refusal_names_a_closed_set_clause_and_nothing_else(message: str, expected: str) -> None:
    assert clause_code(LifecycleConflict(message)) == expected


def test_a_provider_failure_reports_its_class_and_no_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    request = tmp_path / "request.json"
    request.write_text(json.dumps({"action": "import"}))
    monkeypatch.setenv("SOURCE_TEST_DB", "postgresql://user:private-password@localhost:1/source")
    monkeypatch.setenv("DESTINATION_TEST_DB", "postgresql://user:private-password@localhost:1/destination")
    monkeypatch.setenv("SOURCE_PROFILE", "source")
    monkeypatch.setenv("DESTINATION_PROFILE", "destination")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "transfer_run_history.py",
            "--request",
            str(request),
            "--report",
            str(tmp_path / "report.json"),
            "--source-database-url-env",
            "SOURCE_TEST_DB",
            "--destination-database-url-env",
            "DESTINATION_TEST_DB",
            "--expected-source-database-target",
            "postgresql:localhost:1/source",
            "--expected-destination-database-target",
            "postgresql:localhost:1/destination",
            "--source-aws-profile-env",
            "SOURCE_PROFILE",
            "--destination-aws-profile-env",
            "DESTINATION_PROFILE",
            "--journal-directory",
            str(tmp_path / "journal"),
            "--apply",
        ],
    )

    assert main() == 2

    output = capsys.readouterr()
    assert output.err == "Transfer remains incomplete (ValidationError; clause unnamed)\n"
    assert "private-password" not in output.err
