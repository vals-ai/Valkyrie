"""The quiet-interval policy governs the real command; a failed clause preserves everything."""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlmodel import Session

from scripts.transfer_run_history import main
from tests.integration.local.database.test_run_transfer import pair as pair
from tests.integration.local.database.test_run_transfer import seed_rows
from tests.integration.local.database.test_transfer_inspection import execute, imported_pair, snapshot
from tests.transfer_support import transfer_request
from tests.unit.aws.test_log_history_archive import FakeLogs, FakeS3, FakeSession
from tracker.database.models import RunLifecycle
from tracker.run_transfer import TransferOperator
from tracker.run_transfer.contracts import TransferCheckpoint, TransferResponse
from tracker.run_transfer.providers import TransferAWSBoundary


class RunLogs(FakeLogs):
    """The seeded run has its own log group name; the retained events stay old."""

    def __init__(self, group: str) -> None:
        super().__init__()
        self.group = group

    def describe_log_groups(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("groups", request))
        self.scan += 1
        return {
            "logGroups": []
            if self.absent
            else [
                {
                    "logGroupName": self.group,
                    "arn": f"arn:aws:logs:us-east-1:111111111111:log-group:{self.group}:*",
                }
            ]
        }


class OwnerStorage(FakeS3):
    def __init__(self, org_id: UUID) -> None:
        super().__init__()
        self.org_id = org_id

    def get_bucket_tagging(self, **request: Any) -> dict[str, Any]:
        tags = super().get_bucket_tagging(**request)["TagSet"]
        return {
            "TagSet": [
                {**tag, "Value": str(self.org_id)} if tag["Key"] == "valsmith:valkyrie-org-id" else tag for tag in tags
            ]
        }


@pytest.mark.parametrize("action", ["import", "inspect", "cleanup", "finalize", "retired_finalize"])
def test_command_refuses_a_recent_hold_without_removing_rows_releasing_holds_or_replacing_report(
    pair: tuple[Session, Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    action: str,
) -> None:
    operator, _, payload = imported_pair(pair, tmp_path)
    if action == "retired_finalize":
        execute(operator, payload, "cleanup")
        action = "finalize"
    payload["action"] = action
    before = snapshot(pair)
    for session in pair:
        session.rollback()

    logs, storage = FakeLogs(), FakeS3()
    deleted: list[dict[str, Any]] = []

    def delete_log_group(**arguments: Any) -> None:
        deleted.append(arguments)

    monkeypatch.setattr(logs, "delete_log_group", delete_log_group, raising=False)
    boundary = TransferAWSBoundary(
        None,
        None,
        tmp_path / "journal",
        source_session=FakeSession("111111111111", logs),
        destination_session=FakeSession("222222222222", storage),
    )
    # Account/provider doubles leave the production archive authority unchanged.
    monkeypatch.setattr(boundary, "validate", AsyncMock())
    monkeypatch.setattr(boundary, "drain", AsyncMock())
    monkeypatch.setattr("tracker.run_transfer.cli.TransferAWSBoundary", Mock(return_value=boundary))
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    request_path.write_text(json.dumps(payload))
    report_path.write_text("prior report with an old nonce")
    for side, session in zip(("SOURCE", "DESTINATION"), pair, strict=True):
        monkeypatch.setenv(
            f"SAFETY_{side}_DATABASE", session.get_bind().engine.url.render_as_string(hide_password=False)
        )
        monkeypatch.setenv(f"SAFETY_{side}_PROFILE", f"private-{side}-profile")
    arguments = [
        "transfer_run_history.py",
        "--request",
        str(request_path),
        "--report",
        str(report_path),
        "--source-database-url-env",
        "SAFETY_SOURCE_DATABASE",
        "--destination-database-url-env",
        "SAFETY_DESTINATION_DATABASE",
        "--expected-source-database-target",
        payload["plan"]["source_identity"]["database_target"],
        "--expected-destination-database-target",
        payload["plan"]["destination_identity"]["database_target"],
        "--source-aws-profile-env",
        "SAFETY_SOURCE_PROFILE",
        "--destination-aws-profile-env",
        "SAFETY_DESTINATION_PROFILE",
        "--journal-directory",
        str(tmp_path / "journal"),
    ]
    if action != "inspect":
        arguments.append("--apply")
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setenv("DATABASE_URL", "unchanged-by-test-teardown")

    assert main() == 2

    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "Transfer remains incomplete (LifecycleConflict)\n"
    assert report_path.read_text() == "prior report with an old nonce"
    assert snapshot(pair) == before
    assert not deleted
    assert not storage.objects
    assert not (tmp_path / "journal").exists()


def test_command_completes_a_quiet_legacy_run_and_publishes_its_archive(
    pair: tuple[Session, Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, destination = pair
    org, run, _ = seed_rows(source, destination)
    payload = transfer_request(source, destination, org, run)
    logs = RunLogs(f"logs/{run.id}")
    storage = OwnerStorage(org.id)
    boundary = TransferAWSBoundary(
        Mock(),
        Mock(),
        tmp_path / "journal",
        source_session=FakeSession("111111111111", logs),
        destination_session=FakeSession("222222222222", storage),
    )
    # Provider drain and the paired object verifier have their own tests; the policy is under test here.
    monkeypatch.setattr(boundary, "validate", AsyncMock())
    monkeypatch.setattr(boundary, "drain", AsyncMock())
    monkeypatch.setattr("tracker.run_transfer.providers.RelocationAWSBoundary.verify_objects", AsyncMock())
    operator = TransferOperator(source, destination, boundary)
    planned = execute(operator, payload, "plan")
    payload["plan"]["runs"][0]["source_rows_sha256"] = planned.runs[0].source_rows_sha256
    execute(operator, payload, "prepare")
    quiet = datetime.now(UTC) - timedelta(days=2)
    source.connection().execute(
        text("UPDATE runlifecycle SET acquired_at=:quiet WHERE run_id=:id"), {"quiet": quiet, "id": run.id}
    )
    source.commit()

    monkeypatch.setattr("tracker.run_transfer.cli.TransferAWSBoundary", Mock(return_value=boundary))
    payload["action"] = "import"
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    request_path.write_text(json.dumps(payload))
    for side, session in zip(("SOURCE", "DESTINATION"), pair, strict=True):
        monkeypatch.setenv(
            f"QUIET_{side}_DATABASE", session.get_bind().engine.url.render_as_string(hide_password=False)
        )
        monkeypatch.setenv(f"QUIET_{side}_PROFILE", f"private-{side}-profile")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "transfer_run_history.py",
            "--request",
            str(request_path),
            "--report",
            str(report_path),
            "--source-database-url-env",
            "QUIET_SOURCE_DATABASE",
            "--destination-database-url-env",
            "QUIET_DESTINATION_DATABASE",
            "--expected-source-database-target",
            payload["plan"]["source_identity"]["database_target"],
            "--expected-destination-database-target",
            payload["plan"]["destination_identity"]["database_target"],
            "--source-aws-profile-env",
            "QUIET_SOURCE_PROFILE",
            "--destination-aws-profile-env",
            "QUIET_DESTINATION_PROFILE",
            "--journal-directory",
            str(tmp_path / "journal"),
            "--apply",
        ],
    )
    monkeypatch.setenv("DATABASE_URL", "unchanged-by-test-teardown")

    assert main() == 0

    assert capsys.readouterr().err == ""
    response = TransferResponse.model_validate_json(report_path.read_bytes())
    observed = response.runs[0]
    assert observed.destination_phase == "transferred"
    assert observed.archive is not None and observed.archive.event_count == 2
    for session in pair:
        session.rollback()
    record = destination.get(RunLifecycle, run.id)
    assert record is not None and record.checkpoint_json is not None
    assert TransferCheckpoint.model_validate_json(record.checkpoint_json).log_completeness_sha256 is not None
