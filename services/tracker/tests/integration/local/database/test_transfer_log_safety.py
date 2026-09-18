"""Production completeness refusal preserves database state and private command reports."""

import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from sqlmodel import Session

from scripts.transfer_run_history import main
from tests.integration.local.database.test_run_transfer import pair as pair
from tests.integration.local.database.test_transfer_inspection import execute, imported_pair, snapshot
from tests.unit.aws.test_log_history_archive import FakeLogs, FakeS3, FakeSession
from tracker.run_transfer.providers import TransferAWSBoundary


@pytest.mark.parametrize("action", ["import", "inspect", "cleanup", "finalize", "retired_finalize"])
def test_command_refuses_unproved_history_without_removing_rows_releasing_holds_or_replacing_report(
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
