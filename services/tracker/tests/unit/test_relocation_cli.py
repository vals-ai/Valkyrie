"""The installed operator defaults to no mutations and rejects unsafe DB targets."""

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from tests.unit.test_relocation_providers import setup
from tracker.lifecycle import LifecycleConflict
from tracker.run_relocation.cli import execute, write_response
from tracker.storage_migration_exchange import TrackerResponse

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


@pytest.mark.parametrize("failure", ["target", "request-overwrite", "evidence-overwrite", "backend"])
def test_exchange_boundary_refuses_unsafe_targets_before_connecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:

    _, _, payload = setup()
    request_path = tmp_path / "request.json"
    report_path = tmp_path / "report.json"
    target = payload["database_target"]
    if failure == "target":
        target = "postgresql:localhost:1/other"
    elif failure == "request-overwrite":
        report_path = request_path
    elif failure == "evidence-overwrite":
        payload["external_evidence_files"] = [str(report_path)]
    monkeypatch.setenv("DATABASE_URL", "sqlite://")
    with pytest.raises(LifecycleConflict):
        execute(json.dumps(payload).encode(), request_path, report_path, target)
    assert not report_path.exists()


def test_atomic_report_failure_preserves_old_file_and_removes_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:

    report = tmp_path / "report.json"
    report.write_text("previous verified receipt")

    def fail_replace(self: Path, target: Path) -> Path:
        raise OSError("injected filesystem failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError):
        write_response(report, TrackerResponse(nonce=uuid4(), action="inventory", runs=()))
    assert report.read_text() == "previous verified receipt"
    assert list(tmp_path.iterdir()) == [report]
