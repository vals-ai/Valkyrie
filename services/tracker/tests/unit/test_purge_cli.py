"""CLI cannot mutate by default or expose provider error details."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

import tracker.run_purge.cli as cli
from tracker.aws.runtime import AWSResources
from tracker.lifecycle import OperationIdentity, RunScope
from tracker.lifecycle_evidence import HostContractObservation
from tracker.run_purge.cli import main, parser
from tracker.run_purge.contracts import ProviderLocator, PurgePlan, PurgeRun


def test_cli_defaults_to_read_only_plan() -> None:
    options = parser().parse_args(
        ["--database-url-env", "PRIVATE_DB", "--expected-database-target", "tracker", "--plan", "private.json"]
    )
    assert options.action == "plan" and not options.apply


def test_cli_refuses_mutation_without_apply(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:

    create_engine = MagicMock()
    monkeypatch.setattr(cli, "create_engine", create_engine)
    assert (
        main(
            [
                "prepare",
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                "private.json",
            ]
        )
        == 2
    )
    create_engine.assert_not_called()
    assert "--apply" in capsys.readouterr().err


def test_cli_masks_provider_and_database_payload(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    monkeypatch.setenv("PRIVATE_DB", "postgresql://private")
    monkeypatch.setattr(cli, "create_engine", MagicMock(side_effect=RuntimeError("secret password customer payload")))
    assert (
        main(
            [
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                str(tmp_path / "private.json"),
            ]
        )
        == 2
    )
    assert "secret" not in capsys.readouterr().err


def test_cli_never_overwrites_immutable_plan_with_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text("immutable plan")
    monkeypatch.setenv("PRIVATE_DB", "postgresql://private")
    engine = MagicMock()
    monkeypatch.setattr(cli, "create_engine", engine)
    assert (
        main(
            [
                "purge",
                "--apply",
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                str(plan),
                "--report",
                str(plan),
            ]
        )
        == 2
    )
    assert plan.read_text() == "immutable plan"
    engine.assert_not_called()


def test_cli_refuses_abandonment_without_apply(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:

    create_engine = MagicMock()
    monkeypatch.setattr(cli, "create_engine", create_engine)
    assert (
        main(
            [
                "abandon",
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                "private.json",
                "--run",
                "0d1ab1cd-0000-4000-8000-000000000001",
            ]
        )
        == 2
    )
    create_engine.assert_not_called()
    assert "--apply" in capsys.readouterr().err


def test_cli_refuses_an_empty_fence_receipt_file_before_any_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:

    run_id = uuid4()
    scope = RunScope(
        run_id=run_id,
        original_resources=AWSResources(
            region="us-west-2", s3_bucket="owner-bucket", log_group="runs", log_retention_days=7
        ),
    )
    identity = OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=uuid4(),
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-west-2",
        environment="test",
        database_target="tracker",
        run_ids=(run_id,),
    )
    plan = tmp_path / "plan.json"
    plan.write_text(
        PurgePlan(
            identity=identity,
            runs=(PurgeRun(scope=scope, provider=ProviderLocator(kind="daytona", secret_name="provider")),),
        ).model_dump_json()
    )
    observed_at = datetime.now(UTC)
    host_contract = tmp_path / "host.json"
    host_contract.write_text(
        HostContractObservation(
            contract="stable-host-lifecycle-v1",
            deployment_sha256="a" * 64,
            host_inventory=("host-1",),
            observed_at=observed_at,
            acknowledgement_required_since=observed_at,
            verifier="operator",
        ).model_dump_json()
    )
    receipts = tmp_path / "receipts.json"
    receipts.write_text("[]")
    boundary = MagicMock()
    monkeypatch.setenv("PRIVATE_DB", "postgresql://private")
    monkeypatch.setattr(cli, "create_engine", MagicMock())
    monkeypatch.setattr(cli, "AWSProviderBoundary", boundary)
    assert (
        main(
            [
                "purge",
                "--apply",
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                str(plan),
                "--report",
                str(tmp_path / "report.json"),
                "--host-contract",
                str(host_contract),
                "--fence-receipts",
                str(receipts),
            ]
        )
        == 2
    )
    boundary.assert_not_called()


def test_cli_surfaces_the_sanitized_lifecycle_conflict(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:

    monkeypatch.setenv("PRIVATE_DB", "postgresql://private")
    monkeypatch.setattr(cli, "create_engine", MagicMock())
    assert (
        main(
            [
                "--database-url-env",
                "PRIVATE_DB",
                "--expected-database-target",
                "tracker",
                "--plan",
                str(tmp_path / "plan.json"),
            ]
        )
        == 2
    )
    assert "Read-only plan requires an identity file" in capsys.readouterr().err
