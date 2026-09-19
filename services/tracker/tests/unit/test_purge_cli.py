"""CLI cannot mutate by default or expose provider error details."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import tracker.run_purge.cli as cli
from tracker.run_purge.cli import main, parser


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
