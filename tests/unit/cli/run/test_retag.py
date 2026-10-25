"""Tests for putting a run in another project.

Run: uv run pytest tests/unit/cli/run/test_retag.py

Covers the project handle sent to Tracker and the checks made before any request.
"""

import json
from datetime import datetime, timezone
from uuid import UUID

import httpx
from click.testing import CliRunner

from valkyrie.cli.run.retag import retag

from tests.unit.cli.conftest import MockTracker

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_INTERNAL_BLOG = {
    "handle": "internal-blog",
    "name": "Internal Blog",
    "spend_type": "internal",
    "subcategory": "blog",
    "customer": None,
    "extra_tags": {},
    "created_at": datetime(2026, 10, 21, 18, 0, tzinfo=timezone.utc).isoformat(),
    "created_by_email": "researcher@vals.ai",
}


def test_retag_sends_the_project_handle_and_prints_it(cli_runner: CliRunner, tracker: MockTracker) -> None:
    tracker.reply("POST", f"/benchmarks/{_RUN_ID}/project", httpx.Response(200, json=_INTERNAL_BLOG))

    result = cli_runner.invoke(retag, [str(_RUN_ID), "--project", " internal-blog "])

    assert result.exit_code == 0, result.output
    assert [json.loads(request.content) for request in tracker.requests] == [{"project": "internal-blog"}]
    assert result.output == f"✓ Run {_RUN_ID} is now in project internal-blog.\n"


def test_retag_without_project_names_the_flag_and_an_example(cli_runner: CliRunner, tracker: MockTracker) -> None:
    result = cli_runner.invoke(retag, [str(_RUN_ID)])

    assert result.exit_code == 2
    assert "Missing option '--project'. Name the project this run is for, e.g. router-bench." in result.output
    assert tracker.requests == []


def test_retag_into_an_unknown_project_shows_the_existing_ids_escaped(
    cli_runner: CliRunner, tracker: MockTracker
) -> None:
    detail = "Project 'blog' does not exist. Existing projects: internal-blog, evil\x1b[2K."
    tracker.reply("POST", f"/benchmarks/{_RUN_ID}/project", httpx.Response(422, json={"detail": detail}))

    result = cli_runner.invoke(retag, [str(_RUN_ID), "--project", "blog"])

    assert result.exit_code == 1
    assert "\x1b" not in result.output
    assert "Existing projects: internal-blog, evil\\x1b[2K." in result.output


def test_retag_escapes_terminal_control_characters_in_the_saved_project(
    cli_runner: CliRunner, tracker: MockTracker
) -> None:
    saved = {**_INTERNAL_BLOG, "handle": "Blog\x1b[2K\x1b[1A"}
    tracker.reply("POST", f"/benchmarks/{_RUN_ID}/project", httpx.Response(200, json=saved))

    result = cli_runner.invoke(retag, [str(_RUN_ID), "--project", "Blog"])

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert "Blog\\x1b[2K\\x1b[1A" in result.output
