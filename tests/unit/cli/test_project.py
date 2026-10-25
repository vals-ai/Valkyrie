"""Project CLI behavior through the real SDK and a mock Tracker.

Run: uv run pytest tests/unit/cli/test_project.py
"""

import json
from datetime import datetime, timezone

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker

_GOOGLE_REPORT = {
    "handle": "google-report-xyz",
    "name": "Google Report XYZ",
    "spend_type": "customer",
    "subcategory": "report",
    "customer": "google",
    "extra_tags": {"engagement": "q4", "owner": "research"},
    "created_at": datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc).isoformat(),
    "created_by_email": "researcher@vals.ai",
}
_ROUTER_BENCH = {
    **_GOOGLE_REPORT,
    "handle": "router-bench",
    "name": "Router Bench",
    "spend_type": "development",
    "subcategory": "new_benchmark_generation",
    "customer": None,
    "extra_tags": {},
}


def _invoke(*arguments: str) -> Result:
    return CliRunner().invoke(cli, ["project", *arguments])


def test_create_sends_the_project_and_prints_its_spend_fields(tracker: MockTracker) -> None:
    tracker.reply("POST", "/projects", httpx.Response(200, json=_GOOGLE_REPORT))

    result = _invoke(
        "create",
        " google-report-xyz ",
        "--name",
        " Google Report XYZ ",
        "--spend-type",
        "customer",
        "--subcategory",
        "report",
        "--customer",
        " google ",
        "--tag",
        "engagement=q4",
        "--tag",
        "owner=research",
    )

    assert result.exit_code == 0, result.output
    assert [json.loads(request.content) for request in tracker.requests] == [
        {
            "handle": "google-report-xyz",
            "name": "Google Report XYZ",
            "spend_type": "customer",
            "subcategory": "report",
            "customer": "google",
            "extra_tags": {"engagement": "q4", "owner": "research"},
        }
    ]
    assert "✓ Project created: customer · google-report-xyz · report · google · engagement=q4 · owner=research" in (
        result.output
    )


_DEVELOPMENT = (
    "--name",
    "Router Bench",
    "--spend-type",
    "development",
    "--subcategory",
    "new_benchmark_generation",
)
_INTERNAL = ("--name", "Blog", "--spend-type", "internal", "--subcategory", "blog")
_CUSTOMER = ("--name", "Google Report XYZ", "--spend-type", "customer", "--subcategory", "report")


@pytest.mark.parametrize(
    "arguments",
    [
        ("new-benchmark", *_DEVELOPMENT),
        ("launch-runs", *_DEVELOPMENT[:-1], "new_benchmark_launch_runs"),
        ("benchmark-update", *_DEVELOPMENT[:-1], "post_launch_benchmark_update"),
        ("leaderboard", *_INTERNAL[:-1], "unpaid_leaderboard_run"),
        ("other-unpaid", *_INTERNAL[:-1], "other_unpaid_run"),
        ("intern-program", *_INTERNAL[:-1], "fellows_interns"),
        ("blog", *_INTERNAL),
        ("internal-misc", *_INTERNAL[:-1], "miscellaneous"),
        ("google-report", *_CUSTOMER, "--customer", "google"),
        ("google-paid", *_CUSTOMER[:-1], "paid_run", "--customer", "google"),
        ("google-eval", *_CUSTOMER[:-1], "eval_server", "--customer", "google"),
        ("google-platform", *_CUSTOMER[:-1], "platform", "--customer", "google"),
        ("a" * 64, *_DEVELOPMENT),
        ("x1-2-y3", *_DEVELOPMENT),
    ],
    ids=[
        "new-benchmark-generation",
        "new-benchmark-launch-runs",
        "post-launch-benchmark-update",
        "unpaid-leaderboard-run",
        "other-unpaid-run",
        "intern-program",
        "blog",
        "internal-miscellaneous",
        "report",
        "paid-run",
        "eval-server",
        "platform",
        "handle-64-chars",
        "handle-digits",
    ],
)
def test_create_accepts_each_spend_types_subcategories_and_handle_forms(
    tracker: MockTracker, arguments: tuple[str, ...]
) -> None:
    tracker.reply("POST", "/projects", httpx.Response(200, json=_ROUTER_BENCH))

    result = _invoke("create", *arguments)

    assert result.exit_code == 0, result.output
    (body,) = [json.loads(request.content) for request in tracker.requests]
    assert (body["handle"], body["subcategory"]) == (arguments[0], arguments[arguments.index("--subcategory") + 1])


_HANDLE_RULE = "must be lowercase letters, digits and hyphens, at most 64 characters, e.g. my-project"


@pytest.mark.parametrize(
    ("arguments", "messages"),
    [
        (
            ("router-bench", "--name", "Router Bench", "--subcategory", "new_benchmark_generation"),
            ("Missing option '--spend-type'",),
        ),
        (
            ("router-bench", "--spend-type", "development", "--subcategory", "new_benchmark_generation"),
            ("Missing option '--name'",),
        ),
        (
            ("router-bench", "--name", "Router Bench", "--spend-type", "development"),
            (
                "Missing option '--subcategory'",
                "Allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
            ),
        ),
        (("  ", *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("My Project", *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("my_project", *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("my--project", *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("my-project-", *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("a" * 65, *_INTERNAL), ("'HANDLE'", _HANDLE_RULE)),
        (("blog", "--name", "   ", "--spend-type", "internal", "--subcategory", "blog"), ("'--name'", "must name")),
        (
            (
                "router-bench",
                "--name",
                "Router Bench",
                "--spend-type",
                "research",
                "--subcategory",
                "new_benchmark_generation",
            ),
            ("'--spend-type'", "'research' is not one of 'development', 'internal', 'customer'"),
        ),
        (
            ("blog", "--name", "Blog", "--spend-type", "internal", "--subcategory", "report"),
            (
                "'--subcategory'",
                "'report' is not a subcategory of spend type 'internal'",
                "unpaid_leaderboard_run, other_unpaid_run, fellows_interns, blog, miscellaneous",
            ),
        ),
        (
            (
                "router-bench",
                "--name",
                "Router Bench",
                "--spend-type",
                "development",
                "--subcategory",
                "blogs",
            ),
            (
                "'--subcategory'",
                "Allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
            ),
        ),
        (
            (
                "router-bench",
                "--name",
                "Router Bench",
                "--spend-type",
                "development",
                "--subcategory",
                "miscellaneous",
            ),
            (
                "'--subcategory'",
                "Allowed: new_benchmark_generation, new_benchmark_launch_runs, post_launch_benchmark_update",
            ),
        ),
        (("google-report-xyz", *_CUSTOMER), ("Missing option '--customer'",)),
        (("google-report-xyz", *_CUSTOMER, "--customer", "   "), ("Missing option '--customer'",)),
        (("blog", *_INTERNAL, "--tag", "engagement"), ("'--tag'", "key=value")),
        (
            ("blog", *_INTERNAL, "--tag", "engagement=a", "--tag", "engagement=b"),
            ("'--tag'", "'engagement' is given twice"),
        ),
        (
            ("blog", *_INTERNAL, "--tag", "name=Blog"),
            ("'--tag'", "'name' is a core project field and cannot be an extra tag"),
        ),
        (
            ("blog", *_INTERNAL, "--tag", "handle=blog"),
            ("'--tag'", "'handle' is a core project field and cannot be an extra tag"),
        ),
        (
            ("blog", *_INTERNAL, "--tag", "customer=google"),
            ("'--tag'", "'customer' is a core project field and cannot be an extra tag"),
        ),
    ],
    ids=[
        "missing-spend-type",
        "missing-name",
        "missing-subcategory",
        "blank-handle",
        "handle-with-space-and-capitals",
        "handle-with-underscore",
        "handle-with-double-hyphen",
        "handle-with-trailing-hyphen",
        "handle-65-chars",
        "blank-name",
        "unknown-spend-type",
        "subcategory-of-another-spend-type",
        "retired-development-subcategory",
        "internal-only-miscellaneous",
        "customer-without-customer",
        "blank-customer",
        "tag-without-value",
        "tag-key-twice",
        "tag-key-shadows-name",
        "tag-key-shadows-handle",
        "tag-key-shadows-customer",
    ],
)
def test_create_errors_name_the_flag_before_any_request(
    tracker: MockTracker, arguments: tuple[str, ...], messages: tuple[str, ...]
) -> None:
    result = _invoke("create", *arguments)

    assert result.exit_code == 2
    for message in messages:
        assert message in result.output
    assert tracker.requests == []


def test_create_reports_a_taken_id(tracker: MockTracker) -> None:
    tracker.reply("POST", "/projects", httpx.Response(409, json={"detail": "Project 'router-bench' already exists"}))

    result = _invoke("create", "router-bench", *_DEVELOPMENT)

    assert result.exit_code == 1
    assert "Project 'router-bench' already exists" in result.output


def test_list_shows_handle_name_spend_type_subcategory_customer_and_tags(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_GOOGLE_REPORT, _ROUTER_BENCH]}))

    result = _invoke("list")

    assert result.exit_code == 0, result.output
    rows = [line.split() for line in result.output.splitlines()]
    assert ["Handle", "Name", "Spend", "type", "Subcategory", "Customer", "Tags"] in rows
    assert [
        "google-report-xyz",
        "Google",
        "Report",
        "XYZ",
        "customer",
        "report",
        "google",
        "engagement=q4,",
        "owner=research",
    ] in rows
    assert ["router-bench", "Router", "Bench", "development", "new_benchmark_generation"] in rows


def test_list_prints_escape_codes_in_handle_and_name_as_text(tracker: MockTracker) -> None:
    hostile = {**_ROUTER_BENCH, "handle": "evil\x1b[2K", "name": "Evil\x1b[1A"}
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [hostile]}))

    result = _invoke("list")

    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output
    assert "evil\\x1b[2K" in result.output
    assert "Evil\\x1b[1A" in result.output


def test_list_json_prints_the_tracker_response(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_ROUTER_BENCH]}))

    result = _invoke("list", "--format", "json")

    assert result.exit_code == 0, result.output
    assert [(project["handle"], project["name"]) for project in json.loads(result.output)["projects"]] == [
        ("router-bench", "Router Bench")
    ]


def test_update_preserves_unspecified_saved_fields(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_GOOGLE_REPORT]}))
    updated = {**_GOOGLE_REPORT, "name": "Google Report 2027"}
    tracker.reply("PUT", "/projects/google-report-xyz", httpx.Response(200, json=updated))

    result = _invoke("update", "google-report-xyz", "--name", "Google Report 2027")

    assert result.exit_code == 0, result.output
    assert [(request.method, request.url.path) for request in tracker.requests] == [
        ("GET", "/projects"),
        ("PUT", "/projects/google-report-xyz"),
    ]
    assert json.loads(tracker.requests[1].content) == {
        "handle": "google-report-xyz",
        "name": "Google Report 2027",
        "spend_type": "customer",
        "subcategory": "report",
        "customer": "google",
        "extra_tags": {"engagement": "q4", "owner": "research"},
    }


def test_update_tag_replaces_all_saved_tags(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_GOOGLE_REPORT]}))
    updated = {**_GOOGLE_REPORT, "extra_tags": {"team": "platform"}}
    tracker.reply("PUT", "/projects/google-report-xyz", httpx.Response(200, json=updated))

    result = _invoke("update", "google-report-xyz", "--tag", "team=platform")

    assert result.exit_code == 0, result.output
    assert json.loads(tracker.requests[1].content)["extra_tags"] == {"team": "platform"}


def test_update_explicitly_clears_customer_and_tags(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_GOOGLE_REPORT]}))
    updated = {
        **_GOOGLE_REPORT,
        "spend_type": "internal",
        "subcategory": "blog",
        "customer": None,
        "extra_tags": {},
    }
    tracker.reply("PUT", "/projects/google-report-xyz", httpx.Response(200, json=updated))

    result = _invoke(
        "update",
        "google-report-xyz",
        "--spend-type",
        "internal",
        "--subcategory",
        "blog",
        "--clear-customer",
        "--clear-tags",
    )

    assert result.exit_code == 0, result.output
    assert json.loads(tracker.requests[1].content) == {
        "handle": "google-report-xyz",
        "name": "Google Report XYZ",
        "spend_type": "internal",
        "subcategory": "blog",
        "customer": None,
        "extra_tags": {},
    }


def test_delete_requires_confirmation_and_returns_the_deleted_project(tracker: MockTracker) -> None:
    runner = CliRunner()

    cancelled = runner.invoke(cli, ["project", "delete", "router-bench"], input="n\n")

    assert cancelled.exit_code == 0, cancelled.output
    assert "Cancelled." in cancelled.output
    assert tracker.requests == []

    tracker.reply("DELETE", "/projects/router-bench", httpx.Response(200, json=_ROUTER_BENCH))

    deleted = runner.invoke(cli, ["project", "delete", "router-bench"], input="y\n")

    assert deleted.exit_code == 0, deleted.output
    assert [(request.method, request.url.path) for request in tracker.requests] == [
        ("DELETE", "/projects/router-bench")
    ]
    assert "✓ Project deleted: development · router-bench" in deleted.output


def test_update_no_op_reads_without_writing(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_ROUTER_BENCH]}))

    result = _invoke("update", "router-bench", "--name", "Router Bench")

    assert result.exit_code == 0, result.output
    assert result.output == "No changes to project 'router-bench'.\n"
    assert [(request.method, request.url.path) for request in tracker.requests] == [("GET", "/projects")]


def test_update_reports_a_missing_project_without_writing(tracker: MockTracker) -> None:
    tracker.reply("GET", "/projects", httpx.Response(200, json={"projects": [_ROUTER_BENCH]}))

    result = _invoke("update", "missing-project", "--name", "Missing Project")

    assert result.exit_code == 1
    assert "Project 'missing-project' was not found." in result.output
    assert [(request.method, request.url.path) for request in tracker.requests] == [("GET", "/projects")]


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("router-bench",), "Provide at least one field to update."),
        (
            ("router-bench", "--customer", "google", "--clear-customer"),
            "Use either --customer or --clear-customer, not both.",
        ),
        (
            ("router-bench", "--tag", "owner=research", "--clear-tags"),
            "Use either --tag or --clear-tags, not both.",
        ),
        (
            ("router-bench", "--customer", "   "),
            "must not be blank; use --clear-customer to remove it",
        ),
    ],
)
def test_update_rejects_ambiguous_or_empty_changes_before_any_request(
    tracker: MockTracker, arguments: tuple[str, ...], message: str
) -> None:
    result = _invoke("update", *arguments)

    assert result.exit_code == 2
    assert message in result.output
    assert tracker.requests == []


def test_delete_reports_project_history_conflict(tracker: MockTracker) -> None:
    tracker.reply(
        "DELETE",
        "/projects/router-bench",
        httpx.Response(409, json={"detail": "Project 'router-bench' has run history and cannot be deleted"}),
    )

    result = _invoke("delete", "router-bench", "--yes")

    assert result.exit_code == 1
    assert "Project 'router-bench' has run history and cannot be deleted" in result.output
