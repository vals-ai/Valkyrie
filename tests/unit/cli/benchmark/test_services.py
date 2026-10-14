"""Benchmark service list against fake and mock Trackers.

Run: uv run pytest tests/unit/cli/benchmark/test_services.py
"""

import json
from importlib import import_module

import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk import BenchmarkServiceEntry, BenchmarkServiceHealth

from valkyrie.cli.benchmark.services import paginate_services
from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.fake_tracker import FakeTracker

services_module = import_module("valkyrie.cli.benchmark.services")


def test_list_merges_hosted_services_with_the_saved_urls(
    tracker: MockTracker, fake_tracker: FakeTracker, cli_runner: CliRunner
) -> None:
    """A saved URL replaces the hosted service of the same name, others are added, and each row shows its latency."""
    fake_tracker.benchmark_services = {
        "swebench": {"url": "http://localhost:9000"},
        "custombench": {"url": "http://custombench"},
    }
    catalog = [
        {"name": "swebench", "url": "https://swebench.benchmarks.vals.ai"},
        {"name": "fab", "url": "https://fab.benchmarks.vals.ai"},
    ]
    tracker.reply("GET", "/benchmark-services", httpx.Response(200, json={"services": catalog}))

    def check(request: httpx.Request) -> httpx.Response:
        services = json.loads(request.content)["services"]
        health = [
            {**entry, "healthy": entry["name"] == "fab", "latency_ms": 23 if entry["name"] == "fab" else None}
            for entry in services
        ]
        return httpx.Response(200, json={"services": health})

    tracker.reply("POST", "/benchmark-services", check)

    result = cli_runner.invoke(cli, ["benchmark", "service", "list"])

    assert result.exit_code == 0, result.output
    (checked,) = tracker.requested("POST", "/benchmark-services")
    assert [(entry["name"], entry["url"]) for entry in json.loads(checked.content)["services"]] == [
        ("swebench", "http://localhost:9000"),
        ("fab", "https://fab.benchmarks.vals.ai"),
        ("custombench", "http://custombench"),
    ]
    rows = {line.split()[0]: line.split()[2:] for line in result.output.splitlines() if line.startswith(("sw", "fab"))}
    assert rows["swebench"] == ["localhost:9000", "-"]
    assert rows["fab"] == ["benchmarks.vals.ai", "23", "ms"]


def test_list_without_any_services(tracker: MockTracker, fake_tracker: FakeTracker, cli_runner: CliRunner) -> None:
    tracker.reply("GET", "/benchmark-services", httpx.Response(200, json={"services": []}))

    result = cli_runner.invoke(cli, ["benchmark", "service", "list"])

    assert result.exit_code == 0, result.output
    assert result.output == "No benchmark services configured.\n"


def test_paginate_services_renders_latency_as_response_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Service list should show response latency when available and dash when not.

    Test cases:
    - Responding services render measured latency.
    - Non-responding services render a dash without healthy/error columns.
    """
    captured_rows: list[dict[str, str]] = []
    captured_headers: list[str] = []

    def mock_format_table(
        rows: list[dict[str, str]],
        headers: list[str],
        _current_page: int,
        _total_pages: int,
        _total_count: int,
        _item_name: str,
    ) -> None:
        captured_rows.extend(rows)
        captured_headers.extend(headers)

    monkeypatch.setattr(services_module, "format_table", mock_format_table)

    paginate_services(
        [
            BenchmarkServiceHealth(
                name="swebench",
                url="https://swebench.benchmarks.vals.ai",
                healthy=True,
                latency_ms=23,
            ),
            BenchmarkServiceHealth(
                name="vcb",
                url="http://localhost:9000",
                healthy=False,
                latency_ms=None,
                error="[Errno -2] Name or service not known",
            ),
        ]
    )

    assert captured_headers == ["Benchmark", "Service URL", "Source", "Latency"]
    assert [row["Source"] for row in captured_rows] == ["benchmarks.vals.ai", "localhost:9000"]
    assert [row["Latency"] for row in captured_rows] == ["23 ms", "-"]


def test_paginate_services_health_checks_visible_pages_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service pagination should defer health checks until a page is visible.

    Test cases:
    - The first page is health-checked before rendering.
    - Moving forward checks only the next page, and moving back reuses cached results.
    - Each page render clears the previous table output.
    """
    service_entries = [
        BenchmarkServiceEntry(name="one", url="https://one.example"),
        BenchmarkServiceEntry(name="two", url="https://two.example"),
        BenchmarkServiceEntry(name="three", url="https://three.example"),
    ]
    checked_pages: list[list[str]] = []
    rendered_pages: list[list[str]] = []
    keys = iter(["l", "h", "q"])

    def check_services(entries: list[BenchmarkServiceEntry]) -> list[BenchmarkServiceHealth]:
        checked_pages.append([entry.name for entry in entries])
        return [
            BenchmarkServiceHealth(
                name=entry.name,
                url=entry.url,
                healthy=True,
                latency_ms=len(checked_pages),
            )
            for entry in entries
        ]

    def mock_format_table(
        rows: list[dict[str, str]],
        _headers: list[str],
        _current_page: int,
        _total_pages: int,
        _total_count: int,
        _item_name: str,
    ) -> None:
        rendered_pages.append([row["Benchmark"] for row in rows])

    monkeypatch.setattr(services_module.click, "getchar", lambda: next(keys))
    monkeypatch.setattr(services_module, "format_table", mock_format_table)

    paginate_services(service_entries, limit=2, check_services=check_services)

    assert checked_pages == [["one", "two"], ["three"]]
    assert rendered_pages == [["one", "two"], ["three"], ["one", "two"]]
    assert capsys.readouterr().out.count("\033[2J\033[3J\033[1;1H") == 3
