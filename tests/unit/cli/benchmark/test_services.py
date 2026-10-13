"""Benchmark service list against a fake Tracker.

Run: uv run pytest tests/unit/cli/benchmark/test_services.py
"""

from collections.abc import Callable
from importlib import import_module

import pytest
from click.testing import CliRunner
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth, BenchmarkServicesResponse

from tests.unit.cli.fake_tracker import FakeTracker
from valkyrie.cli.benchmark.services import paginate_services
from valkyrie.cli.main import cli

services_module = import_module("valkyrie.cli.benchmark.services")


class HostedTracker:
    """Stand in for TrackerService: a hosted catalog whose services answer health checks."""

    catalog = [
        BenchmarkServiceEntry(name="swebench", url="https://swebench.benchmarks.vals.ai"),
        BenchmarkServiceEntry(name="fab", url="https://fab.benchmarks.vals.ai"),
    ]

    def __enter__(self) -> "HostedTracker":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def catalog_benchmark_services(self) -> list[BenchmarkServiceEntry]:
        return self.catalog

    def check_benchmark_services(self, services: list[BenchmarkServiceEntry]) -> BenchmarkServicesResponse:
        return BenchmarkServicesResponse(
            services=[
                BenchmarkServiceHealth(name=service.name, url=service.url, healthy=True, latency_ms=1)
                for service in services
            ]
        )


def test_list_merges_hosted_services_with_the_saved_urls(
    fake_tracker: FakeTracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Saved URLs replace hosted services of the same name, and every row is health-checked."""
    fake_tracker.benchmark_services = {
        "swebench": {"url": "http://local-swebench"},
        "custombench": {"url": "http://custombench"},
    }
    checked: list[tuple[str, str]] = []

    def capture_paginated_services(
        services: list[BenchmarkServiceEntry],
        *,
        check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]],
    ) -> None:
        checked.extend((service.name, service.url) for service in check_services(services))

    monkeypatch.setattr(services_module, "TrackerService", HostedTracker)
    monkeypatch.setattr(services_module, "paginate_services", capture_paginated_services)

    result = CliRunner().invoke(cli, ["benchmark", "service", "list"])

    assert result.exit_code == 0, result.output
    assert checked == [
        ("swebench", "http://local-swebench"),
        ("fab", "https://fab.benchmarks.vals.ai"),
        ("custombench", "http://custombench"),
    ]


def test_list_without_any_services(fake_tracker: FakeTracker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(services_module, "TrackerService", HostedTracker)
    monkeypatch.setattr(HostedTracker, "catalog", [])

    result = CliRunner().invoke(cli, ["benchmark", "service", "list"])

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
