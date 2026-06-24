from click.testing import CliRunner
import pytest
from tracker.types import BenchmarkListEntry, ListBenchmarksResponse

from valkyrie.cli import main as cli_main
from valkyrie.cli.exceptions import TrackerServiceError


class FakeTrackerService:
    def __enter__(self) -> "FakeTrackerService":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def close(self) -> None:
        pass

    def list_benchmarks(self, include_inaccessible: bool = False) -> ListBenchmarksResponse:
        del include_inaccessible
        return ListBenchmarksResponse(
            benchmarks=[
                BenchmarkListEntry(benchmark_name="ioi", datasets=["ioi2024"]),
                BenchmarkListEntry(benchmark_name="swebench", datasets=["default"]),
            ]
        )


class NoAccessTrackerService(FakeTrackerService):
    def list_benchmarks(self, include_inaccessible: bool = False) -> ListBenchmarksResponse:
        return ListBenchmarksResponse(
            benchmarks=[BenchmarkListEntry(benchmark_name="swebench", datasets=[])] if include_inaccessible else []
        )


class BrokenTrackerService(FakeTrackerService):
    def __init__(self) -> None:
        raise TrackerServiceError("broken tracker")


def test_benchmark_list_outputs_accessible_benchmarks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: True)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list"])

    assert result.exit_code == 0
    assert "swebench" in result.output
    assert "default" in result.output
    assert "ioi" in result.output
    assert "ioi2024" in result.output


def test_benchmark_list_can_show_inaccessible_benchmarks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "TrackerService", NoAccessTrackerService)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: True)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list", "--include-inaccessible"])

    assert result.exit_code == 0
    assert "swebench" in result.output
    assert "No access" in result.output


def test_benchmark_list_prints_empty_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "TrackerService", NoAccessTrackerService)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: True)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list"])

    assert result.exit_code == 0
    assert "No accessible benchmarks found." in result.output


def test_benchmark_list_exits_when_tracker_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: False)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list"])

    assert result.exit_code == 0
    assert result.output == ""


def test_benchmark_list_surfaces_tracker_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "TrackerService", BrokenTrackerService)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list"])

    assert result.exit_code != 0
    assert "broken tracker" in result.output
