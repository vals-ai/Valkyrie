import pytest
from click.testing import CliRunner

from valkyrie.cli import main as cli_main
from valkyrie.cli.exceptions import TrackerServiceError


class FakeTrackerService:
    calls: list[tuple[str, str | None, str | None]] = []

    def __enter__(self) -> "FakeTrackerService":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    @staticmethod
    def get_benchmark_auth(_benchmark_name: str) -> str | None:
        return None

    def close(self) -> None:
        pass

    def fetch_benchmark_tasks(
        self,
        benchmark_name: str,
        dataset: str | None = None,
        slice_str: str | None = None,
        ignore_custom_services: bool = False,
        service_headers: dict[str, str] | None = None,
    ) -> list[str]:
        del ignore_custom_services, service_headers
        self.calls.append((benchmark_name, dataset, slice_str))
        if (benchmark_name, dataset) in {("swebench", None), ("ioi", "ioi2024")}:
            return []
        raise TrackerServiceError("no access")


def test_benchmark_list_outputs_accessible_benchmarks(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeTrackerService.calls = []
    monkeypatch.setattr(
        cli_main,
        "HOSTED_BENCHMARK_DATASETS",
        {"ioi": ("ioi2024", "ioi2025"), "swebench": ("default", "vals_index")},
    )
    monkeypatch.setattr(cli_main, "TrackerService", FakeTrackerService)
    monkeypatch.setattr(cli_main, "check_tracker_service_health", lambda _tracker: True)

    result = CliRunner().invoke(cli_main.cli, ["benchmark", "list"])

    assert result.exit_code == 0
    assert "swebench" in result.output
    assert "default" in result.output
    assert "ioi" in result.output
    assert "ioi2024" in result.output
    assert ("swebench", None, "0:0") in FakeTrackerService.calls
    assert ("swebench", "vals_index", "0:0") in FakeTrackerService.calls
