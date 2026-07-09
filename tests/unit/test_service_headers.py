import pytest

from valkyrie.cli import service_headers


class MockTrackerService:
    @staticmethod
    def get_benchmark_auth(benchmark_name: str) -> str | None:
        return "Bearer configured" if benchmark_name == "private" else None


def test_benchmark_service_headers_merge_configured_and_cli_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Benchmark service headers should preserve configured auth while letting CLI flags override.

    Test cases:
    - Configured auth is included with extra headers.
    - CLI Authorization overrides configured auth, and public benchmarks keep only CLI headers.
    """
    monkeypatch.setattr(service_headers, "TrackerService", MockTrackerService)

    assert service_headers.benchmark_service_headers("private", [("X-Test", "1")]) == {
        "Authorization": "Bearer configured",
        "X-Test": "1",
    }
    assert service_headers.benchmark_service_headers("private", [("Authorization", "Bearer cli")]) == {
        "Authorization": "Bearer cli"
    }
    assert service_headers.benchmark_service_headers("public", [("X-Test", "1")]) == {"X-Test": "1"}
