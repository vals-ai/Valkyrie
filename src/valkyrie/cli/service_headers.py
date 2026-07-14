from collections.abc import Iterable

from valkyrie.cli.tracker_client import TrackerService


def benchmark_service_headers(
    benchmark_name: str,
    headers: Iterable[tuple[str, str]] = (),
) -> dict[str, str]:
    """Resolve configured and CLI-provided benchmark service headers."""
    service_headers: dict[str, str] = {}
    auth_credential = TrackerService.get_benchmark_auth(benchmark_name)
    if auth_credential:
        service_headers["Authorization"] = str(auth_credential)
    service_headers.update(headers)
    return service_headers
