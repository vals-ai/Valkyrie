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
        if benchmark_name == "cyber-range":
            service_headers["x-descope-api-key"] = str(auth_credential).removeprefix("Bearer ")
        else:
            service_headers["Authorization"] = str(auth_credential)
    service_headers.update(headers)
    return service_headers
