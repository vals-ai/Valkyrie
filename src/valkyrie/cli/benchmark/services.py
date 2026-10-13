"""List benchmark services and save the service URLs and credentials your runs use through Tracker."""

from collections.abc import Callable, Sequence
from urllib.parse import quote, urlparse

import click
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth

from valkyrie.cli.display import format_table, paginate_cli_pages
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService, request_tracker


@click.group()
def service() -> None:
    """Save benchmark service URLs that your runs use instead of the hosted services."""


@service.command("set")
@click.argument("benchmark")
@click.argument("url")
@click.option(
    "--auth-secret",
    metavar="SECRET",
    help="Organization secret whose value the Tracker sends to the service. Create it with `valkyrie secret set`.",
)
@click.option("--auth-header", metavar="HEADER", help="Header that carries the secret. Defaults to Authorization.")
def service_set(benchmark: str, url: str, auth_secret: str | None, auth_header: str | None) -> None:
    """Use a service URL for your runs of a benchmark.

    Replaces the URL and credential saved before. The Tracker must be able to reach the URL.

    Example: valkyrie benchmark service set swebench https://my-tunnel.ngrok.io --auth-secret swebench-key
    """
    settings = request_tracker(
        "PUT",
        f"/settings/benchmark-services/{quote(benchmark, safe='')}",
        json={"url": url, "auth_secret_name": auth_secret, "auth_header_name": auth_header},
    )
    saved = settings["benchmark_services"][benchmark]
    message = f"Runs of {benchmark} now use {saved['url']}"
    if saved["auth_secret_name"]:
        message += f", sending the secret {saved['auth_secret_name']} as {saved['auth_header_name']}"
    click.echo(f"{message}.")


@service.command("remove")
@click.argument("benchmark")
def service_remove(benchmark: str) -> None:
    """Go back to the hosted service for your runs of a benchmark.

    Example: valkyrie benchmark service remove swebench
    """
    request_tracker("DELETE", f"/settings/benchmark-services/{quote(benchmark, safe='')}")
    click.echo(f"Runs of {benchmark} now use the hosted service.")


@service.command("list")
def service_list() -> None:
    """List hosted benchmark services and your saved URLs, with their latency.

    A URL you saved replaces the hosted service with the same name.
    """
    saved: dict[str, dict[str, str]] = request_tracker("GET", "/settings")["benchmark_services"]

    try:
        with TrackerService() as tracker:
            services_by_name: dict[str, BenchmarkServiceEntry | BenchmarkServiceHealth] = {
                service.name: service for service in tracker.catalog_benchmark_services()
            }
            services_by_name.update(
                {name: BenchmarkServiceEntry(name=name, url=entry["url"]) for name, entry in saved.items()}
            )
            if not services_by_name:
                click.echo(click.style("No benchmark services configured.", fg="yellow"))
                return

            paginate_services(
                list(services_by_name.values()),
                check_services=lambda entries: tracker.check_benchmark_services(entries).services,
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e)) from e


def _service_source_domain(service: BenchmarkServiceHealth) -> str:
    host = urlparse(service.url).netloc or service.url
    return host.removeprefix(f"{service.name}.")


def _health_checked_page(
    services: Sequence[BenchmarkServiceEntry | BenchmarkServiceHealth],
    cache: dict[str, BenchmarkServiceHealth],
    check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]] | None,
) -> list[BenchmarkServiceHealth]:
    missing = [service for service in services if service.name not in cache]
    if missing and check_services is not None:
        entries = [BenchmarkServiceEntry(name=service.name, url=service.url) for service in missing]
        cache.update({service.name: service for service in check_services(entries)})

    for service in missing:
        cache.setdefault(
            service.name,
            service
            if isinstance(service, BenchmarkServiceHealth)
            else BenchmarkServiceHealth(name=service.name, url=service.url, healthy=False, latency_ms=None),
        )

    return [cache[service.name] for service in services]


def paginate_services(
    services: Sequence[BenchmarkServiceEntry | BenchmarkServiceHealth],
    limit: int = 10,
    check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]] | None = None,
) -> None:
    """Interactively page through benchmark service rows."""
    health_cache: dict[str, BenchmarkServiceHealth] = {}

    def load_page(offset: int, page_limit: int) -> tuple[int, list[BenchmarkServiceHealth]]:
        page_services = _health_checked_page(services[offset : offset + page_limit], health_cache, check_services)
        return len(services), page_services

    def render_page(
        page_services: list[BenchmarkServiceHealth],
        current_page: int,
        total_pages: int,
        total_count: int,
    ) -> None:
        rows = [
            {
                "Benchmark": service.name,
                "Service URL": service.url,
                "Source": _service_source_domain(service),
                "Latency": f"{service.latency_ms} ms" if service.latency_ms is not None else "-",
            }
            for service in page_services
        ]
        format_table(
            rows,
            ["Benchmark", "Service URL", "Source", "Latency"],
            current_page,
            total_pages,
            total_count,
            "service",
        )

    paginate_cli_pages(
        load_page,
        render_page,
        limit=limit,
        render_empty=lambda: click.echo(click.style("No benchmark services found.", fg="yellow")),
    )
