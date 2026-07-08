from collections.abc import Callable, Sequence
from urllib.parse import urlparse

import click
from tracker.types import BenchmarkServiceEntry, BenchmarkServiceHealth

from valkyrie.cli.config.state import load_config, write_config
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.display import format_table
from valkyrie.cli.tracker_client import TrackerService


@click.group()
def service() -> None:
    """Manage custom benchmark service URL overrides."""
    pass


@service.command("set")
@click.argument("name")
@click.argument("url")
def service_set(name: str, url: str) -> None:
    """Set a custom URL for a benchmark service (creates or updates).

    Example: valkyrie config service set swebench https://my-tunnel.ngrok.io
    """
    harness_config = load_config()

    if "custom_benchmark_services" not in harness_config:
        harness_config["custom_benchmark_services"] = {}

    harness_config["custom_benchmark_services"][name] = url

    write_config(harness_config)

    click.echo(click.style(f"Service '{name}' has been set.", fg="green"))


@service.command("remove")
@click.argument("name")
def service_remove(name: str) -> None:
    """Remove a custom URL override for a benchmark service.

    Example: valkyrie config service remove swebench
    """
    current = load_config()

    services = current.get("custom_benchmark_services") or {}
    if name not in services:
        raise click.ClickException(f"Service '{name}' not configured.")

    del services[name]
    current["custom_benchmark_services"] = services

    write_config(current)

    click.echo(click.style(f"Service '{name}' has been removed.", fg="green"))


@service.command("list")
def service_list() -> None:
    """List hosted and custom benchmark services."""
    current = load_config()

    services: dict[str, str] = current.get("custom_benchmark_services") or {}
    custom_entries = [BenchmarkServiceEntry(name=name, url=url) for name, url in services.items()]

    try:
        with TrackerService(require_config=False) as tracker:
            services_by_name = {service.name: service for service in tracker.catalog_benchmark_services()}
            services_by_name.update({service.name: service for service in custom_entries})
            services_list = list(services_by_name.values())
            if not services_list:
                click.echo(click.style("No benchmark services configured.", fg="yellow"))
                return

            paginate_services(
                services_list,
                check_services=lambda entries: tracker.check_benchmark_services(entries).services,
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e)) from e


def _clear_pager() -> None:
    click.echo("\033[2J\033[3J\033[1;1H", nl=False, color=True)


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
        if isinstance(service, BenchmarkServiceHealth):
            cache.setdefault(service.name, service)
        else:
            cache.setdefault(
                service.name,
                BenchmarkServiceHealth(name=service.name, url=service.url, healthy=False, latency_ms=None),
            )

    return [cache[service.name] for service in services]


def paginate_services(
    services: Sequence[BenchmarkServiceEntry | BenchmarkServiceHealth],
    limit: int = 10,
    check_services: Callable[[list[BenchmarkServiceEntry]], list[BenchmarkServiceHealth]] | None = None,
) -> None:
    """Interactively page through benchmark service rows."""
    total_count = len(services)
    if total_count == 0:
        _clear_pager()
        click.echo(click.style("No benchmark services found.", fg="yellow"))
        return

    current_page = 1
    total_pages = max(1, (total_count + limit - 1) // limit)
    health_cache: dict[str, BenchmarkServiceHealth] = {}

    while True:
        _clear_pager()

        page_start = (current_page - 1) * limit
        page_services = _health_checked_page(services[page_start : page_start + limit], health_cache, check_services)
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

        if total_pages <= 1:
            break

        char = click.getchar()

        if char == "l" and current_page < total_pages:
            current_page += 1
        elif char == "h" and current_page > 1:
            current_page -= 1
        elif char == "q" or char == "\x03":
            break
