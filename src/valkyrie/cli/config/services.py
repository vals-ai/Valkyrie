from typing import Any

import click
import yaml
from tracker.types import BenchmarkServiceEntry

from valkyrie.cli.config import config
from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import CONFIG_LOCATION, paginate_services


@config.group()
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
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    if "custom_benchmark_services" not in harness_config:
        harness_config["custom_benchmark_services"] = {}

    harness_config["custom_benchmark_services"][name] = url

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(harness_config, f, default_flow_style=False)

    click.echo(click.style(f"Service '{name}' has been set.", fg="green"))


@service.command("remove")
@click.argument("name")
def service_remove(name: str) -> None:
    """Remove a custom URL override for a benchmark service.

    Example: valkyrie config service remove swebench
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    services = current.get("custom_benchmark_services") or {}
    if name not in services:
        raise click.ClickException(f"Service '{name}' not configured.")

    del services[name]
    current["custom_benchmark_services"] = services

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"Service '{name}' has been removed.", fg="green"))


@service.command("list")
def service_list() -> None:
    """List hosted and custom benchmark services."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

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
