"""CLI views/commands for Valkyrie."""

import click

from valkyrie.cli.agent import agent
from valkyrie.cli.agent.commands import agent_remove, download, install, list_installed_agents, push
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.benchmark.tasks import tasks
from valkyrie.cli.config import (
    _REQUIRED_ENVIRONMENT_VARIABLES,
    auth,
    auth_list,
    auth_remove,
    auth_set,
    config,
    config_remove,
    init,
    provider,
    provider_default,
    provider_list,
    provider_remove,
    provider_set,
    service,
    service_list,
    service_remove,
    service_set,
    set,
)
from valkyrie.cli.logging import configure_cli_logging
from valkyrie.cli.run import (
    analyze,
    fetch,
    list_benchmarks,
    output_path,
    outputs,
    results,
    resume,
    retry_command,
    run,
    start,
    stop,
)
from valkyrie.cli.s3_client import get_contract_from_s3
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import CONFIG_LOCATION, check_tracker_service_health, paginate_services, stream_benchmark_status


@click.group()
def cli():
    """Valkyrie CLI."""
    configure_cli_logging()


cli.add_command(run)
cli.add_command(agent)
cli.add_command(benchmark)
cli.add_command(config)

__all__ = [
    "CONFIG_LOCATION",
    "TrackerService",
    "_REQUIRED_ENVIRONMENT_VARIABLES",
    "agent",
    "agent_remove",
    "analyze",
    "auth",
    "auth_list",
    "auth_remove",
    "auth_set",
    "benchmark",
    "check_tracker_service_health",
    "cli",
    "config",
    "config_remove",
    "download",
    "fetch",
    "get_contract_from_s3",
    "init",
    "install",
    "list_benchmarks",
    "list_installed_agents",
    "output_path",
    "outputs",
    "paginate_services",
    "provider",
    "provider_default",
    "provider_list",
    "provider_remove",
    "provider_set",
    "push",
    "results",
    "resume",
    "retry_command",
    "run",
    "service",
    "service_list",
    "service_remove",
    "service_set",
    "set",
    "start",
    "stop",
    "stream_benchmark_status",
    "tasks",
]


if __name__ == "__main__":
    cli()
