"""CLI views/commands for Valkyrie."""

import click

from valkyrie.cli.agent import agent
from valkyrie.cli.agent.commands import agent_remove, download, install, list_installed_agents, push
from valkyrie.cli.benchmark import benchmark
from valkyrie.cli.benchmark.tasks import tasks
from valkyrie.cli.config import config
from valkyrie.cli.config.auth import auth, auth_list, auth_remove, auth_set
from valkyrie.cli.config.base import _REQUIRED_ENVIRONMENT_VARIABLES, config_remove, init, set
from valkyrie.cli.config.providers import provider, provider_default, provider_list, provider_remove, provider_set
from valkyrie.cli.config.services import service, service_list, service_remove, service_set
from valkyrie.cli.logging import configure_cli_logging
from valkyrie.cli.run import run
from valkyrie.cli.run.analyze import analyze
from valkyrie.cli.run.fetch import fetch
from valkyrie.cli.run.list import list_benchmarks
from valkyrie.cli.run.outputs import output_path, outputs
from valkyrie.cli.run.results import results
from valkyrie.cli.run.resume import resume, retry_command
from valkyrie.cli.run.start import start
from valkyrie.cli.run.stop import stop
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
