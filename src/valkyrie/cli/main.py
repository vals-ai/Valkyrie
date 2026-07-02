"""CLI views/commands for Valkyrie."""

import asyncio
import os
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import click
import yaml
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX
from tracker.database.models import BenchmarkStatus, RetryMode
from tracker.exceptions import S3Error
from tracker.types import (
    BenchmarkServiceEntry,
    FinalViewResponse,
    Order,
    RetrieveResultsResponse,
    StartBenchmarkResponse,
)

from valkyrie.cli.bundler import get_contract
from valkyrie.cli.exceptions import BundlerError, ContractValidationError, TrackerServiceError
from valkyrie.cli.logging import configure_cli_logging
from valkyrie.cli.s3_client import (
    download_agent,
    download_s3_path,
    get_contract_from_s3,
    get_ingest_lambda_from_s3,
    install_agent,
    list_agents,
    push_agent,
    remove_agent,
    update_benchmark_agent_version,
)
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import (
    CONFIG_LOCATION,
    ConfigValue,
    check_tracker_service_health,
    download_run_outputs,
    download_final_view,
    format_agent_start_details,
    format_benchmark_status,
    format_run_start_details,
    format_start_benchmark_response,
    format_table,
    paginate_agents,
    paginate_benchmarks,
    paginate_services,
    resolve_task_ids,
    resolve_webhook_config,
    stream_benchmark_status,
)
from valkyrie.schemas import AgentConfig


@click.group()
def cli():
    """Valkyrie CLI."""
    configure_cli_logging()


@cli.group()
def run():
    """Run command group"""
    pass


@cli.group()
def agent():
    """Agent command group"""
    pass


@cli.group()
def benchmark():
    """Benchmark command group"""
    pass


@cli.group()
def config():
    """Config command group"""
    pass


# Mapping between the expected key and default value
# None means its user provided if not found
_REQUIRED_ENVIRONMENT_VARIABLES: dict[str, str | None | int] = {
    "AWS_ACCESS_KEY_ID": None,  # AWS ACCESS KEY
    "AWS_SECRET_ACCESS_KEY": None,  # AWS SECRETS KEY
    "AWS_DEFAULT_REGION": None,  # What region your secrets are in
    "S3_BUCKET": None,  # Center point where all agents and benchmark results are uploaded
    "LOG_GROUP": "benchmarks",  # the prefix to the cloudwatch logs (e.x. benchmarks/<benchmark_id>)
    "LOG_RETENTION_POLICY": 365,  # How long logs are kept until auto deleted
}


@config.command()
def init() -> None:
    """
    Initializes a config we can trust to have references to dependencies to run Valkyrie,
    this becomes our source of truth for secrets required to run Valkyrie
    """

    current_config: dict[str, Any] = {}
    if CONFIG_LOCATION.exists():
        with open(CONFIG_LOCATION) as f:
            try:
                current_config = yaml.safe_load(f) or {}
            except Exception:
                pass

    mode = click.prompt(
        "Setup mode",
        type=click.Choice(["hosted", "self-hosted"]),
        default="self-hosted",
    )

    if mode == "hosted":
        api_key = os.environ.get("VALKYRIE_API_KEY") or click.prompt("API Key")
        current_config["api_key"] = api_key

        # Validate the key and create/confirm org (uses default tracker URL)
        try:
            result = TrackerService.init_org(api_key)
        except TrackerServiceError as e:
            raise click.ClickException(str(e))
        click.echo(f"Organization '{result['org_name']}' configured successfully.\n")

        if result.get("email_claim_missing"):
            click.echo(
                click.style(
                    "⚠  This access key is missing the 'email' custom claim. "
                    "Run attribution for runs you start will be empty.\n"
                    "   Ask your Vals admin to add an 'email' (and optionally 'name') "
                    "custom claim to this key.",
                    fg="yellow",
                )
            )

    # Both modes require AWS credentials
    collected_keys: dict[str, str] = {}
    for key, default in _REQUIRED_ENVIRONMENT_VARIABLES.items():
        sourced = current_config.get(key) or os.environ.get(key)
        if sourced:
            click.echo(f"  {key}: sourced from {'environment' if not current_config.get(key) else 'existing config'}")
            collected_keys[key] = sourced
            continue

        if not default:
            value = click.prompt(
                f"  {key} (required, Enter to cancel)",
                default="",
                show_default=False,
            )

            if not value.strip():
                click.echo(click.style(f"\n  {key} is required. Aborting.", fg="red"))
                raise click.Abort()
        else:
            value = click.prompt(f"  {key}", default=str(default))

        collected_keys[key] = value

    current_config.update(collected_keys)

    if mode != "hosted":
        current_config.pop("api_key", None)

    CONFIG_LOCATION.parent.mkdir(parents=True, exist_ok=True)

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current_config, f, default_flow_style=False)

    click.echo(click.style(f"\nConfig written to {CONFIG_LOCATION}", fg="green", bold=True))


@config.command()
@click.argument("key")
@click.argument("value")
def set(key: str, value: str) -> None:
    """
    Set a single key in the Valkyrie config.

    Example: valkyrie config set AWS_DEFAULT_REGION us-west-2
    """

    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    try:
        config_value = ConfigValue.from_str(key)
    except ValueError:
        raise click.ClickException(
            f"Key '{key}' is not a valid config key. Valid keys: {', '.join(m.value for m in ConfigValue)}"
        )

    current[config_value.value] = value

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"  {key} updated.", fg="green"))


@config.command(name="remove")
@click.argument("key")
def config_remove(key: str) -> None:
    """
    Remove a single key from the Valkyrie config.

    Example: valkyrie config remove AWS_DEFAULT_REGION
    """

    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    try:
        config_value = ConfigValue.from_str(key)
    except ValueError:
        raise click.ClickException(
            f"Key '{key}' is not a valid config key. Valid keys: {', '.join(m.value for m in ConfigValue)}"
        )

    if config_value.value in _REQUIRED_ENVIRONMENT_VARIABLES:
        raise click.ClickException(
            f"Key '{key}' is required and cannot be removed. Consider using `valkyrie config set` to update it."
        )

    if config_value.value in current:
        del current[config_value.value]

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"  {key} removed.", fg="green"))


@config.group()
def provider() -> None:
    """Manage sandbox provider secret mappings."""
    pass


@provider.command("set")
@click.argument("name")
@click.argument("secret_name")
def provider_set(name: str, secret_name: str) -> None:
    """Set a named sandbox provider secret.

    Example: valkyrie config provider set daytona DaytonaSecrets
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    providers = harness_config.get("sandbox_providers")
    if not isinstance(providers, dict):
        providers = {}

    providers[name] = secret_name
    harness_config["sandbox_providers"] = providers

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(harness_config, f, default_flow_style=False, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' has been set.", fg="green"))


@provider.command("default")
@click.argument("name")
def provider_default(name: str) -> None:
    """Set the default sandbox provider.

    Example: valkyrie config provider default modal
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    providers = harness_config.get("sandbox_providers")
    if not isinstance(providers, dict) or name not in providers:
        raise click.ClickException(f"Provider '{name}' not configured.")

    harness_config["default_sandbox_provider"] = name

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(harness_config, f, default_flow_style=False, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' is now the default.", fg="green"))


@provider.command("remove")
@click.argument("name")
def provider_remove(name: str) -> None:
    """Remove a named sandbox provider secret.

    Example: valkyrie config provider remove modal
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    providers = harness_config.get("sandbox_providers") or {}
    if name not in providers:
        raise click.ClickException(f"Provider '{name}' not configured.")

    del providers[name]
    if providers:
        harness_config["sandbox_providers"] = providers
    else:
        harness_config.pop("sandbox_providers", None)
    if harness_config.get("default_sandbox_provider") == name:
        harness_config.pop("default_sandbox_provider", None)

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(harness_config, f, default_flow_style=False, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' has been removed.", fg="green"))


@provider.command("list")
def provider_list() -> None:
    """List all configured sandbox providers."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    providers: dict[str, str] = harness_config.get("sandbox_providers") or {}
    if not providers:
        click.echo(click.style("No sandbox providers configured.", fg="yellow"))
        return

    for name, secret_name in providers.items():
        click.echo(f"{name}: {secret_name}")


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


@config.group()
def auth() -> None:
    """Manage benchmark service auth credentials."""
    pass


@auth.command("set")
@click.argument("name")
@click.argument("credential")
def auth_set(name: str, credential: str) -> None:
    """Set an auth credential for a benchmark service.

    Example: valkyrie config auth set swebench my-secret-credential
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        harness_config: dict[str, Any] = yaml.safe_load(f) or {}

    if "benchmark_auth" not in harness_config:
        harness_config["benchmark_auth"] = {}

    harness_config["benchmark_auth"][name] = credential

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(harness_config, f, default_flow_style=False)

    click.echo(click.style(f"Auth for '{name}' has been set.", fg="green"))


@auth.command("remove")
@click.argument("name")
def auth_remove(name: str) -> None:
    """Remove an auth credential for a benchmark service.

    Example: valkyrie config auth remove swebench
    """
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    auth_credentials = current.get("benchmark_auth") or {}
    if name not in auth_credentials:
        raise click.ClickException(f"Auth for '{name}' not configured.")

    del auth_credentials[name]
    current["benchmark_auth"] = auth_credentials

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"Auth for '{name}' has been removed.", fg="green"))


@auth.command("list")
def auth_list() -> None:
    """List all configured benchmark auth credentials."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    auth_credentials: dict[str, str] = current.get("benchmark_auth") or {}
    if not auth_credentials:
        click.echo(click.style("No benchmark auth credentials configured.", fg="yellow"))
        return

    rows = [
        {"Benchmark": name, "Credential": credential[:4] + "***" if len(credential) > 4 else "***"}
        for name, credential in auth_credentials.items()
    ]
    format_table(rows, ["Benchmark", "Credential"], item_name="credential")


@benchmark.command(
    name="tasks",
    help="Save task IDs for a benchmark dataset. \n\nExample:\nvalkyrie benchmark tasks swebench --dataset default",
)
@click.argument("benchmark_name", type=str)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Dataset name to fetch from the benchmark service (defaults to 'default')",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential)",
)
@click.option(
    "--ignore-custom-services",
    "--ics",
    is_flag=True,
    help="Ignore custom benchmark services that have been configured. Provides opt-out for custom services.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=False,
    default=None,
    help="Path to save task IDs, one per line. Defaults to <benchmark>-<dataset>-tasks.txt.",
)
def tasks(
    benchmark_name: str,
    dataset: str | None,
    headers: tuple[tuple[str, str]],
    ignore_custom_services: bool,
    output: Path | None,
):
    """
    Save task IDs for a benchmark dataset.
    """
    service_headers: dict[str, str] = {}
    auth_credential = TrackerService.get_benchmark_auth(benchmark_name)
    if auth_credential:
        service_headers["Authorization"] = str(auth_credential)
    for name, value in headers:
        service_headers[name] = value

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            response = tracker.fetch_benchmark_tasks(
                benchmark_name,
                dataset=dataset,
                ignore_custom_services=ignore_custom_services,
                service_headers=service_headers,
            )
            output_path = output or Path(f"{benchmark_name}-{dataset or 'default'}-tasks.txt")
            output_path.write_text("\n".join(response) + "\n", encoding="utf-8")
            click.echo(f"Wrote {len(response)} task IDs to {output_path}")
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    help="Start a run. \n\nExample:\nvalkyrie run start --agent agents/claude_code --benchmark swebench --concurrency 5"
)
@click.option(
    "--agent",
    type=str,
    required=True,
    help="Path to local agent directory or S3 agent name (e.g., agents/claude_code or claude_code)",
)
@click.option(
    "--model",
    type=str,
    required=False,
    help="Model key (e.g., openai/gpt-4o)",
)
@click.option(
    "--benchmark",
    type=str,
    required=True,
    help="Name of the benchmark to run (e.g., swebench, finance)",
)
@click.option(
    "--concurrency",
    type=int,
    default=5,
    required=False,
    help="Number of concurrent tasks to run (e.g., 5)",
)
@click.option(
    "--lambda",
    "lambda_function",
    type=str,
    default=None,
    required=False,
    help="Lambda function to invoke at the end of the benchmark run",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908)",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line",
)
@click.option(
    "--slice",
    "slice_str",
    type=str,
    required=False,
    default=None,
    help="Slice string to use for slicing the benchmark (e.g., 1-10)",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Dataset name to use from the benchmark service (defaults to 'default')",
)
@click.option(
    "--provider",
    type=str,
    required=False,
    default=None,
    help="Named sandbox provider from config (e.g., daytona, modal)",
)
@click.option(
    "--label",
    "-l",
    type=str,
    required=False,
    default=None,
    help="Label to attach to the run",
)
@click.option(
    "--kwarg",
    "-k",
    "kwargs",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Kwargs as key value (e.g., -k temperature 7 -k max_tokens 1000)",
)
@click.option(
    "--secret",
    "-s",
    "secrets",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Secret as ENV_VAR aws_secret_name (e.g., -s ANTHROPIC_API_KEY devEvalInfraAnthropicKey)",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential)",
)
@click.option(
    "--interval",
    "-i",
    "intervals",
    multiple=True,
    type=int,
    help="Progress percentage threshold for Slack notification (e.g., -i 25 -i 75). Max 3, must be divisible by 5, range 5-100.",
)
@click.option(
    "--ignore-custom-services",
    "--ics",
    is_flag=True,
    help="Ignore custom benchmark services that have been configured. Provides opt-out for custom services.",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after starting",
)
def start(
    agent: str,
    model: str | None,
    benchmark: str,
    concurrency: int,
    lambda_function: str | None,
    task_ids: str | None,
    task_ids_file: str | None,
    slice_str: str | None,
    dataset: str | None,
    provider: str | None,
    label: str | None,
    kwargs: tuple[tuple[str, str]],
    secrets: tuple[tuple[str, str]],
    headers: tuple[tuple[str, str]],
    intervals: tuple[int, ...],
    ignore_custom_services: bool,
    connect: bool,
):
    """
    Run an agent on a benchmark.

    Example:
        valkyrie run start --agent agents/claude_code --benchmark swebench
    """
    formatted_task_ids = resolve_task_ids(task_ids, task_ids_file)

    try:
        TrackerService.validate_sandbox_provider(provider)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))

    service_headers: dict[str, str] = {}
    auth_credential = TrackerService.get_benchmark_auth(benchmark)
    if auth_credential:
        service_headers["Authorization"] = str(auth_credential)
    for name, value in headers:
        service_headers[name] = value

    # Webhook notification setup (may print a warning before the boxes)
    webhook_secret, webhook_intervals = resolve_webhook_config(intervals, TrackerService.get_webhook_secret())

    task_ids_display = ",".join(formatted_task_ids) if formatted_task_ids else None
    format_run_start_details(benchmark, dataset, concurrency, slice_str, task_ids_display)

    format_agent_start_details(agent, model, secrets, kwargs, service_headers, webhook_secret, webhook_intervals)

    try:
        # Build agent config
        config_kwargs: dict[str, Any] = {}
        if model:
            config_kwargs["model"] = model

        config_kwargs["kwargs"] = {key: value for key, value in kwargs}
        agent_config = AgentConfig(**config_kwargs)

        agent_path = Path(agent)

        # If the user specified an agent on their machine we upload it first
        if agent_path.is_dir():
            asyncio.run(push_agent(agent_path.stem, agent_path))
            contract_file = next(
                (
                    agent_path / f"contract{ext}"
                    for ext in (".yaml", ".yml")
                    if (agent_path / f"contract{ext}").exists()
                ),
                agent_path / "contract.yaml",
            )
            contract = get_contract(contract_file, agent_config)
            contract.name = agent_path.stem
        else:
            contract = asyncio.run(get_contract_from_s3(agent, agent_config))
            contract.name = agent

        # Merge CLI secrets into contract defaults (override with cli secret)
        if secrets:
            contract.secrets.update({key: value for key, value in secrets})

        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            click.echo(f"\r\033[KStarting run for: {contract.name}...", nl=False)

            response = tracker.start_benchmark(
                contract,
                benchmark,
                concurrency,
                ignore_custom_services,
                formatted_task_ids,
                slice_str,
                label,
                lambda_function,
                dataset,
                service_headers=service_headers or None,
                provider=provider,
                webhook_secret_name=webhook_secret if webhook_intervals else None,
                webhook_intervals=webhook_intervals,
            )

            click.echo("\r\033[K", nl=False)
            if response.status_code != 200:
                click.echo(click.style("Run failed to start!", fg="red", bold=True))
                detail = str(response.json().get("detail", response.text))
                match response.status_code:
                    case 401 | 403:
                        click.echo(click.style("Authentication error: ", fg="yellow", bold=True) + detail)
                    case 502:
                        click.echo(click.style("Benchmark service error: ", fg="yellow", bold=True) + detail)
                    case _:
                        click.echo(detail)
                return

            start_response = StartBenchmarkResponse.model_validate(response.json())
            format_start_benchmark_response(start_response)
            if connect:
                stream_benchmark_status(tracker, start_response.benchmark_id)
            else:
                click.echo(
                    f"{'Track progress:':<17} "
                    + click.style(f"valkyrie run fetch {start_response.benchmark_id} --connect", fg="cyan")
                )
    except (BundlerError, TrackerServiceError, ContractValidationError) as e:
        raise click.ClickException(str(e))


@run.command(
    help="Fetch a run by its run id. \n\nExample:\nvalkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates",
)
def fetch(run_id: UUID, connect: bool):
    """
    Fetch a run by its run id.

    Example:
        valkyrie run fetch 123e4567-e89b-12d3-a456-426614174000 --connect
    """

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if connect:
                stream_benchmark_status(tracker, run_id)
            else:
                response = tracker.fetch_benchmark(run_id)
                format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    name="results",
    help=(
        "Retrieve run results by its run id. \n\n"
        "Example:\n"
        "valkyrie run results 123e4567-e89b-12d3-a456-426614174000 "
        "--path ./results-123e4567-e89b-12d3-a456-426614174000.json"
    ),
)
@click.argument("run_id", type=UUID)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    default=None,
    required=False,
    help="Path to save the results (default: ./results-<run_id>.json)",
)
@click.option(
    "--s3",
    is_flag=True,
    default=False,
    required=False,
    help="Saves results to s3 instead of downloading them locally. Can be found at bucket://benchmarks/run_id/<benchmark>.json",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated task IDs to score the subset over. Final score is recomputed over the subset.",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line",
)
def results(run_id: UUID, path: Path | None, s3: bool, task_ids: str | None, task_ids_file: str | None):
    """
    Retrieve the results of a run by its run id.

    Example:
        valkyrie run results e532551e-d51b-4912-983d-47695bd24174 --path ./results-e532551e-d51b-4912-983d-47695bd24174.json
    """
    subset_task_ids = resolve_task_ids(task_ids, task_ids_file)

    click.echo(f"Retrieving results for run: {run_id}")

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if s3:
                if tracker.check_results_exist_in_s3(run_id):
                    if not click.confirm("Results already exist in S3. Overwrite?"):
                        raise click.Abort()

            results_response: RetrieveResultsResponse = tracker.retrieve_results(run_id, s3, task_ids=subset_task_ids)

            if isinstance(results_response, FinalViewResponse):
                if subset_task_ids:
                    scored = len(results_response.evaluation_results or {}) + len(results_response.task_errors or {})
                    click.echo(
                        click.style(
                            f"Scored over {scored} of {len(subset_task_ids)} subset task ids.",
                            fg="yellow" if scored < len(subset_task_ids) else "green",
                        )
                    )
                default_path: Path = Path(f"./results-{run_id}.json")

                download_final_view(path or default_path, results_response)
            else:
                click.echo(click.style("Download (expires in 1 day):", fg="cyan", bold=True))
                click.echo(f"  {results_response.presigned_url}")
                click.echo()
                click.echo(click.style("AWS Console:", fg="cyan", bold=True))
                click.echo(f"  {results_response.console_url}")

    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    help="Stop a run by its run id. \n\nExample:\nvalkyrie run stop 123e4567-e89b-12d3-a456-426614174000 --force"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--force",
    is_flag=True,
    required=False,
    default=False,
    help="Force stop the benchmark run",
)
def stop(run_id: UUID, force: bool):
    """
    Stop a run by its run id.

    Example:
        valkyrie run stop 123e4567-e89b-12d3-a456-426614174000
    """
    action = "Force stop" if force else "Stop"
    if not click.confirm(f"{action} run {run_id}?"):
        click.echo("Cancelled.")
        return

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            _ = tracker.stop_benchmark(run_id, force)

            if force:
                click.echo(click.style("✓ Run force stopped.", fg="green", bold=True))
            else:
                click.echo(
                    click.style(
                        "Run is stopping — will finish after in-flight tasks complete.",
                        fg="yellow",
                        bold=True,
                    )
                )
            click.echo("┌─ Next Steps " + "─" * 66)
            click.echo(
                f"│ {'Get results:':<17} "
                + click.style(f"valkyrie run results {run_id} --path ./results-{run_id}.json", fg="cyan")
            )
            click.echo("└" + "─" * 79)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    name="analyze",
    help="Trigger Docent ingestion + error analysis for a finished run.",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Bypass the cached reading-plan URL and re-fire ingestion.",
)
def analyze(run_id: UUID, no_cache: bool) -> None:
    """Trigger Docent ingestion + error analysis for a finished run."""
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                raise click.ClickException("Tracker service is unhealthy.")

            # Resolve the analyzer Lambda from the agent's current pushed contract.
            metadata = tracker.fetch_benchmark_metadata(run_id)
            contract_name = metadata.benchmark_arguments.contract.name
            try:
                lambda_function = asyncio.run(get_ingest_lambda_from_s3(contract_name))
            except S3Error as e:
                raise click.ClickException(
                    f"Could not load contract for agent '{contract_name}' from S3: {e}\n\n"
                    "If the agent has never been pushed, run `valk agent push ./<agent_dir>`."
                )
            if not lambda_function:
                raise click.ClickException(
                    f"Agent '{contract_name}' has no `ingest_lambda` set in its current contract. "
                    "Declare it in contract.yaml and re-push with `valk agent push ./<agent_dir>`."
                )

            terminal: tuple[str, dict[str, Any]] | None = None
            for event, data in tracker.analyze_benchmark(
                run_id,
                no_cache=no_cache,
                lambda_function=lambda_function,
            ):
                if event == "started":
                    click.echo(f"  Invoking {data.get('lambda_function')}...")
                elif event == "heartbeat":
                    click.echo(".", nl=False)
                elif event == "done":
                    terminal = (event, data)
                elif event == "error":
                    raise click.ClickException(data.get("message") or "analyzer Lambda failed")

            click.echo()

            if not terminal:
                return

            event, data = terminal
            url = data.get("reading_plan_url")
            if url:
                click.echo(f"  Reading plan: {click.style(url, fg='blue', underline=True)}")
            else:
                click.echo(
                    click.style(
                        "Analysis completed but no reading plan URL was produced.",
                        fg="yellow",
                    )
                )
    except TrackerServiceError as e:
        # "Cannot analyze run X: status is IN_PROGRESS (must be FINISHED)." —
        # not really an error, just "come back later." Render it cleanly.
        msg = str(e)
        if "must be FINISHED" in msg:
            for status, line in (
                ("IN_PROGRESS", f"Run {run_id} is still in progress. Try again after it finishes."),
                ("STOPPING", f"Run {run_id} is stopping. Try again once it settles."),
                ("STOPPED", f"Run {run_id} was stopped before completion — nothing to analyze."),
                ("ERROR", f"Run {run_id} errored before completing — nothing to analyze."),
            ):
                if f"status is {status}" in msg:
                    click.echo(click.style(line, fg="yellow"))
                    sys.exit(1)
        raise click.ClickException(msg)


@run.command(
    help="Resume a run by its run id. \n\nExample:\nvalkyrie run resume 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--retry",
    is_flag=True,
    required=False,
    default=False,
    help="Retry tasks with the status error",
)
@click.option(
    "--concurrency",
    type=int,
    required=False,
    default=None,
    help="Override concurrency level (e.g., 20)",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs (e.g., astropy__astropy-12907,astropy__astropy-12908)",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line",
)
@click.option(
    "--secret",
    "-s",
    "secrets",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Secret as ENV_VAR aws_secret_name (e.g., -s ANTHROPIC_API_KEY devEvalInfraAnthropicKey)",
)
@click.option(
    "--update-agent",
    "-u",
    is_flag=True,
    default=False,
    help="Refresh the frozen agent copy from the current agents/<name>.zip in S3 before resuming.",
)
@click.option(
    "--from-scratch",
    is_flag=True,
    default=False,
    help="Clear durable eval state and rerun generation.",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after resuming",
)
@click.pass_context
def resume(
    ctx: click.Context,
    run_id: UUID,
    retry: bool,
    concurrency: int | None,
    task_ids: str | None,
    task_ids_file: str | None,
    secrets: tuple[tuple[str, str]],
    update_agent: bool,
    from_scratch: bool,
    connect: bool,
):
    """
    Resume a run by its run id.

    Example:
        valkyrie run resume 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20
    """
    retry_task_ids = resolve_task_ids(task_ids, task_ids_file) or []

    # NOTE: workaround for auto retrying tasks when using the retry command
    if ctx.info_name == "retry":
        retry = True

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            # Resolve service headers from config using the benchmark name
            benchmark_info = tracker.fetch_benchmark(run_id)
            service_headers: dict[str, str] = {}
            auth_credential = TrackerService.get_benchmark_auth(benchmark_info.benchmark_name)
            if auth_credential:
                service_headers["Authorization"] = str(auth_credential)

            if update_agent:
                metadata = tracker.fetch_benchmark_metadata(run_id)
                agent_name = metadata.benchmark_arguments.contract.name
                click.echo(f"\r\033[KUpdating agent '{agent_name}'...", nl=False)
                asyncio.run(update_benchmark_agent_version(agent_name, str(run_id)))
                click.echo(click.style("\r\033[K✓ Agent updated", fg="green"))

            _ = tracker.retry_or_resume_benchmark(
                run_id,
                retry,
                RetryMode.FROM_SCRATCH if from_scratch else RetryMode.AUTO,
                concurrency,
                retry_task_ids,
                service_headers=service_headers,
                secrets={key: value for key, value in secrets},
            )
            action_label = "retried" if retry else "resumed"
            click.echo(click.style(f"✓ Run {action_label} successfully!", fg="green", bold=True))
            click.echo("┌─ Next Steps " + "─" * 66)
            if not connect:
                click.echo(
                    f"│ {'Track progress:':<17} " + click.style(f"valkyrie run fetch {run_id} --connect", fg="cyan")
                )
            click.echo(
                f"│ {'Get results:':<17} "
                + click.style(f"valkyrie run results {run_id} --path ./results-{run_id}.json", fg="cyan")
            )
            click.echo("└" + "─" * 79)
            if connect:
                stream_benchmark_status(tracker, run_id)
    except (TrackerServiceError, S3Error) as e:
        raise click.ClickException(str(e))


# Alias for run resume, the logic is the same under the hood
retry_command = click.Command(
    name="retry",
    callback=resume.callback,
    params=resume.params,
    help="Retry a run by its run id. \n\nExample:\nvalkyrie run retry 123e4567-e89b-12d3-a456-426614174000 --concurrency 20",
    short_help="Retry a run by its run id.",
)
run.add_command(retry_command)


@run.command(
    name="list",
    help="List runs by providing filter values. \n\nExample:\nvalkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC",
)
@click.option(
    "--agent-name",
    type=str,
    required=False,
    help="Agent name (e.g., claude_code)",
)
@click.option(
    "--benchmark-name",
    type=str,
    required=False,
    help="Benchmark name (e.g., swebench)",
)
@click.option(
    "--model",
    type=str,
    required=False,
    help="Model name (e.g., anthropic/claude-sonnet-4-20250514)",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    help="Dataset name (e.g., default, terminal-bench-2.1)",
)
@click.option(
    "--label",
    "-l",
    type=str,
    required=False,
    help="Run label",
)
@click.option(
    "--status",
    type=click.Choice([option.value for option in BenchmarkStatus], case_sensitive=False),
    required=False,
    default=None,
    help="Status of the benchmarks to fetch (e.g., in_progress, finished, error)",
)
@click.option(
    "--order-by",
    type=click.Choice([option.value for option in Order], case_sensitive=False),
    required=False,
    default=Order.DESC.value,
    help="Order by the benchmarks to fetch (e.g., desc, asc)",
)
@click.option(
    "--started-by",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of starter emails (e.g., alice@vals.ai,bob@vals.ai). Case-insensitive.",
)
def list_benchmarks(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str = "desc",
    started_by: str | None = None,
):
    """
    List runs based on the request parameters.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC
    """
    started_by_list: list[str] = [s.strip() for s in started_by.split(",") if s.strip()] if started_by else []

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            paginate_benchmarks(
                tracker,
                agent_name,
                benchmark_name,
                model,
                dataset,
                label,
                status,
                order_by,
                started_by=started_by_list or None,
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    name="outputs",
    help="Fetch run outputs by run id. \n\nExample:\nvalkyrie run outputs 123e4567-e89b-12d3-a456-426614174000 --output-dir ./run_outputs",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save run outputs (defaults to ./<benchmark>_<agent>_<run-id>)",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs to download (e.g., astropy__astropy-7606,django__django-10880)",
)
def outputs(run_id: UUID, output_dir: Path | None, task_ids: str | None):
    """
    Fetch run outputs for a benchmark by its run id.

    Example:
        valkyrie run outputs 123e4567-e89b-12d3-a456-426614174000
        valkyrie run outputs 123e4567-e89b-12d3-a456-426614174000 --task-ids astropy__astropy-7606,django__django-10880
    """
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            metadata = tracker.fetch_benchmark_metadata(run_id)

            if output_dir is None:
                output_dir = Path(
                    f"{metadata.benchmark_name}_{metadata.benchmark_arguments.contract.name}_{metadata.benchmark_id}"
                )

            click.echo(f"\r\033[KFetching run outputs for run {run_id}...", nl=False)

            response = tracker.fetch_run_outputs(
                run_id,
                task_ids=resolve_task_ids(task_ids),
            )

            download_run_outputs(response, output_dir)

            click.echo(click.style(f"\r\033[K✓ Run outputs extracted to: {output_dir}", fg="green"))

    except TrackerServiceError as e:
        click.echo(click.style(f"✗ Error: {e}", fg="red"), err=True)
        raise click.Abort()


@run.command(name="output", help="Download files from a benchmark run by its ID.")
@click.argument("benchmark_id", type=UUID)
@click.argument("subpath", type=str, default="", required=False)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save downloaded files (defaults to ./<benchmark_id>)",
)
def output_path(benchmark_id: UUID, subpath: str, output_dir: Path | None):
    """
    Download all files under a benchmark's S3 directory.

    Example:
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 astropy__astropy-7606
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 swebench.json -o .
    """
    try:
        path = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}"
        if subpath:
            path = f"{path}/{subpath.strip('/')}"

        if output_dir is None:
            output_dir = Path(str(benchmark_id))

        click.echo(f"\r\033[KDownloading from s3://{path}...", nl=False)
        count = asyncio.run(download_s3_path(path, output_dir))
        click.echo(click.style(f"\r\033[K✓ {count} file(s) downloaded to: {output_dir}", fg="green"))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e}", fg="red"), err=True)
        raise click.Abort()


@agent.command(name="install", help="Installs agent from a github project to the users aws environment")
@click.argument("github_url", type=str)
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to repository name or subfolder name)",
)
def install(github_url: str, name: str | None):
    """Install an agent from a GitHub repository or a subfolder within a repository.

    Supports both full repository URLs and subfolder paths using GitHub's tree syntax.

    Example:
        valkyrie agent install https://github.com/user/my-agent
        valkyrie agent install https://github.com/user/my-agent --name my-custom-name
        valkyrie agent install https://github.com/org/registry/tree/main/agents/codex
        valkyrie agent install https://github.com/org/registry/tree/main/agents/codex --name my-agent
    """
    try:
        resolved_name = asyncio.run(install_agent(name, github_url))
        click.echo(click.style(f"✓ Agent '{resolved_name}' installed successfully!", fg="green", bold=True))
    except (RuntimeError, S3Error) as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@agent.command(name="push", help="Pushes agent to the users aws environment from the local filesystem")
@click.argument("agent_path", type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True))
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to path stem)",
)
def push(agent_path: Path, name: str | None):
    """Push a local agent to S3.

    Example:
        valkyrie agent push ./agents/my-agent
        valkyrie agent push ./agents/my-agent --name my-agent
    """
    try:
        agent_name = name or agent_path.stem
        asyncio.run(push_agent(agent_name, agent_path))
        click.echo(click.style(f"✓ Agent '{agent_name}' pushed successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@agent.command(name="remove", help="Remove an installed agent")
@click.argument("agent_name", type=str)
def agent_remove(agent_name: str):
    """Remove an agent from S3.

    Example:
        valkyrie agent remove my-agent
    """
    try:
        if not click.confirm(f"Are you sure you want to remove agent '{agent_name}'?"):
            click.echo("Cancelled.")
            return

        asyncio.run(remove_agent(agent_name))
        click.echo(click.style(f"✓ Agent '{agent_name}' removed successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@agent.command(name="download", help="Download an installed agent")
@click.argument("agent_name", type=str)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=None,
    required=False,
    help="Output directory for downloaded agent (default: current directory)",
)
def download(agent_name: str, output_dir: Path | None):
    """Download an agent from S3.

    Example:
        valkyrie agent download my-agent
    """
    try:
        asyncio.run(download_agent(agent_name, output_dir))
        click.echo(click.style(f"✓ Agent '{agent_name}' downloaded successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@agent.command(name="list", help="List installed agents")
def list_installed_agents():
    """List all installed agents in S3.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie agent list
    """
    try:
        agents = asyncio.run(list_agents())

        if not agents:
            click.echo(click.style("\r\033[KNo agents found.", fg="yellow"))
            return

        paginate_agents(agents)
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    cli()
