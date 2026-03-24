"""CLI views/commands for Valkyrie."""

import asyncio
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import click
import yaml
from tracker.database.models import BenchmarkStatus
from tracker.exceptions import S3Error
from tracker.types import FinalViewResponse, Order, RetrieveResultsResponse, StartBenchmarkResponse

from valkyrie.cli.bundler import get_contract
from valkyrie.cli.exceptions import BundlerError, TrackerServiceError
from valkyrie.cli.s3_client import (
    download_agent,
    get_contract_from_s3,
    install_agent,
    list_agents,
    push_agent,
    remove_agent,
)
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import (
    CONFIG_LOCATION,
    check_tracker_service_health,
    download_agent_outputs,
    download_final_view,
    format_benchmark_status,
    format_start_benchmark_response,
    paginate_agents,
    paginate_benchmarks,
    paginate_services,
    stream_benchmark_status,
)
from valkyrie.schemas import AgentConfig


@click.group()
def cli():
    """Valkyrie CLI."""
    pass


@cli.group()
def run():
    """Run command group"""
    pass


@cli.group()
def agent():
    """Agent command group"""
    pass


@cli.group()
def config():
    """Config command group"""
    pass


@config.command()
def init() -> None:
    """
    Initializes a config we can trust to have references to dependencies to run Valkyrie,
    this becomes our source of truth for secrets required to run Valkyrie
    """

    # Mapping between the expected key and default value
    # None means its user provided if not found
    environment_variables: dict[str, str | None | int] = {
        "AWS_ACCESS_KEY_ID": None,  # AWS ACCESS KEY
        "AWS_SECRET_ACCESS_KEY": None,  # AWS SECRETS KEY
        "AWS_DEFAULT_REGION": None,  # What region your secrets are in
        "S3_BUCKET": None,  # Center point where all agents and benchmark results are uploaded
        "DAYTONA_SECRET_NAME": None,  # AWS Secrets Manager name for Daytona credentials
        "LOG_GROUP": "benchmarks",  # the prefix to the cloudwatch logs (e.x. benchmarks/<benchmark_id>)
        "LOG_RETENTION_POLICY": 365,  # How long logs are kept until auto deleted
    }

    current_config: dict[str, str] = {}
    if CONFIG_LOCATION.exists():
        with open(CONFIG_LOCATION) as f:
            try:
                current_config = yaml.safe_load(f)
            except Exception:
                pass

    collected_keys: dict[str, str] = {}
    for key, default in environment_variables.items():
        sourced = current_config.get(key) or os.environ.get(key)
        if sourced:
            click.echo(
                f"  {key}: sourced from {'environment' if not current_config.get(key) else 'already created config'}"
            )
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

    CONFIG_LOCATION.parent.mkdir(parents=True, exist_ok=True)

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(collected_keys, f, default_flow_style=False)

    click.echo(click.style(f"\nConfig written to {CONFIG_LOCATION}", fg="green", bold=True))


@config.command()
@click.argument("key")
@click.argument("value")
def modify(key: str, value: str) -> None:
    """
    Modify a single key in the Valkyrie config.

    Example: valkyrie config modify AWS_DEFAULT_REGION us-west-2
    """

    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, str] = yaml.safe_load(f) or {}

    if key not in current:
        raise click.ClickException(f"Key '{key}' not found in config. Valid keys: {', '.join(current)}")

    current[key] = value

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"  {key} updated.", fg="green"))


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

    click.echo(click.style(f"Service '{name}' has been set set", fg="green"))


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
    """List all custom benchmark service URL overrides."""
    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `valkyrie config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, Any] = yaml.safe_load(f) or {}

    services: dict[str, str] = current.get("custom_benchmark_services") or {}
    if not services:
        click.echo(click.style("No custom service URLs configured.", fg="yellow"))
        return

    # Create a table of all the services that the user has inside of their config
    services_list = list(services.items())
    paginate_services(services_list)


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

    for name, credential in auth_credentials.items():
        masked = credential[:4] + "***" if len(credential) > 4 else "***"
        click.echo(f"  {name}: {masked}")


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
    type=click.Path(exists=True, path_type=Path, file_okay=True, dir_okay=False),
    required=False,
    default=None,
    help="Path to a text file with one task ID per line",
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
def start(
    agent: str,
    model: str | None,
    benchmark: str,
    concurrency: int,
    lambda_function: str | None,
    task_ids: str | None,
    task_ids_file: Path | None,
    slice_str: str | None,
    dataset: str | None,
    kwargs: tuple[tuple[str, str]],
    secrets: tuple[tuple[str, str]],
    headers: tuple[tuple[str, str]],
):
    """
    Run an agent on a benchmark.

    Example:
        valkyrie run start --agent agents/claude_code --benchmark swebench
    """
    if task_ids and task_ids_file:
        raise click.UsageError("--task-ids and --task-ids-file are mutually exclusive")

    if task_ids_file:
        lines = task_ids_file.read_text().splitlines()
        task_ids = ",".join(line.strip() for line in lines if line.strip())

    click.echo("Arguments:")
    click.echo(f"  - Benchmark: {benchmark}")
    click.echo(f"  - Agent: {agent}")
    if model:
        click.echo(f"  - Model: {model}")
    click.echo(f"  - Concurrency: {concurrency}")
    click.echo(f"  - Slice: {slice_str}")
    if dataset:
        click.echo(f"  - Dataset: {dataset}")
    if task_ids:
        click.echo(f"  - Task IDs: {task_ids[:100]}{'...' if len(task_ids) > 100 else ''}")
    else:
        click.echo("  - Task IDs: all tasks")

    service_headers: dict[str, str] = {}
    auth_credential = TrackerService.get_benchmark_auth(benchmark)
    if auth_credential:
        service_headers["Authorization"] = str(auth_credential)
    for name, value in headers:
        service_headers[name] = value

    if service_headers:
        click.echo(f"  - Service headers: {', '.join(service_headers.keys())}")

    formatted_task_ids: list[str] | None = None
    if task_ids:
        formatted_task_ids = task_ids.split(",")
        click.echo(f"Discovered {len(formatted_task_ids)} task IDs")

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
            contract = get_contract(agent_path / "contract.py", agent_config)
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
                formatted_task_ids,
                slice_str,
                lambda_function,
                dataset,
                service_headers=service_headers or None,
            )

            click.echo("\r\033[K", nl=False)
            if response.status_code != 200:
                click.echo(click.style("Run failed to start!", fg="red", bold=True))
                click.echo(response.text)
                return

            format_start_benchmark_response(StartBenchmarkResponse.model_validate(response.json()))
    except (BundlerError, TrackerServiceError) as e:
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
                click.echo(f"Fetching run: {run_id}")
                response = tracker.fetch_benchmark(run_id)
                format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@run.command(
    name="results",
    help="Retrieve run results by its run id. \n\nExample:\nvalkyrie run results 123e4567-e89b-12d3-a456-426614174000 --path ./results.json",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    default=None,
    required=False,
    help="Path to save the results (default: ./<benchmark>.json)",
)
@click.option(
    "--s3",
    is_flag=True,
    default=False,
    required=False,
    help="Saves results to s3 instead of downloading them locally. Can be found at bucket://benchmarks/run_id/<benchmark>.json",
)
def results(run_id: UUID, path: Path | None, s3: bool):
    """
    Retrieve the results of a run by its run id.

    Example:
        valkyrie run results e532551e-d51b-4912-983d-47695bd24174 --path ./results.json
    """
    click.echo(f"Retrieving results for run: {run_id}")

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if s3:
                if tracker.check_results_exist_in_s3(run_id):
                    if not click.confirm("Results already exist in S3. Overwrite?"):
                        raise click.Abort()

            results_response: RetrieveResultsResponse = tracker.retrieve_results(run_id, s3)

            if isinstance(results_response, FinalViewResponse):
                default_path: Path = Path(f"./{results_response.benchmark_name}.json")

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
    click.echo(f"Stopping run: {run_id}")

    if force:
        click.echo(click.style("Force stopping the run", fg="yellow", bold=True))
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            _ = tracker.stop_benchmark(run_id, force)

            if not force:
                click.echo(
                    click.style(
                        "Run is currently being stopped. Will be stopped when all tasks in flight are finished.",
                        fg="yellow",
                        bold=True,
                    )
                )
            click.echo(
                click.style(
                    f"Retrieve results: valkyrie run results {run_id} --path ./results.json",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


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
    type=click.Path(exists=True, path_type=Path, file_okay=True, dir_okay=False),
    required=False,
    default=None,
    help="Path to a text file with one task ID per line",
)
@click.pass_context
def resume(
    ctx: click.Context,
    run_id: UUID,
    retry: bool,
    concurrency: int | None,
    task_ids: str | None,
    task_ids_file: Path | None,
):
    """
    Resume a run by its run id.

    Example:
        valkyrie run resume 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20
    """
    if task_ids and task_ids_file:
        raise click.UsageError("--task-ids and --task-ids-file are mutually exclusive")

    if task_ids_file:
        lines = task_ids_file.read_text().splitlines()
        task_ids = ",".join(line.strip() for line in lines if line.strip())

    # NOTE: workaround for auto retrying tasks when using the retry command
    if ctx.info_name == "retry":
        retry = True

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            retry_task_ids = task_ids.split(",") if task_ids else []
            _ = tracker.retry_or_resume_benchmark(run_id, retry, concurrency, retry_task_ids)
            click.echo(click.style("Run continued successfully!", fg="green", bold=True))
            click.echo(
                click.style(
                    f"Track progress: valkyrie run fetch {run_id} --connect",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
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
def list_benchmarks(
    agent_name: str | None,
    benchmark_name: str | None,
    status: str | None,
    order_by: str = "desc",
):
    """
    List runs based on the request parameters.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC
    """
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            paginate_benchmarks(tracker, agent_name, benchmark_name, status, order_by)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@agent.command(
    name="outputs",
    help="Fetch agent outputs by run id. \n\nExample:\nvalkyrie agent outputs 123e4567-e89b-12d3-a456-426614174000 --output-dir ./agent_outputs",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save agent outputs (defaults to ./agent_outputs/<run-id>)",
)
def outputs(run_id: UUID, output_dir: Path | None):
    """
    Fetch agent outputs for a benchmark by its run id.

    Example:
        valkyrie agent outputs 123e4567-e89b-12d3-a456-426614174000
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

            click.echo(f"\r\033[KFetching agent outputs for run {run_id}...", nl=False)

            response = tracker.fetch_agent_outputs(run_id)

            download_agent_outputs(response, output_dir)

            click.echo(click.style(f"\r\033[K✓ Agent outputs extracted to: {output_dir}", fg="green"))

    except TrackerServiceError as e:
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
        asyncio.run(install_agent(name, github_url))
        click.echo(
            click.style(f"✓ Agent {'(' + name + ') ' if name else ''}installed successfully!", fg="green", bold=True)
        )
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
        asyncio.run(push_agent(name, agent_path))
        click.echo(click.style(f"✓ Agent '{agent_name}' pushed successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@agent.command(name="remove", help="Remove an installed agent")
@click.argument("agent_name", type=str)
def remove(agent_name: str):
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
        if not click.confirm(f"Are you sure you want to download agent '{agent_name}'?"):
            click.echo("Cancelled.")
            return

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
