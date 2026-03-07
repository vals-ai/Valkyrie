"""CLI views/commands for the agentic harness."""

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

from agentic_harness.cli.bundler import get_contract
from agentic_harness.cli.exceptions import BundlerError, TrackerServiceError
from agentic_harness.cli.s3_client import (
    download_agent,
    get_contract_from_s3,
    install_agent,
    list_agents,
    push_agent,
    remove_agent,
)
from agentic_harness.cli.tracker_service import TrackerService
from agentic_harness.cli.utils import (
    CONFIG_LOCATION,
    check_tracker_service_health,
    download_agent_outputs,
    download_final_view,
    format_benchmark_status,
    format_start_benchmark_response,
    paginate_agents,
    paginate_benchmarks,
    stream_benchmark_status,
)
from agentic_harness.schemas import AgentConfig


@click.group()
def cli():
    """Agentic Harness CLI."""
    pass


@cli.group()
def benchmark():
    """Benchmark command group"""
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
    Initializes a config we can trust to have references to dependencies to run the harness,
    this becomes our source of truth for secrets required to run the harness
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
    Modify a single key in the harness config.

    Example: harness config modify AWS_DEFAULT_REGION us-west-2
    """

    if not CONFIG_LOCATION.exists():
        raise click.ClickException("Config not found. Run `harness config init` first.")

    with open(CONFIG_LOCATION) as f:
        current: dict[str, str] = yaml.safe_load(f) or {}

    if key not in current:
        raise click.ClickException(f"Key '{key}' not found in config. Valid keys: {', '.join(current)}")

    current[key] = value

    with open(CONFIG_LOCATION, "w") as f:
        yaml.dump(current, f, default_flow_style=False)

    click.echo(click.style(f"  {key} updated.", fg="green"))


@benchmark.command(
    help="Start a benchmark run. \n\nExample:\nharness benchmark start --agent agents/claude_code --benchmark swebench --concurrency 5"
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
def start(
    agent: str,
    model: str | None,
    benchmark: str,
    concurrency: int,
    lambda_function: str | None,
    task_ids: str | None,
    task_ids_file: Path | None,
    slice_str: str | None,
    kwargs: tuple[tuple[str, str]],
    secrets: tuple[tuple[str, str]],
):
    """
    Run an agent on a benchmark.

    Example:
        harness run --agent agents/claude_code --benchmark swebench
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
    if task_ids:
        click.echo(f"  - Task IDs: {task_ids[:100]}{'...' if len(task_ids) > 100 else ''}")
    else:
        click.echo("  - Task IDs: all tasks")

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
        else:
            contract = asyncio.run(get_contract_from_s3(agent, agent_config))

        # Merge CLI secrets into contract defaults (override with cli secret)
        if secrets:
            contract.secrets.update({key: value for key, value in secrets})

        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            click.echo(f"\r\033[KStarting benchmark for: {contract.name}...", nl=False)

            response = tracker.start_benchmark(
                contract,
                benchmark,
                concurrency,
                formatted_task_ids,
                slice_str,
                lambda_function,
            )

            click.echo("\r\033[K", nl=False)
            if response.status_code != 200:
                click.echo(click.style("Benchmark failed to start!", fg="red", bold=True))
                click.echo(response.text)
                return

            format_start_benchmark_response(StartBenchmarkResponse.model_validate(response.json()))
    except (BundlerError, TrackerServiceError) as e:
        raise click.ClickException(str(e))


@benchmark.command(
    help="Fetch a benchmark by its benchmark id. \n\nExample:\nharness benchmark fetch --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --connect"
)
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream benchmark updates",
)
def fetch(benchmark_id: UUID, connect: bool):
    """
    Fetch a benchmark by its benchmark id.

    Example:
        harness benchmark fetch --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --connect
    """

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if connect:
                stream_benchmark_status(tracker, benchmark_id)
            else:
                click.echo(f"Fetching benchmark: {benchmark_id}")
                response = tracker.fetch_benchmark(benchmark_id)
                format_benchmark_status(response)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@benchmark.command(
    name="results",
    help="Retrieve benchmark results by its benchmark id. \n\nExample:\nharness benchmark results --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --path ./results.json",
)
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
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
    help="Saves results to s3 instead of downloading them locally. Can be found at bucket://benchmarks/benchmark_id/<benchmark>.json",
)
def results(benchmark_id: UUID, path: Path | None, s3: bool):
    """
    Retrieve the results of a benchmark by its benchmark id.

    Example:
        harness benchmark results --benchmark-id e532551e-d51b-4912-983d-47695bd24174 --path ./results.json
    """
    click.echo(f"Retrieving results for benchmark: {benchmark_id}")

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            if s3:
                if tracker.check_results_exist_in_s3(benchmark_id):
                    if not click.confirm("Results already exist in S3. Overwrite?"):
                        raise click.Abort()

            results_response: RetrieveResultsResponse = tracker.retrieve_results(benchmark_id, s3)

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


@benchmark.command(
    help="Stop a benchmark run by its benchmark id. \n\nExample:\nharness benchmark stop --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --force"
)
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
@click.option(
    "--force",
    is_flag=True,
    required=False,
    default=False,
    help="Force stop the benchmark run",
)
def stop(benchmark_id: UUID, force: bool):
    """
    Stop a benchmark by its benchmark id.

    Example:
        harness benchmark stop --benchmark-id 123e4567-e89b-12d3-a456-426614174000
    """
    click.echo(f"Stopping benchmark for benchmark: {benchmark_id}")

    if force:
        click.echo(click.style("Force stopping the benchmark", fg="yellow", bold=True))
    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            _ = tracker.stop_benchmark(benchmark_id, force)

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
                    f"Retrieve results: harness benchmark results --benchmark-id {benchmark_id} --path ./results.json",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


@benchmark.command(
    help="Resume a benchmark run by its benchmark id. \n\nExample:\nharness benchmark resume --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20"
)
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
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
    benchmark_id: UUID,
    retry: bool,
    concurrency: int | None,
    task_ids: str | None,
    task_ids_file: Path | None,
):
    """
    Resume a benchmark run by its benchmark id.

    Example:
        harness benchmark resume --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --retry --concurrency 20
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
            _ = tracker.retry_or_resume_benchmark(benchmark_id, retry, concurrency, retry_task_ids)
            click.echo(click.style("Run continued successfully!", fg="green", bold=True))
            click.echo(
                click.style(
                    f"Track progress: harness benchmark fetch --benchmark-id {benchmark_id} --connect",
                    fg="cyan",
                )
            )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


# Alias for benchmark resume, the logic is the same under the hood
retry_command = click.Command(
    name="retry",
    callback=resume.callback,
    params=resume.params,
    help="Retry a benchmark run by its benchmark id. \n\nExample:\nharness benchmark retry --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --concurrency 20",
    short_help="Retry a benchmark run by its benchmark id.",
)
benchmark.add_command(retry_command)


@benchmark.command(
    name="list",
    help="List benchmarks by providing filter values. \n\nExample:\nharness benchmark list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC",
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
    List benchmarks based on the request parameters.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        harness benchmark list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC
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
    help="Fetch agent outputs by benchmark id. \n\nExample:\nharness agent outputs --benchmark-id 123e4567-e89b-12d3-a456-426614174000 --output-dir ./agent_outputs",
)
@click.option(
    "--benchmark-id",
    type=UUID,
    required=True,
    help="Benchmark id (e.g., 123e4567-e89b-12d3-a456-426614174000)",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save agent outputs (defaults to ./agent_outputs/<benchmark-id>)",
)
def outputs(benchmark_id: UUID, output_dir: Path | None):
    """
    Fetch agent outputs for a benchmark by its benchmark id.

    Example:
        harness agent outputs --benchmark-id 123e4567-e89b-12d3-a456-426614174000
    """

    try:
        with TrackerService() as tracker:
            if not check_tracker_service_health(tracker):
                return

            metadata = tracker.fetch_benchmark_metadata(benchmark_id)

            if output_dir is None:
                output_dir = Path(
                    f"{metadata.benchmark_name}_{metadata.benchmark_arguments.contract.name}_{metadata.benchmark_id}"
                )

            click.echo(f"\r\033[KFetching agent outputs for benchmark {benchmark_id}...", nl=False)

            response = tracker.fetch_agent_outputs(benchmark_id)

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
    help="Agent name (defaults to repository name)",
)
def install(github_url: str, name: str | None):
    """Install an agent from a GitHub repository.

    Example:
        harness agent install https://github.com/user/my-agent
        harness agent install https://github.com/user/my-agent --name my-custom-name
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
        harness agent push ./agents/my-agent
        harness agent push ./agents/my-agent --name my-agent
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
        harness agent remove my-agent
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
        harness agent download my-agent
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
        harness agent list
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
