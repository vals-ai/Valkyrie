import asyncio
from pathlib import Path
from uuid import UUID

import click
from tracker.agent.contract import get_contract
from tracker.agent.schemas import AgentConfig
from tracker.database.models import AgentContractRequest
from tracker.types import StartBenchmarkResponse

from valkyrie.sdk.errors import ValkyrieSDKError

from valkyrie.cli.exceptions import BundlerError, ContractValidationError, TrackerServiceError
from valkyrie.cli.run.progress import stream_benchmark_status
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.agent.storage import push_agent_if_absent
from valkyrie.cli.display import local_time
from valkyrie.cli.tracker_client import TrackerService, response_error_detail


BOX_WIDTH = 80
COLUMN_WIDTH = 14
RUN_COLUMN_WIDTH = 17
_UNKNOWN_START_OUTCOME = "The latest start request's outcome may be unknown. Verify with `valkyrie run list`."
_CLEAR_LINE = "\r\033[K"


def row(label: str, value: str, width: int = COLUMN_WIDTH) -> str:
    return f"│ {label:<{width}} {value}"


def format_run_start_details(
    benchmark: str,
    dataset: str | None,
    concurrency: int,
    slice_str: str | None,
    task_ids: str | None,
    priority: int | None,
) -> None:
    """Format and display the start details of a run."""
    click.echo("┌─ Benchmark ".ljust(BOX_WIDTH, "─"))
    click.echo(row("Name:", benchmark))
    click.echo(row("Dataset:", dataset or "default"))
    click.echo(row("Concurrency:", str(concurrency)))
    if priority is not None:
        click.echo(row("Priority override:", str(priority)))
    if slice_str:
        click.echo(row("Slice:", slice_str))
    click.echo(
        row("Task IDs:", (task_ids[:60] + "..." if len(task_ids) > 60 else task_ids) if task_ids else "all tasks")
    )
    click.echo("└".ljust(BOX_WIDTH, "─"))
    click.echo()


def format_agent_start_details(
    agent: str,
    model: str | None,
    secrets: tuple[tuple[str, str], ...],
    kwargs: tuple[tuple[str, str], ...],
    service_headers: dict[str, str],
) -> None:
    """Format and display the start details of an agent."""
    click.echo("┌─ Agent ".ljust(BOX_WIDTH, "─"))
    click.echo(row("Agent:", agent))
    if model:
        click.echo(row("Model:", model))
    for index, (env_var, secret_name) in enumerate(secrets):
        click.echo(row("Secrets:" if index == 0 else "", f"{env_var} → {secret_name}"))
    if kwargs:
        click.echo(row("Kwargs:", ", ".join(f"{key}={value}" for key, value in kwargs)))
    if service_headers:
        click.echo(row("Headers:", ", ".join(service_headers)))
    click.echo("└".ljust(BOX_WIDTH, "─"))
    click.echo()


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse, connect: bool = False) -> None:
    """Format and display the start run response."""
    run_id = start_benchmark_response.benchmark_id
    version = start_benchmark_response.dataset_version
    details = [
        ("Benchmark:", start_benchmark_response.benchmark_name),
        ("Agent:", start_benchmark_response.agent_name),
        ("Run ID:", run_id),
        ("Started at:", local_time(start_benchmark_response.started_at)),
        ("Max concurrency:", start_benchmark_response.concurrency),
        ("Total tasks:", start_benchmark_response.task_count),
        ("Dataset version:", version.label or version.id if version else "not pinned"),
    ]
    commands = [
        ("Get results:", f"valkyrie run results {run_id} --path ./results-{run_id}.json"),
        ("Stop:", f"valkyrie run stop {run_id}"),
        ("Resume:", f"valkyrie run resume {run_id}"),
        ("Retry:", f"valkyrie run retry {run_id}"),
        ("Run outputs:", f"valkyrie run outputs {run_id} --output-dir ."),
    ]
    if not connect:
        commands.insert(0, ("Track progress:", f"valkyrie run fetch {run_id} --connect"))

    click.echo()
    click.echo("┌─ Run Details ".ljust(BOX_WIDTH, "─"))
    for label, value in details:
        click.echo(row(label, str(value), RUN_COLUMN_WIDTH))
    if start_benchmark_response.dataset_version_warning:
        click.secho(start_benchmark_response.dataset_version_warning, fg="yellow")
    click.echo(row("Logs:", start_benchmark_response.cloudwatch_url, RUN_COLUMN_WIDTH))
    click.echo(row("Artifacts:", start_benchmark_response.s3_bucket_url, RUN_COLUMN_WIDTH))
    click.echo("├".ljust(BOX_WIDTH, "─"))
    for label, command in commands:
        click.echo(row(label, click.style(command, fg="cyan"), RUN_COLUMN_WIDTH))
    click.echo("└".ljust(BOX_WIDTH, "─"))


def format_confirmed_start_summary(run_ids: list[UUID], count: int, outcome_unknown: bool = False) -> None:
    """Display confirmed run IDs, their combined status command, and any unknown start outcome."""
    if count > 1:
        click.echo()
        click.echo(f"{len(run_ids)} / {count} requested runs successfully started.")
        if run_ids:
            click.echo(f"Track progress: valkyrie run status --ids {','.join(str(run_id) for run_id in run_ids)}")
    if outcome_unknown:
        click.echo(click.style(_UNKNOWN_START_OUTCOME, fg="yellow"))


@click.command(
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
    type=click.IntRange(min=1),
    default=5,
    required=False,
    help="Maximum concurrent tasks; defaults to 5",
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
    help="Dataset slice in Python slice syntax start:stop[:step] (e.g., 0:10)",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Dataset name to use from the benchmark service (defaults to 'default')",
)
@click.option("--dataset-version", type=str, default=None, help="Fixed dataset version to use for this run")
@click.option(
    "--provider",
    type=str,
    required=False,
    default=None,
    help="Sandbox provider (e.g., daytona, modal); defaults to your default provider, then daytona",
)
@click.option(
    "--priority",
    type=click.IntRange(0, 4),
    required=False,
    default=None,
    help="Override configured queue priority: 0 (highest) through 4 (lowest); default is 3",
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
    help="Secret as ENV_VAR aws_secret_name (e.g., -s ANTHROPIC_API_KEY YourAnthropicKeySecret)",
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
    help="Use the hosted benchmark service instead of the URL saved with `valkyrie benchmark service set`.",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after starting",
)
@click.option(
    "--count",
    "-n",
    type=click.IntRange(1, 10),
    default=1,
    show_default=True,
    help="Number of independent runs to start",
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
    dataset_version: str | None,
    provider: str | None,
    priority: int | None,
    label: str | None,
    kwargs: tuple[tuple[str, str]],
    secrets: tuple[tuple[str, str]],
    headers: tuple[tuple[str, str]],
    ignore_custom_services: bool,
    connect: bool,
    count: int,
) -> None:
    """
    Run an agent on a benchmark.

    Example:
        valkyrie run start --agent agents/claude_code --benchmark swebench
    """
    if count > 1 and connect:
        raise click.UsageError("--connect cannot be used when --count is greater than 1")

    formatted_task_ids = resolve_task_ids(task_ids, task_ids_file)

    service_headers = dict(headers)

    task_ids_display = ",".join(formatted_task_ids) if formatted_task_ids else None
    format_run_start_details(benchmark, dataset, concurrency, slice_str, task_ids_display, priority)

    format_agent_start_details(agent, model, secrets, kwargs, service_headers)

    try:
        agent_path = Path(agent)
        if agent_path.is_dir():
            candidates = (agent_path / "contract.yaml", agent_path / "contract.yml")
            contract_file = next(filter(Path.exists, candidates), candidates[0])
            local_contract = get_contract(contract_file, AgentConfig(model=model, kwargs=dict(kwargs)))
            if not asyncio.run(push_agent_if_absent(local_contract.name, agent_path)):
                raise click.UsageError(
                    f"A published agent named '{local_contract.name}' already exists. "
                    "Refusing to overwrite it as a side effect of run start. "
                    f"Use --agent {local_contract.name} to run the published release, "
                    "or explicitly publish the local directory under a different name first."
                )
            agent = local_contract.name

        contract = AgentContractRequest(name=agent, model=model, kwargs=dict(kwargs), secrets=dict(secrets))

        confirmed_run_ids: list[UUID] = []
        with TrackerService() as tracker:
            for _ in range(count):
                click.echo(f"{_CLEAR_LINE}Starting run for: {contract.name}...", nl=False)

                try:
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
                        dataset_version,
                        priority=priority,
                        service_headers=service_headers,
                        provider=provider,
                    )
                except TrackerServiceError as error:
                    click.echo(_CLEAR_LINE, nl=False)
                    format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=True)
                    raise click.ClickException(str(error)) from error

                click.echo(_CLEAR_LINE, nl=False)
                if response.status_code != 200:
                    click.echo(click.style("Run failed to start!", fg="red", bold=True))
                    format_confirmed_start_summary(
                        confirmed_run_ids, count, outcome_unknown=response.status_code >= 500
                    )
                    raise click.ClickException(str(response_error_detail(response)))

                try:
                    start_response = StartBenchmarkResponse.model_validate(response.json())
                except ValueError as error:
                    format_confirmed_start_summary(confirmed_run_ids, count, outcome_unknown=True)
                    raise click.ClickException("Tracker returned a malformed 200 start response.") from error

                confirmed_run_ids.append(start_response.benchmark_id)
                format_start_benchmark_response(start_response, connect)
                if connect:
                    stream_benchmark_status(tracker, start_response.benchmark_id)

        format_confirmed_start_summary(confirmed_run_ids, count)
    except (BundlerError, TrackerServiceError, ContractValidationError, ValkyrieSDKError) as e:
        raise click.ClickException(str(e))
