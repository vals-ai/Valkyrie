import asyncio
from pathlib import Path
from typing import Any

import click
from tracker.types import StartBenchmarkResponse

from valkyrie.cli.bundler import get_contract
from valkyrie.cli.exceptions import BundlerError, ContractValidationError, TrackerServiceError
from valkyrie.cli.s3_client import get_contract_from_s3, push_agent
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import (
    check_tracker_service_health,
    format_agent_start_details,
    format_run_start_details,
    format_start_benchmark_response,
    resolve_task_ids,
    resolve_webhook_config,
    stream_benchmark_status,
)
from valkyrie.schemas import AgentConfig


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
