import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

import click
from tracker.agent.contract import get_contract
from tracker.agent.schemas import AgentConfig
from tracker.types import StartBenchmarkResponse

from valkyrie.cli.exceptions import BundlerError, ContractValidationError, TrackerServiceError
from valkyrie.cli.machine_output import emit_json, json_errors, json_option, utc_isoformat
from valkyrie.cli.run.progress import stream_benchmark_status
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.agent.storage import get_contract_from_s3, push_agent
from valkyrie.cli.display import local_time
from valkyrie.cli.service_headers import benchmark_service_headers
from valkyrie.cli.tracker_client import TrackerService, response_error_detail


COLUMN_WIDTH = 14
_UNKNOWN_START_OUTCOME = "The latest start request's outcome may be unknown. Verify with `valkyrie run list`."


def row(label: str, value: str) -> str:
    return f"│ {label:<{COLUMN_WIDTH}} {value}"


def cont(value: str) -> str:
    return f"│ {' ' * COLUMN_WIDTH} {value}"


def format_run_start_details(
    benchmark: str, dataset: str | None, concurrency: int, slice_str: str | None, task_ids: str | None
) -> None:
    """Format and display the start details of a run."""
    click.echo("┌─ Benchmark " + "─" * 67)
    click.echo(row("Name:", benchmark))
    click.echo(row("Dataset:", dataset or "default"))
    click.echo(row("Concurrency:", str(concurrency)))
    if slice_str:
        click.echo(row("Slice:", slice_str))
    if task_ids:
        display = task_ids[:60] + "..." if len(task_ids) > 60 else task_ids
        click.echo(row("Task IDs:", display))
    else:
        click.echo(row("Task IDs:", "all tasks"))
    click.echo("└" + "─" * 79)
    click.echo()


def format_agent_start_details(
    agent: str,
    model: str | None,
    secrets: tuple[tuple[str, str], ...],
    kwargs: tuple[tuple[str, str], ...],
    service_headers: dict[str, str],
    webhook_secret: str | None,
    webhook_intervals: list[int] | None,
) -> None:
    """Format and display the start details of an agent."""
    click.echo("┌─ Agent " + "─" * 71)
    click.echo(row("Agent:", agent))
    if model:
        click.echo(row("Model:", model))
    if secrets:
        for index, (env_var, secret_name) in enumerate(secrets):
            if index == 0:
                click.echo(row("Secrets:", f"{env_var} → {secret_name}"))
            else:
                click.echo(cont(f"{env_var} → {secret_name}"))
    if kwargs:
        click.echo(row("Kwargs:", ", ".join(f"{key}={value}" for key, value in kwargs)))
    if service_headers:
        click.echo(row("Headers:", ", ".join(service_headers.keys())))
    if webhook_secret and webhook_intervals:
        click.echo(row("Notify at:", ", ".join(f"{interval}%" for interval in webhook_intervals)))
    click.echo("└" + "─" * 79)
    click.echo()


def format_start_benchmark_response(start_benchmark_response: StartBenchmarkResponse, connect: bool = False) -> None:
    """Format and display the start run response."""
    run_id = start_benchmark_response.benchmark_id

    click.echo()
    click.echo("┌─ Run Details " + "─" * 65)
    click.echo(f"│ {'Benchmark:':<17} {start_benchmark_response.benchmark_name}")
    click.echo(f"│ {'Agent:':<17} {start_benchmark_response.agent_name}")
    click.echo(f"│ {'Run ID:':<17} {run_id}")
    click.echo(f"│ {'Started at:':<17} {local_time(start_benchmark_response.started_at)}")
    click.echo(f"│ {'Max concurrency:':<17} {start_benchmark_response.concurrency}")
    click.echo(f"│ {'Total tasks:':<17} {start_benchmark_response.task_count}")
    click.echo(f"│ {'CloudWatch:':<17} {start_benchmark_response.cloudwatch_url}")
    click.echo(f"│ {'S3 Bucket:':<17} {start_benchmark_response.s3_bucket_url}")
    click.echo("├" + "─" * 79)
    if not connect:
        click.echo(f"│ {'Track progress:':<17} " + click.style(f"valkyrie run fetch {run_id} --connect", fg="cyan"))
    click.echo(
        f"│ {'Get results:':<17} "
        + click.style(f"valkyrie run results {run_id} --path ./results-{run_id}.json", fg="cyan")
    )
    click.echo(f"│ {'Stop:':<17} " + click.style(f"valkyrie run stop {run_id}", fg="cyan"))
    click.echo(f"│ {'Resume:':<17} " + click.style(f"valkyrie run resume {run_id}", fg="cyan"))
    click.echo(f"│ {'Retry:':<17} " + click.style(f"valkyrie run retry {run_id}", fg="cyan"))
    click.echo(f"│ {'Run outputs:':<17} " + click.style(f"valkyrie run outputs {run_id} --output-dir .", fg="cyan"))
    click.echo("└" + "─" * 79)


def format_confirmed_start_summary(run_ids: list[UUID], count: int) -> None:
    """Display confirmed run IDs and their combined status command."""
    if count == 1:
        return

    click.echo()
    click.echo(f"{len(run_ids)} / {count} requested runs successfully started.")
    if not run_ids:
        return

    joined_run_ids = ",".join(str(run_id) for run_id in run_ids)
    click.echo(f"Track progress: valkyrie run status --ids {joined_run_ids}")


def validate_intervals(intervals: tuple[int, ...]) -> list[int]:
    """Validate notification interval values."""
    interval_list = list(intervals)
    if len(interval_list) > 3:
        raise click.UsageError("Maximum of 3 intervals allowed.")
    for interval in interval_list:
        if interval < 5 or interval > 100:
            raise click.UsageError(f"Interval {interval} out of range. Must be between 5 and 100.")
        if interval % 5 != 0:
            raise click.UsageError(f"Interval {interval} must be divisible by 5.")
    return interval_list


def resolve_webhook_config(
    intervals: tuple[int, ...], webhook_secret: str | None
) -> tuple[str | None, list[int] | None]:
    """Resolve webhook secret and intervals for a benchmark run."""
    if intervals and not webhook_secret:
        click.echo(
            click.style(
                "  Warning: --interval specified but no webhook secret configured. "
                "Run `valkyrie config set webhook <secret-name>` first. Ignoring intervals.",
                fg="yellow",
            ),
            err=True,
        )
        return None, None

    if intervals:
        return webhook_secret, validate_intervals(intervals)

    if webhook_secret:
        return webhook_secret, [100]

    return None, None


def build_start_run(response: StartBenchmarkResponse) -> dict[str, object]:
    """Build the allowlisted per-run view for a confirmed launch."""
    return {
        "run_id": str(response.benchmark_id),
        "benchmark_name": response.benchmark_name,
        "agent_name": response.agent_name,
        "started_at": utc_isoformat(response.started_at),
        "max_concurrency": response.concurrency,
        "total_tasks": response.task_count,
    }


def emit_start_outcome(
    responses: list[StartBenchmarkResponse],
    *,
    requested_count: int,
    attempted_count: int,
    confirmed_count: int,
    outcome: str,
    latest_request_outcome: str,
    event: str | None = None,
) -> None:
    """Emit one versioned start document.

    A ``launch`` event carries only the run it confirms so that a multi-run
    stream never repeats earlier runs; the terminal document carries no event and
    lists every confirmed run.
    """
    emit_json(
        "run_start",
        **({"event": event} if event is not None else {}),
        outcome=outcome,
        latest_request_outcome=latest_request_outcome,
        requested_count=requested_count,
        attempted_count=attempted_count,
        confirmed_count=confirmed_count,
        runs=[build_start_run(response) for response in responses],
    )


def report_start_failure(
    responses: list[StartBenchmarkResponse],
    *,
    requested_count: int,
    attempted_count: int,
    uncertain: bool,
    rejected: bool,
    json_output: bool,
) -> None:
    """Report a failed attempt while preserving any confirmed run IDs."""
    if json_output:
        emit_start_outcome(
            responses,
            requested_count=requested_count,
            attempted_count=attempted_count,
            confirmed_count=len(responses),
            outcome="partial" if responses else "uncertain" if uncertain else "failed",
            latest_request_outcome="unknown" if uncertain else "failed",
        )
        return

    click.echo("\r\033[K", nl=False, err=True)
    if rejected:
        click.echo(click.style("Run failed to start!", fg="red", bold=True), err=True)
    format_confirmed_start_summary([response.benchmark_id for response in responses], requested_count)
    if uncertain:
        click.echo(click.style(_UNKNOWN_START_OUTCOME, fg="yellow"), err=True)


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
@click.option(
    "--count",
    "-n",
    type=click.IntRange(1, 10),
    default=1,
    show_default=True,
    help="Number of independent runs to start",
)
@json_option
@json_errors
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
    count: int,
    json_output: bool,
) -> None:
    """
    Run an agent on a benchmark.

    Example:
        valkyrie run start --agent agents/claude_code --benchmark swebench
    """
    if count > 1 and connect:
        raise click.UsageError("--connect cannot be used when --count is greater than 1")

    formatted_task_ids = resolve_task_ids(task_ids, task_ids_file)

    try:
        TrackerService.validate_sandbox_provider(provider)
    except TrackerServiceError as e:
        raise click.ClickException(str(e))

    service_headers = benchmark_service_headers(benchmark, headers)

    # Webhook notification setup (may print a warning before the boxes)
    webhook_secret, webhook_intervals = resolve_webhook_config(intervals, TrackerService.get_webhook_secret())

    task_ids_display = ",".join(formatted_task_ids) if formatted_task_ids else None
    if not json_output:
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
            contract_file = next(
                (
                    agent_path / f"contract{ext}"
                    for ext in (".yaml", ".yml")
                    if (agent_path / f"contract{ext}").exists()
                ),
                agent_path / "contract.yaml",
            )
            contract = get_contract(contract_file, agent_config)
            asyncio.run(push_agent(contract.name, agent_path))
        else:
            contract = asyncio.run(get_contract_from_s3(agent, agent_config))
            contract.name = agent

        # Merge CLI secrets into contract defaults (override with cli secret)
        if secrets:
            contract.secrets.update({key: value for key, value in secrets})

        start_responses: list[StartBenchmarkResponse] = []
        with TrackerService() as tracker:
            for attempt_index in range(count):
                click.echo(f"\r\033[KStarting run for: {contract.name}...", nl=False, err=True)

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
                        service_headers=service_headers or None,
                        provider=provider,
                        webhook_secret_name=webhook_secret if webhook_intervals else None,
                        webhook_intervals=webhook_intervals,
                    )
                except TrackerServiceError as error:
                    report_start_failure(
                        start_responses,
                        requested_count=count,
                        attempted_count=attempt_index + 1,
                        uncertain=True,
                        rejected=False,
                        json_output=json_output,
                    )
                    raise click.ClickException(str(error)) from error

                click.echo("\r\033[K", nl=False, err=True)
                if response.status_code != 200:
                    report_start_failure(
                        start_responses,
                        requested_count=count,
                        attempted_count=attempt_index + 1,
                        uncertain=response.status_code >= 500,
                        rejected=True,
                        json_output=json_output,
                    )
                    raise click.ClickException(str(response_error_detail(response)))

                try:
                    start_response = StartBenchmarkResponse.model_validate(response.json())
                except ValueError as error:
                    report_start_failure(
                        start_responses,
                        requested_count=count,
                        attempted_count=attempt_index + 1,
                        uncertain=True,
                        rejected=False,
                        json_output=json_output,
                    )
                    raise click.ClickException("Tracker returned a malformed 200 start response.") from error

                start_responses.append(start_response)
                if not json_output:
                    format_start_benchmark_response(start_response, connect)
                elif connect or count > 1:
                    # Publish each run ID before the next request so a later
                    # failure cannot strand an already-confirmed run.
                    emit_start_outcome(
                        [start_response],
                        requested_count=count,
                        attempted_count=attempt_index + 1,
                        confirmed_count=len(start_responses),
                        outcome="completed" if len(start_responses) == count else "in_progress",
                        latest_request_outcome="confirmed",
                        event="launch",
                    )
                if connect:
                    if json_output:
                        stream_benchmark_status(tracker, start_response.benchmark_id, output_format="jsonl")
                    else:
                        stream_benchmark_status(tracker, start_response.benchmark_id)

        if json_output:
            emit_start_outcome(
                start_responses,
                requested_count=count,
                attempted_count=count,
                confirmed_count=len(start_responses),
                outcome="completed",
                latest_request_outcome="confirmed",
            )
        else:
            format_confirmed_start_summary([response.benchmark_id for response in start_responses], count)
    except (BundlerError, TrackerServiceError, ContractValidationError) as e:
        raise click.ClickException(str(e))
