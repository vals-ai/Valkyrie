import asyncio
from uuid import UUID

import click
from tracker.database.models import RetryMode
from tracker.exceptions import S3Error

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.progress import stream_benchmark_status
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.agent.storage import update_benchmark_agent_version
from valkyrie.cli.service_headers import benchmark_service_headers
from valkyrie.cli.tracker_client import TrackerService


@click.command(
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
    type=click.IntRange(min=1),
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
            benchmark_info = tracker.fetch_benchmark(run_id)
            service_headers = benchmark_service_headers(benchmark_info.benchmark_name)

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
