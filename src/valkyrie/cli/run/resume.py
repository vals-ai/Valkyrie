from copy import copy
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import echo_next_steps, stream_benchmark_status
from valkyrie.cli.run.task_ids import resolve_task_ids


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
    "--update-agent",
    "-u",
    is_flag=True,
    default=False,
    help="Refresh the frozen agent copy from the current published agent bundle before resuming.",
)
@click.option(
    "--from-scratch",
    is_flag=True,
    default=False,
    help="Clear durable eval state and rerun generation.",
)
@click.option(
    "--benchmark-url",
    type=str,
    default=None,
    help="Replace the benchmark service URL for this run.",
)
@click.option(
    "--connect",
    is_flag=True,
    required=False,
    help="Connect to the tracker service to stream run updates after resuming",
)
@click.option(
    "--lambda", "lambda_function", type=str, default=None, help="Replace the post-run Lambda for this retry or resume."
)
@click.pass_context
def resume(
    ctx: click.Context,
    run_id: UUID,
    retry: bool,
    concurrency: int | None,
    task_ids: str | None,
    task_ids_file: str | None,
    secrets: tuple[tuple[str, str], ...],
    headers: tuple[tuple[str, str], ...],
    update_agent: bool,
    from_scratch: bool,
    benchmark_url: str | None,
    lambda_function: str | None,
    connect: bool,
):
    retry_task_ids = resolve_task_ids(task_ids, task_ids_file)

    # The retry alias shares this callback and always retries errored tasks.
    if ctx.info_name == "retry":
        retry = True

    async def restart(client: ValkyrieClient) -> None:
        await (client.runs.retry if retry else client.runs.resume)(
            run_id,
            concurrency=concurrency,
            task_ids=retry_task_ids,
            secrets=dict(secrets),
            service_headers=dict(headers),
            from_scratch=from_scratch,
            update_agent=update_agent,
            benchmark_url=benchmark_url,
            lambda_function=lambda_function,
        )
        click.echo(click.style(f"✓ Run {'retried' if retry else 'resumed'} successfully!", fg="green", bold=True))
        echo_next_steps(run_id, track=not connect)
        if connect:
            await stream_benchmark_status(client, run_id)

    call(restart)


def _retry_alias_params() -> list[click.Parameter]:
    """Copy resume's parameters, hiding the retry flag that this alias always forces on.

    The callback sets ``retry`` from the invoked command name, so ``--retry`` is a no-op here.
    It stays accepted for compatibility but is hidden from help and from generated docs.
    """
    parameters: list[click.Parameter] = []
    for parameter in resume.params:
        if isinstance(parameter, click.Option) and parameter.name == "retry":
            forced = copy(parameter)
            forced.hidden = True
            parameters.append(forced)
            continue
        parameters.append(parameter)
    return parameters


# Alias for run resume, the logic is the same under the hood
retry_command = click.Command(
    name="retry",
    callback=resume.callback,
    params=_retry_alias_params(),
    help="Retry a run's errored tasks by its run id. \n\nExample:\nvalkyrie run retry 123e4567-e89b-12d3-a456-426614174000 --concurrency 20",
    short_help="Retry a run's errored tasks by its run id.",
)
