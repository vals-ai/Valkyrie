from uuid import UUID

import click

from valkyrie.cli.client import call
from valkyrie.cli.run.progress import echo_next_steps
from valkyrie.cli.run.task_ids import resolve_task_ids


@click.command(
    help="Stop a run by its run id. \n\nExample:\nvalkyrie run stop 123e4567-e89b-12d3-a456-426614174000 --force"
)
@click.argument("run_id", type=UUID)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs to stop",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line",
)
@click.option(
    "--force",
    is_flag=True,
    required=False,
    default=False,
    help="Force stop the benchmark run",
)
def stop(run_id: UUID, task_ids: str | None, task_ids_file: str | None, force: bool):
    selected_task_ids = resolve_task_ids(task_ids, task_ids_file)
    scope = f"{len(selected_task_ids)} selected task(s) in run {run_id}" if selected_task_ids else f"run {run_id}"
    if not click.confirm(f"{'Force stop' if force else 'Stop'} {scope}?"):
        click.echo("Cancelled.")
        return

    call(lambda client: client.runs.stop(run_id, force=force, task_ids=selected_task_ids))

    subject = "Selected tasks" if selected_task_ids else "Run"
    if force:
        click.echo(click.style(f"✓ {subject} force stopped.", fg="green", bold=True))
    else:
        detail = (
            "agent tasks already running may finish"
            if selected_task_ids
            else "will finish after in-flight tasks complete"
        )
        click.echo(
            click.style(
                f"{subject} {'are' if selected_task_ids else 'is'} stopping — {detail}.", fg="yellow", bold=True
            )
        )
    echo_next_steps(run_id)
