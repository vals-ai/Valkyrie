from uuid import UUID

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.tracker_client import TrackerService


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
    """
    Stop a run by its run id.

    Example:
        valkyrie run stop 123e4567-e89b-12d3-a456-426614174000
    """
    selected_task_ids = resolve_task_ids(task_ids, task_ids_file)
    action = "Force stop" if force else "Stop"
    target = f"{len(selected_task_ids)} selected task(s) in run {run_id}" if selected_task_ids else f"run {run_id}"
    if not click.confirm(f"{action} {target}?"):
        click.echo("Cancelled.")
        return

    try:
        with TrackerService() as tracker:
            _ = tracker.stop_benchmark(
                run_id,
                force,
                task_ids=selected_task_ids,
            )

            if force:
                message = "✓ Selected tasks force stopped." if selected_task_ids else "✓ Run force stopped."
                click.echo(click.style(message, fg="green", bold=True))
            else:
                message = (
                    "Selected tasks are stopping — agent tasks already running may finish."
                    if selected_task_ids
                    else "Run is stopping — will finish after in-flight tasks complete."
                )
                click.echo(
                    click.style(
                        message,
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
