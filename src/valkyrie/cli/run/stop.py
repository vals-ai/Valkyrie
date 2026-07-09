from uuid import UUID

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_client import TrackerService


@click.command(
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
