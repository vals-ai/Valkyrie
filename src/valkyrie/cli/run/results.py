from pathlib import Path
from uuid import UUID

import click
from tracker.types import FinalViewResponse, RetrieveResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    name="results",
    help=(
        "Retrieve run results by its run id. \n\n"
        "Example:\n"
        "valkyrie run results 123e4567-e89b-12d3-a456-426614174000 "
        "--path ./results-123e4567-e89b-12d3-a456-426614174000.json"
    ),
)
@click.argument("run_id", type=UUID)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    default=None,
    required=False,
    help="Path to save the results (default: ./results-<run_id>.json)",
)
@click.option(
    "--s3",
    is_flag=True,
    default=False,
    required=False,
    help="Saves results to s3 instead of downloading them locally. Can be found at bucket://benchmarks/run_id/<benchmark>.json",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated task IDs to score the subset over. Final score is recomputed over the subset.",
)
@click.option(
    "--task-ids-file",
    type=str,
    required=False,
    default=None,
    help="Path or http(s) URL to a text file with one task ID per line",
)
def results(
    run_id: UUID,
    path: Path | None,
    s3: bool,
    task_ids: str | None,
    task_ids_file: str | None,
):
    """
    Retrieve the results of a run by its run id.

    Example:
        valkyrie run results e532551e-d51b-4912-983d-47695bd24174 --path ./results-e532551e-d51b-4912-983d-47695bd24174.json
    """
    subset_task_ids = resolve_task_ids(task_ids, task_ids_file)

    click.echo(f"Retrieving results for run: {run_id}")

    try:
        with TrackerService() as tracker:
            if s3:
                if tracker.check_results_exist_in_s3(run_id):
                    if not click.confirm("Results already exist in S3. Overwrite?"):
                        raise click.Abort()

            results_response: RetrieveResultsResponse = tracker.retrieve_results(run_id, s3, task_ids=subset_task_ids)

            if isinstance(results_response, FinalViewResponse):
                if subset_task_ids:
                    scored = len(results_response.evaluation_results or {}) + len(results_response.task_errors or {})
                    click.echo(
                        click.style(
                            f"Scored over {scored} of {len(subset_task_ids)} subset task ids.",
                            fg="yellow" if scored < len(subset_task_ids) else "green",
                        )
                    )
                default_path: Path = Path(f"./results-{run_id}.json")

                download_final_view(path or default_path, results_response)
            else:
                click.echo(click.style("Download (expires in 1 day):", fg="cyan", bold=True))
                click.echo(f"  {results_response.presigned_url}")
                click.echo()
                click.echo(click.style("AWS Console:", fg="cyan", bold=True))
                click.echo(f"  {results_response.console_url}")

    except TrackerServiceError as e:
        raise click.ClickException(str(e))


def download_final_view(path: Path, final_view: FinalViewResponse) -> None:
    if not path.parent.exists():
        raise click.ClickException(f"'{path.parent}' directory does not exist! Please create it first.")

    if path.exists():
        if not click.confirm(f"File '{path}' already exists. Overwrite?"):
            raise click.Abort()

    with open(path, "w") as output_file:
        output_file.write(
            final_view.model_dump_json(
                indent=4,
                exclude_none=True,
                exclude={"benchmark_arguments": {"contract": {"env"}}},
            )
        )

    click.echo(click.style(f"Results saved to '{path}'", fg="green", bold=True))
