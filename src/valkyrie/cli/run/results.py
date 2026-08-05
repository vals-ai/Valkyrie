from pathlib import Path
from uuid import UUID

import click
from tracker.types import FinalViewResponse, RetrieveResultsResponse

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.machine_output import confirm_action, emit_json, json_errors, json_option
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
@click.option(
    "--force",
    is_flag=True,
    default=False,
    required=False,
    help="Overwrite an existing local file or S3 result without prompting.",
)
@json_option
@json_errors
def results(
    run_id: UUID,
    path: Path | None,
    s3: bool,
    task_ids: str | None,
    task_ids_file: str | None,
    force: bool,
    json_output: bool,
) -> None:
    """
    Retrieve the results of a run by its run id.

    Example:
        valkyrie run results e532551e-d51b-4912-983d-47695bd24174 --path ./results-e532551e-d51b-4912-983d-47695bd24174.json
    """
    subset_task_ids = resolve_task_ids(task_ids, task_ids_file)

    if not json_output:
        click.echo(f"Retrieving results for run: {run_id}")

    try:
        with TrackerService() as tracker:
            if s3 and tracker.check_results_exist_in_s3(run_id):
                if not authorize_overwrite(run_id, None, json_output=json_output, force=force):
                    return

            results_response: RetrieveResultsResponse = tracker.retrieve_results(run_id, s3, task_ids=subset_task_ids)

            if isinstance(results_response, FinalViewResponse):
                scored = len(results_response.evaluation_results or {}) + len(results_response.task_errors or {})
                if subset_task_ids and not json_output:
                    click.echo(
                        click.style(
                            f"Scored over {scored} of {len(subset_task_ids)} subset task ids.",
                            fg="yellow" if scored < len(subset_task_ids) else "green",
                        )
                    )
                # Decide only once the replacement is in hand, so a blocked receipt is truthful.
                output_path = path or Path(f"./results-{run_id}.json")
                if output_path.exists() and not authorize_overwrite(
                    run_id, output_path, json_output=json_output, force=force
                ):
                    return

                download_final_view(output_path, results_response)
                if json_output:
                    emit_write_receipt(
                        "completed",
                        run_id=run_id,
                        output_path=output_path,
                        requested_task_count=len(subset_task_ids) if subset_task_ids else None,
                        scored_task_count=scored,
                    )
                else:
                    click.echo(click.style(f"Results saved to '{output_path}'", fg="green", bold=True))
            else:
                if json_output:
                    emit_write_receipt(
                        "completed",
                        run_id=run_id,
                        s3_url=results_response.s3_url,
                        requested_task_count=len(subset_task_ids) if subset_task_ids else None,
                    )
                else:
                    click.echo(click.style("Download (expires in 1 day):", fg="cyan", bold=True))
                    click.echo(f"  {results_response.presigned_url}")
                    click.echo()
                    click.echo(click.style("AWS Console:", fg="cyan", bold=True))
                    click.echo(f"  {results_response.console_url}")

    except TrackerServiceError as e:
        raise click.ClickException(str(e))


def emit_write_receipt(status: str, *, run_id: UUID, output_path: Path | None = None, **fields: object) -> None:
    """Emit one retrieval receipt; a local target is the one with an output path."""
    emit_json(
        "run_results",
        action="write",
        status=status,
        run_id=str(run_id),
        target="local" if output_path is not None else "s3",
        **({"output_path": str(output_path.resolve())} if output_path is not None else {}),
        **fields,
    )


def authorize_overwrite(run_id: UUID, output_path: Path | None, *, json_output: bool, force: bool) -> bool:
    """Resolve an overwrite into a decision the caller can act on.

    Returns ``True`` to proceed and ``False`` when the operator declined, which is
    a completed decision rather than a failure. An unanswerable prompt raises, so
    a non-interactive caller gets actionable remediation instead of a bare abort.
    """
    target = f"'{output_path}'" if output_path is not None else "S3 results for this run"
    prompt = (
        f"File '{output_path}' already exists. Overwrite?"
        if output_path is not None
        else "Results already exist in S3. Overwrite?"
    )
    decision = confirm_action(prompt, json_output=json_output, force=force)

    if decision is None:
        if json_output:
            emit_write_receipt("blocked", run_id=run_id, output_path=output_path, reason="target_exists")
        raise click.ClickException(f"Refusing to overwrite {target} without confirmation. Re-run with --force.")

    if not decision:
        if json_output:
            emit_write_receipt("cancelled", run_id=run_id, output_path=output_path)
            return False
        raise click.Abort()

    return True


def download_final_view(path: Path, final_view: FinalViewResponse) -> None:
    """Write the final view to an already-authorized path.

    The caller owns the overwrite decision and the success report so that it can
    emit the matching machine receipt; this only refuses a destination it cannot
    write to.
    """
    if not path.parent.exists():
        raise click.ClickException(f"'{path.parent}' directory does not exist! Please create it first.")

    with open(path, "w") as output_file:
        output_file.write(
            final_view.model_dump_json(
                indent=4,
                exclude_none=True,
                exclude={"benchmark_arguments": {"contract": {"secrets", "kwargs"}}},
            )
        )
