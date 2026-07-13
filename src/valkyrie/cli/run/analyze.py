import asyncio
import sys
from typing import Any
from uuid import UUID

import click
from tracker.exceptions import S3Error

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.agent.storage import get_ingest_lambda_from_s3
from valkyrie.cli.tracker_client import TrackerService


@click.command(
    name="analyze",
    help="Trigger Docent ingestion + error analysis for a finished run.",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Bypass the cached reading-plan URL and re-fire ingestion.",
)
def analyze(run_id: UUID, no_cache: bool) -> None:
    """Trigger Docent ingestion + error analysis for a finished run."""
    try:
        with TrackerService() as tracker:
            # Resolve the analyzer Lambda from the agent's current pushed contract.
            metadata = tracker.fetch_benchmark_metadata(run_id)
            contract_name = metadata.benchmark_arguments.contract.name
            try:
                lambda_function = asyncio.run(get_ingest_lambda_from_s3(contract_name))
            except S3Error as e:
                raise click.ClickException(
                    f"Could not load contract for agent '{contract_name}' from S3: {e}\n\n"
                    "If the agent has never been pushed, run `valk agent push ./<agent_dir>`."
                )
            if not lambda_function:
                raise click.ClickException(
                    f"Agent '{contract_name}' has no `ingest_lambda` set in its current contract. "
                    "Declare it in contract.yaml and re-push with `valk agent push ./<agent_dir>`."
                )

            terminal: tuple[str, dict[str, Any]] | None = None
            for event, data in tracker.analyze_benchmark(
                run_id,
                no_cache=no_cache,
                lambda_function=lambda_function,
            ):
                if event == "started":
                    click.echo(f"  Invoking {data.get('lambda_function')}...")
                elif event == "heartbeat":
                    click.echo(".", nl=False)
                elif event == "done":
                    terminal = (event, data)
                elif event == "error":
                    raise click.ClickException(data.get("message") or "analyzer Lambda failed")

            click.echo()

            if not terminal:
                return

            event, data = terminal
            url = data.get("reading_plan_url")
            if url:
                click.echo(f"  Reading plan: {click.style(url, fg='blue', underline=True)}")
            else:
                click.echo(
                    click.style(
                        "Analysis completed but no reading plan URL was produced.",
                        fg="yellow",
                    )
                )
    except TrackerServiceError as e:
        # "Cannot analyze run X: status is IN_PROGRESS (must be FINISHED)." —
        # not really an error, just "come back later." Render it cleanly.
        msg = str(e)
        if "must be FINISHED" in msg:
            for status, line in (
                ("IN_PROGRESS", f"Run {run_id} is still in progress. Try again after it finishes."),
                ("STOPPING", f"Run {run_id} is stopping. Try again once it settles."),
                ("STOPPED", f"Run {run_id} was stopped before completion — nothing to analyze."),
                ("ERROR", f"Run {run_id} errored before completing — nothing to analyze."),
            ):
                if f"status is {status}" in msg:
                    click.echo(click.style(line, fg="yellow"))
                    sys.exit(1)
        raise click.ClickException(msg)
