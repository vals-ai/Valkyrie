import asyncio
from typing import Any
from uuid import UUID

import click
from tracker.exceptions import S3Error

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.agent.storage import get_ingest_lambda_from_s3
from valkyrie.cli.machine_output import credential_free_url, emit_json, json_errors, json_option, redact_urls
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
@json_option
@json_errors
def analyze(run_id: UUID, no_cache: bool, json_output: bool) -> None:
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

            terminal: dict[str, Any] | None = None
            for event, data in tracker.analyze_benchmark(
                run_id,
                no_cache=no_cache,
                lambda_function=lambda_function,
            ):
                if event == "started":
                    if json_output:
                        emit_json(
                            "run_analysis_event",
                            event="started",
                            run_id=str(run_id),
                            lambda_function=str(data.get("lambda_function") or lambda_function),
                        )
                    else:
                        click.echo(f"  Invoking {data.get('lambda_function')}...")
                elif event == "heartbeat":
                    if json_output:
                        emit_json("run_analysis_event", event="heartbeat", run_id=str(run_id))
                    else:
                        click.echo(".", nl=False)
                elif event == "done":
                    terminal = data
                    if json_output:
                        raw_reading_plan_url = data.get("reading_plan_url")
                        safe_reading_plan_url = credential_free_url(raw_reading_plan_url)
                        emit_json(
                            "run_analysis_event",
                            event="complete",
                            run_id=str(run_id),
                            reading_plan_url=safe_reading_plan_url,
                            # Distinguish "the analyzer returned no URL" from "the URL
                            # was withheld", which are different follow-up actions.
                            reading_plan_url_status=(
                                "present"
                                if safe_reading_plan_url is not None
                                else "withheld"
                                if raw_reading_plan_url
                                else "absent"
                            ),
                        )
                elif event == "error":
                    if json_output:
                        emit_json(
                            "run_analysis_event",
                            event="error",
                            run_id=str(run_id),
                            error_message=redact_urls(str(data.get("message") or "analyzer Lambda failed")),
                        )
                    raise click.ClickException(data.get("message") or "analyzer Lambda failed")

            if json_output:
                if terminal is not None:
                    return
                emit_json("run_analysis_event", event="disconnect", run_id=str(run_id))
                raise click.ClickException("Analysis stream ended without a terminal result.")

            click.echo()
            if terminal is None:
                return

            url = terminal.get("reading_plan_url")
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
                    if json_output:
                        emit_json(
                            "run_analysis_event",
                            event="unavailable",
                            run_id=str(run_id),
                            run_status=status,
                        )
                    else:
                        click.echo(click.style(line, fg="yellow"))
                    raise click.exceptions.Exit(1)
        raise click.ClickException(msg)
