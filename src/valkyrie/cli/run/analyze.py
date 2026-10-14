import tempfile
from pathlib import Path
from typing import Any
from uuid import UUID

import click
import yaml
from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieSDKError

from valkyrie.cli.client import call

_UNFINISHED_RUN_MESSAGES = {
    "IN_PROGRESS": "Run {run_id} is still in progress. Try again after it finishes.",
    "STOPPING": "Run {run_id} is stopping. Try again once it settles.",
    "STOPPED": "Run {run_id} was stopped before completion — nothing to analyze.",
    "ERROR": "Run {run_id} errored before completing — nothing to analyze.",
}


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

    async def run_analysis(client: ValkyrieClient) -> dict[str, Any] | None:
        metadata = await client.runs.metadata(run_id)
        lambda_function = await _ingest_lambda(client, metadata.benchmark_arguments.contract.name)
        result: dict[str, Any] | None = None
        try:
            async for event in client.runs.analyze(run_id, no_cache=no_cache, lambda_function=lambda_function):
                if event.event == "started":
                    click.echo(f"  Invoking {event.data.get('lambda_function')}...")
                elif event.event == "heartbeat":
                    click.echo(".", nl=False)
                elif event.event == "done":
                    result = event.data
        except ValkyrieAPIError as error:
            # Tracker refuses unfinished runs with "status is <STATUS> (must be FINISHED)"; that's "come back later".
            detail = str(error.detail)
            status = next((status for status in _UNFINISHED_RUN_MESSAGES if f"status is {status}" in detail), None)
            if "must be FINISHED" not in detail or status is None:
                raise
            click.echo(click.style(_UNFINISHED_RUN_MESSAGES[status].format(run_id=run_id), fg="yellow"))
            raise click.exceptions.Exit(1) from error
        click.echo()
        return result

    result = call(run_analysis)
    if result is None:
        return
    if url := result.get("reading_plan_url"):
        click.echo(f"  Reading plan: {click.style(url, fg='blue', underline=True)}")
    else:
        click.echo(click.style("Analysis completed but no reading plan URL was produced.", fg="yellow"))


async def _ingest_lambda(client: ValkyrieClient, agent_name: str) -> str:
    """Read `ingest_lambda` from the agent's currently pushed contract, not the run's stored copy.

    That lets past runs be analyzed once the agent's contract declares an analyzer Lambda. Only that field is
    read, so a contract that current validation would reject in other fields still works.
    """
    try:
        with tempfile.TemporaryDirectory() as directory:
            agent_path = Path(await client.agents.download(agent_name, directory))
            paths = [agent_path / name for name in ("contract.yaml", "contract.yml")]
            contract = next((yaml.safe_load(path.read_text()) for path in paths if path.is_file()), None)
    except (ValkyrieSDKError, ValueError, yaml.YAMLError) as error:
        raise click.ClickException(
            f"Could not load contract for agent '{agent_name}' through Tracker: {error}\n\n"
            "If the agent has never been pushed, run `valk agent push ./<agent_dir>`."
        ) from error
    lambda_function = contract.get("ingest_lambda") if isinstance(contract, dict) else None
    if not isinstance(lambda_function, str) or not lambda_function:
        raise click.ClickException(
            f"Agent '{agent_name}' has no `ingest_lambda` set in its current contract. "
            "Declare it in contract.yaml and re-push with `valk agent push ./<agent_dir>`."
        )
    return lambda_function
