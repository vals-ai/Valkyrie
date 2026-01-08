"""CLI views/commands for the agentic harness."""

from pathlib import Path

import click

import cli.contract_bundler as bundler
from cli.contract_bundler import BundlerError
from cli.tracker_service import TrackerService, TrackerServiceError


@click.group()
def cli():
    """Agentic Harness CLI."""
    pass


@cli.command()
@click.option(
    "--contract",
    type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True),
    required=True,
    help="Path to contract directory (e.g., contracts/claude_code)",
)
@click.option(
    "--benchmark",
    type=str,
    required=True,
    help="Name of the benchmark to run (e.g., swebench, finance)",
)
def run(
    contract: Path,
    benchmark: str,
):
    """
    Run an agent on a benchmark.

    Example:
        harness run --contract contracts/claude_code --benchmark swebench
    """
    click.echo(f"Running benchmark: {benchmark}")
    click.echo(f"Contract: {contract}")
    click.echo()

    click.echo("Validating contract...")
    bundler.validate_contract(contract)

    try:
        with TrackerService() as tracker:
            click.echo("\nChecking tracker service health...")
            tracker.health_check()

            click.echo(f"Creating contract bundle for: {contract}")
            click.echo("Zipping bundle...")

            with bundler.create_contract_bundle_stream(contract) as file_stream:
                click.echo("Uploading bundle to tracker service...")
                tracker.upload_contract(contract.name, file_stream)

            click.echo(f"Starting benchmark run for: {contract.name}")
            tracker.start_run(contract.name, benchmark)

            click.echo("Benchmark run started successfully")
    except (BundlerError, TrackerServiceError) as e:
        raise click.ClickException(str(e))


if __name__ == "__main__":
    cli()
