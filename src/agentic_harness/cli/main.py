"""CLI views/commands for the agentic harness."""

from pathlib import Path

import click

from agentic_harness.cli.utils import start_benchmark_run, upload_to_tracker, validate_contract


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
    validate_contract(contract)
    click.echo("Contract validation passed!")

    # Upload contract to tracker service
    upload_to_tracker(contract)

    # Start the benchmark run
    click.echo("\nStarting benchmark run...")
    contract_name = contract.name
    start_benchmark_run(contract_name, benchmark)


if __name__ == "__main__":
    cli()
