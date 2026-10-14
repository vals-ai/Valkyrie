"""Discover run-filter values through Tracker."""

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import terminal_safe


@click.command(name="filter-options")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def filter_options(output_format: str) -> None:
    """List benchmark, agent, model, dataset, and starter values from your run history."""
    response = call(lambda client: client.runs.filter_options())
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    for field in type(response).model_fields:
        values: list[str] = getattr(response, field)
        click.echo(f"{field}: {terminal_safe(', '.join(values), preserve_newlines=False)}")
