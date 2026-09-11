"""Discover run-filter values through Tracker."""

import asyncio

import click
from valkyrie.sdk import FilterOptionsResponse, ValkyrieClient, ValkyrieSDKError

from valkyrie.cli.display import terminal_safe
from valkyrie.cli.runtime_config import config_location, tracker_service_url


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
    try:
        response = asyncio.run(_filter_options())
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    for field in type(response).model_fields:
        values: list[str] = getattr(response, field)
        click.echo(f"{field}: {terminal_safe(', '.join(values), preserve_newlines=False)}")


async def _filter_options() -> FilterOptionsResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        return await client.runs.filter_options()
