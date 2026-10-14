"""Store, list, and remove the tenant's own run secrets through Tracker."""

import click

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table


@click.group()
def secret() -> None:
    """Store secrets that your runs reference"""


@secret.command(name="set")
@click.argument("name", type=str)
def set_secret(name: str) -> None:
    """Store a secret, replacing its value if it already exists.

    Reads one line from a hidden prompt, or the whole value from stdin when piped; pipe multi-line values such
    as JSON. The value is never taken from the command line. Prints the reference to use as the secret name.

    Example:
        valkyrie secret set openai-key
        valkyrie secret set daytona < daytona.json
    """
    stdin = click.get_text_stream("stdin")
    value = (click.prompt("Value", hide_input=True) if stdin.isatty() else stdin.read()).strip()
    if not value:
        raise click.ClickException("The secret value is empty")
    entry = call(lambda client: client.secrets.set(name, value))
    click.echo(click.style(f"✓ Secret '{entry.name}' saved.", fg="green", bold=True))
    click.echo(f"Use this reference as the secret name: {entry.reference}")


@secret.command(name="list")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
def list_secrets(output_format: str) -> None:
    """List stored secrets by name, without their values.

    Secrets set or removed in the last few minutes may not show up yet.
    """
    response = call(lambda client: client.secrets.list())
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    format_table(
        [{"Secret": entry.name, "Reference": entry.reference} for entry in response.secrets],
        ["Secret", "Reference"],
        item_name="secret",
    )


@secret.command(name="remove")
@click.argument("name", type=str)
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def remove_secret(name: str, yes: bool) -> None:
    """Delete a stored secret. It cannot be recovered."""
    if not yes and not click.confirm(f"Delete secret '{name}'? Runs that reference it will fail."):
        click.echo("Cancelled.")
        return
    call(lambda client: client.secrets.remove(name))
    click.echo(click.style(f"✓ Secret '{name}' removed.", fg="green", bold=True))
