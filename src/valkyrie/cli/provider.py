"""Map sandbox providers to the organization's secrets and choose your default provider through Tracker."""

from urllib.parse import quote

import click

from valkyrie.cli.client import request_tracker


@click.group()
def provider() -> None:
    """Choose the secret that configures each sandbox provider, and your default provider."""


@provider.command("set")
@click.argument("name")
@click.argument("secret")
def provider_set(name: str, secret: str) -> None:
    """Configure a sandbox provider with a stored secret.

    Replaces the secret the provider had. SECRET is the name or reference that `valkyrie secret set` printed.

    Example: valkyrie provider set daytona daytona
    """
    saved = request_tracker("PUT", f"/sandbox-providers/{quote(name, safe='')}", json={"secret_name": secret})
    click.echo(f"Provider {name} now uses the secret {saved['providers'][name]}.")


@provider.command("remove")
@click.argument("name")
def provider_remove(name: str) -> None:
    """Stop configuring a sandbox provider.

    Runs on it fail until it has a secret again.

    Example: valkyrie provider remove modal
    """
    request_tracker("DELETE", f"/sandbox-providers/{quote(name, safe='')}")
    click.echo(f"Provider {name} no longer has a secret.")


@provider.command("list")
def provider_list() -> None:
    """List the secret behind each sandbox provider; * marks your default provider."""
    providers = request_tracker("GET", "/sandbox-providers")["providers"]
    default = request_tracker("GET", "/settings")["default_provider"]
    if not providers and default is None:
        click.echo("No provider has a secret yet. Map one with `valkyrie provider set NAME SECRET`.")
    for name in sorted({*providers, *([default] if default else [])}):
        if name in providers:
            click.echo(f"{'*' if name == default else ' '} {name} → {providers[name]}")
        else:
            click.echo(f"* {name} (default; no secret set — run `valkyrie provider set {name} SECRET`)")


@provider.command("default")
@click.argument("name")
def provider_default(name: str) -> None:
    """Run on this sandbox provider unless a run passes --provider.

    Example: valkyrie provider default modal
    """
    settings = request_tracker("PUT", "/settings/default-provider", json={"provider": name})
    click.echo(f"Your default provider is now {settings['default_provider']}.")
