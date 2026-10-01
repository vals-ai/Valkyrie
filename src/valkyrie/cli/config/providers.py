import click

from valkyrie.cli.config.state import load_config, write_config


@click.group()
def provider() -> None:
    """Manage sandbox provider secret mappings."""
    pass


@provider.command("set")
@click.argument("name")
@click.argument("secret_name")
def provider_set(name: str, secret_name: str) -> None:
    """Set a named sandbox provider secret.

    Example: valkyrie config provider set daytona DaytonaSecrets
    """
    harness_config = load_config()

    providers = harness_config.get("sandbox_providers")
    if not isinstance(providers, dict):
        providers = {}

    providers[name] = secret_name
    harness_config["sandbox_providers"] = providers

    write_config(harness_config, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' has been set.", fg="green"))


@provider.command("default")
@click.argument("name")
def provider_default(name: str) -> None:
    """Set the default sandbox provider.

    Example: valkyrie config provider default modal
    """
    harness_config = load_config()

    providers = harness_config.get("sandbox_providers")
    if not isinstance(providers, dict) or name not in providers:
        raise click.ClickException(f"Provider '{name}' not configured.")

    harness_config["default_sandbox_provider"] = name

    write_config(harness_config, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' is now the default.", fg="green"))


@provider.command("remove")
@click.argument("name")
def provider_remove(name: str) -> None:
    """Remove a named sandbox provider secret.

    Example: valkyrie config provider remove modal
    """
    harness_config = load_config()

    providers = harness_config.get("sandbox_providers") or {}
    if name not in providers:
        raise click.ClickException(f"Provider '{name}' not configured.")

    del providers[name]
    if providers:
        harness_config["sandbox_providers"] = providers
    else:
        harness_config.pop("sandbox_providers", None)
    if harness_config.get("default_sandbox_provider") == name:
        harness_config.pop("default_sandbox_provider", None)

    write_config(harness_config, sort_keys=False)

    click.echo(click.style(f"Provider '{name}' has been removed.", fg="green"))


@provider.command("list")
def provider_list() -> None:
    """List all configured sandbox providers."""
    harness_config = load_config()

    providers: dict[str, str] = harness_config.get("sandbox_providers") or {}
    if not providers:
        click.echo(click.style("No sandbox providers configured.", fg="yellow"))
        return

    for name, secret_name in providers.items():
        click.echo(f"{name}: {secret_name}")
