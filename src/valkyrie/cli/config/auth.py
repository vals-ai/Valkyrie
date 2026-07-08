import click

from valkyrie.cli.config.state import load_config, write_config
from valkyrie.cli.display import format_table


@click.group()
def auth() -> None:
    """Manage benchmark service auth credentials."""
    pass


@auth.command("set")
@click.argument("name")
@click.argument("credential")
def auth_set(name: str, credential: str) -> None:
    """Set an auth credential for a benchmark service.

    Example: valkyrie config auth set swebench my-secret-credential
    """
    harness_config = load_config()

    if "benchmark_auth" not in harness_config:
        harness_config["benchmark_auth"] = {}

    harness_config["benchmark_auth"][name] = credential

    write_config(harness_config)

    click.echo(click.style(f"Auth for '{name}' has been set.", fg="green"))


@auth.command("remove")
@click.argument("name")
def auth_remove(name: str) -> None:
    """Remove an auth credential for a benchmark service.

    Example: valkyrie config auth remove swebench
    """
    current = load_config()

    auth_credentials = current.get("benchmark_auth") or {}
    if name not in auth_credentials:
        raise click.ClickException(f"Auth for '{name}' not configured.")

    del auth_credentials[name]
    current["benchmark_auth"] = auth_credentials

    write_config(current)

    click.echo(click.style(f"Auth for '{name}' has been removed.", fg="green"))


@auth.command("list")
def auth_list() -> None:
    """List all configured benchmark auth credentials."""
    current = load_config()

    auth_credentials: dict[str, str] = current.get("benchmark_auth") or {}
    if not auth_credentials:
        click.echo(click.style("No benchmark auth credentials configured.", fg="yellow"))
        return

    rows = [
        {"Benchmark": name, "Credential": credential[:4] + "***" if len(credential) > 4 else "***"}
        for name, credential in auth_credentials.items()
    ]
    format_table(rows, ["Benchmark", "Credential"], item_name="credential")
