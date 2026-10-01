import os
from typing import Any

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.runtime_config import (
    ENVIRONMENT_CONFIG_KEY,
    TRACKER_SERVICE_URL_ENV_VAR,
    config_location,
    tracker_url_for_environment,
)
from valkyrie.cli.tracker_client import TrackerService
from valkyrie.cli.config.state import ConfigValue, load_config, read_config_if_exists, write_config
from valkyrie.sdk.config import ValkyrieConfig


def _rotate_matching_benchmark_auth(config: dict[str, Any], new_api_key: str) -> int:
    """Rotate benchmark credentials derived from the previous hosted API key."""
    previous_api_key = config.get("api_key")
    benchmark_auth = config.get("benchmark_auth")
    if (
        not isinstance(previous_api_key, str)
        or not previous_api_key
        or previous_api_key == new_api_key
        or not isinstance(benchmark_auth, dict)
    ):
        return 0

    replacements = {
        previous_api_key: new_api_key,
        f"Bearer {previous_api_key}": f"Bearer {new_api_key}",
    }
    updated = 0
    for benchmark_name, credential in benchmark_auth.items():
        if isinstance(credential, str) and credential in replacements:
            benchmark_auth[benchmark_name] = replacements[credential]
            updated += 1

    return updated


@click.command()
def init() -> None:
    """Create the Valkyrie config with the credentials and endpoints a run needs."""
    current_config: dict[str, Any] = {}
    config_path = config_location()
    if config_path.exists():
        try:
            current_config = read_config_if_exists()
        except Exception:
            pass

    current_config = {
        field.alias or name: current_config.get(field.alias or name, current_config.get(name))
        for name, field in ValkyrieConfig.model_fields.items()
        if (field.alias or name) in current_config or name in current_config
    }
    mode = click.prompt(
        "Setup mode",
        type=click.Choice(["hosted", "self-hosted"]),
        default="self-hosted",
    )

    if mode == "hosted":
        environment = click.prompt(
            "Hosted environment",
            type=click.Choice(["bench", "prod"]),
            default="bench",
        )
        current_config[ENVIRONMENT_CONFIG_KEY] = environment
        tracker_url = os.environ.get(TRACKER_SERVICE_URL_ENV_VAR) or tracker_url_for_environment(environment)
        api_key = (os.environ.get("VALKYRIE_API_KEY") or click.prompt("API Key", hide_input=True)).strip()
        if not api_key:
            raise click.ClickException("API key must not be blank")
        _rotate_matching_benchmark_auth(current_config, api_key)
        current_config["api_key"] = api_key

        try:
            result = TrackerService.init_org(api_key, tracker_url)
            runtime = TrackerService.aws_runtime_metadata(api_key, tracker_url)
        except TrackerServiceError as e:
            raise click.ClickException(str(e)) from e
        click.echo(f"Organization '{result['org_name']}' configured successfully.\n")

        if result.get("email_claim_missing"):
            click.echo(
                click.style(
                    "⚠  This access key is missing the 'email' custom claim. "
                    "Run attribution for runs you start will be empty.\n"
                    "   Ask your Vals admin to add an 'email' (and optionally 'name') "
                    "custom claim to this key.",
                    fg="yellow",
                )
            )

        if runtime.mode == "managed":
            click.echo(
                "Managed AWS execution is enabled. Runs resolve AWS resources and the "
                "sandbox provider from the Vals deployment.\n"
            )
        else:
            click.echo(
                click.style(
                    "Managed AWS execution is not enabled for this organization; "
                    "hosted runs will be rejected until Vals support enables it.\n",
                    fg="yellow",
                )
            )
    if mode != "hosted":
        current_config.pop("api_key", None)
        current_config.pop(ENVIRONMENT_CONFIG_KEY, None)

    write_config(current_config, sort_keys=False)

    click.echo(click.style(f"\nConfig written to {config_path}", fg="green", bold=True))


@click.command()
@click.argument("key")
@click.argument("value")
def set(key: str, value: str) -> None:
    """
    Set a single key in the Valkyrie config.

    """

    current = load_config()

    try:
        config_value = ConfigValue.from_str(key)
    except ValueError:
        raise click.ClickException(
            f"Key '{key}' is not a valid config key. Valid keys: {', '.join(m.value for m in ConfigValue)}"
        )

    rotated_benchmark_auth = 0
    if config_value is ConfigValue.API_KEY:
        rotated_benchmark_auth = _rotate_matching_benchmark_auth(current, value)

    current[config_value.value] = value

    write_config(current, sort_keys=False)

    click.echo(click.style(f"  {key} updated.", fg="green"))
    if rotated_benchmark_auth:
        label = "benchmark" if rotated_benchmark_auth == 1 else "benchmarks"
        click.echo(f"  Updated benchmark service auth for {rotated_benchmark_auth} {label}.")


@click.command(name="remove")
@click.argument("key")
def config_remove(key: str) -> None:
    """
    Remove a single key from the Valkyrie config.

    """

    current = load_config()

    try:
        config_value = ConfigValue.from_str(key)
    except ValueError:
        raise click.ClickException(
            f"Key '{key}' is not a valid config key. Valid keys: {', '.join(m.value for m in ConfigValue)}"
        )

    current.pop(config_value.value, None)

    write_config(current, sort_keys=False)

    click.echo(click.style(f"  {key} removed.", fg="green"))
