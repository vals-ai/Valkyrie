import os
from typing import Any

import click

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.runtime_config import config_location
from valkyrie.cli.tracker_client import TrackerService
from valkyrie.cli.config.state import ConfigValue, load_config, read_config_if_exists, write_config


_REQUIRED_ENVIRONMENT_VARIABLES: dict[str, str | None | int] = {
    "AWS_ACCESS_KEY_ID": None,  # AWS ACCESS KEY
    "AWS_SECRET_ACCESS_KEY": None,  # AWS SECRETS KEY
    "AWS_DEFAULT_REGION": None,  # What region your secrets are in
    "S3_BUCKET": None,  # Center point where all agents and benchmark results are uploaded
    "LOG_GROUP": "benchmarks",  # the prefix to the cloudwatch logs (e.x. benchmarks/<benchmark_id>)
    "LOG_RETENTION_POLICY": 365,  # How long logs are kept until auto deleted
}
_STATIC_AWS_CREDENTIAL_KEYS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}


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
    """
    Initializes a config we can trust to have references to dependencies to run Valkyrie,
    this becomes our source of truth for secrets required to run Valkyrie
    """

    current_config: dict[str, Any] = {}
    config_path = config_location()
    if config_path.exists():
        try:
            current_config = read_config_if_exists()
        except Exception:
            pass

    mode = click.prompt(
        "Setup mode",
        type=click.Choice(["hosted", "self-hosted"]),
        default="self-hosted",
    )
    environment_variables = _REQUIRED_ENVIRONMENT_VARIABLES

    if mode == "hosted":
        api_key = (os.environ.get("VALKYRIE_API_KEY") or click.prompt("API Key")).strip()
        _rotate_matching_benchmark_auth(current_config, api_key)
        current_config["api_key"] = api_key

        try:
            result = TrackerService.init_org(api_key)
            runtime = TrackerService.aws_runtime_metadata(api_key)
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
            if not runtime.region or not runtime.s3_bucket:
                raise click.ClickException("Managed AWS configuration is missing its Region or S3 bucket.")
            current_config["AWS_DEFAULT_REGION"] = runtime.region
            current_config["S3_BUCKET"] = runtime.s3_bucket
            removed_static_credentials = any(key in current_config for key in _STATIC_AWS_CREDENTIAL_KEYS)
            for key in _STATIC_AWS_CREDENTIAL_KEYS:
                current_config.pop(key, None)
            environment_variables = {
                "LOG_GROUP": _REQUIRED_ENVIRONMENT_VARIABLES["LOG_GROUP"],
                "LOG_RETENTION_POLICY": _REQUIRED_ENVIRONMENT_VARIABLES["LOG_RETENTION_POLICY"],
            }
            click.echo(
                "Managed AWS execution is enabled. Local AWS operations will use the AWS SDK credential chain.\n"
            )
            if removed_static_credentials:
                click.echo(
                    "Existing static AWS credentials were removed. Restore them before retrying or resuming "
                    "an access-key run.\n"
                )

    collected_keys: dict[str, str] = {}
    for key, default in environment_variables.items():
        sourced = current_config.get(key) or os.environ.get(key)
        if sourced:
            click.echo(f"  {key}: sourced from {'environment' if not current_config.get(key) else 'existing config'}")
            collected_keys[key] = sourced
            continue

        if not default:
            value = click.prompt(
                f"  {key} (required, Enter to cancel)",
                default="",
                show_default=False,
            ).strip()

            if not value:
                click.echo(click.style(f"\n  {key} is required. Aborting.", fg="red"))
                raise click.Abort()
        else:
            value = click.prompt(f"  {key}", default=str(default)).strip()

        collected_keys[key] = value

    current_config.update(collected_keys)

    if mode != "hosted":
        current_config.pop("api_key", None)

    write_config(current_config)

    click.echo(click.style(f"\nConfig written to {config_path}", fg="green", bold=True))


@click.command()
@click.argument("key")
@click.argument("value")
def set(key: str, value: str) -> None:
    """
    Set a single key in the Valkyrie config.

    Example: valkyrie config set AWS_DEFAULT_REGION us-west-2
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

    write_config(current)

    click.echo(click.style(f"  {key} updated.", fg="green"))
    if rotated_benchmark_auth:
        label = "benchmark" if rotated_benchmark_auth == 1 else "benchmarks"
        click.echo(f"  Updated benchmark service auth for {rotated_benchmark_auth} {label}.")


@click.command(name="remove")
@click.argument("key")
def config_remove(key: str) -> None:
    """
    Remove a single key from the Valkyrie config.

    Example: valkyrie config remove AWS_DEFAULT_REGION
    """

    current = load_config()

    try:
        config_value = ConfigValue.from_str(key)
    except ValueError:
        raise click.ClickException(
            f"Key '{key}' is not a valid config key. Valid keys: {', '.join(m.value for m in ConfigValue)}"
        )

    if config_value.value in _REQUIRED_ENVIRONMENT_VARIABLES and config_value.value not in _STATIC_AWS_CREDENTIAL_KEYS:
        raise click.ClickException(
            f"Key '{key}' is required and cannot be removed. Consider using `valkyrie config set` to update it."
        )

    if config_value.value in current:
        del current[config_value.value]

    write_config(current)

    click.echo(click.style(f"  {key} removed.", fg="green"))
