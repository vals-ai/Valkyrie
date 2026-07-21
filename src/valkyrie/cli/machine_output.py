"""Shared machine-readable CLI output helpers."""

import json
from collections.abc import Mapping
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from urllib.parse import urlsplit

import click
from click.core import ParameterSource

json_option = click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Emit strict, versioned JSON (JSONL for streams) on stdout; diagnostics and prompts use stderr.",
)


def utc_isoformat(value: datetime) -> str:
    """Format a datetime as an ISO-8601 UTC timestamp."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def strict_json(payload: Mapping[str, object]) -> str:
    """Serialize deterministic compact JSON without non-finite number extensions."""
    return json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":"))


def emit_json(kind: str, **fields: object) -> None:
    """Write one versioned machine document to stdout."""
    click.echo(strict_json({"schema_version": 1, "kind": kind, **fields}))


def confirm_action(text: str, *, json_output: bool) -> bool:
    """Write machine-mode prompts to stderr so stdout remains parseable."""
    if not json_output:
        return click.confirm(text)

    with redirect_stdout(StringIO()):
        return click.confirm(text, err=True)


def credential_free_url(value: str | None) -> str | None:
    """Return a URL only when it contains no credential-bearing components."""
    if not isinstance(value, str):
        return None

    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        return None

    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None

    return value


def resolve_json_format(
    output_format: str,
    json_output: bool,
    *,
    connected: bool = False,
) -> str:
    """Resolve ``--json`` to the compatible existing format spelling."""
    if not json_output:
        return output_format

    context = click.get_current_context(silent=True)
    if context is not None and context.get_parameter_source("output_format") is ParameterSource.COMMANDLINE:
        raise click.UsageError("--json cannot be combined with --format.")

    return "jsonl" if connected else "json"
