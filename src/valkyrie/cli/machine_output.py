"""Shared machine-readable CLI output helpers."""

import functools
import json
import re
from collections.abc import Callable, Mapping
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

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_URL_TRAILING_PUNCTUATION = ".,;:!?)]}'\""


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


def redact_urls(text: str) -> str:
    """Drop any URL a receipt must not carry, such as a presigned task-IDs source.

    Upstream failures quote the URL they were given, so an unredacted receipt would
    publish its credentials to stdout, where agents persist it. A plain service URL
    is diagnostic and stays; stderr still carries the original text either way.
    """

    def redact(match: re.Match[str]) -> str:
        url = match.group().rstrip(_URL_TRAILING_PUNCTUATION)
        trailing = match.group()[len(url) :]
        try:
            parsed = urlsplit(url)
            credentialed = parsed.username is not None or bool(parsed.query) or bool(parsed.fragment)
        except ValueError:
            return "<redacted-url>" + trailing

        return ("<redacted-url>" if credentialed else url) + trailing

    return _URL_RE.sub(redact, text)


def json_errors(func: Callable[..., None]) -> Callable[..., None]:
    """Make a failing ``--json`` invocation end with a parseable error document.

    Apply closest to the callback; it reads the ``json_output`` parameter Click
    passes by keyword. Rejected usage and documented terminal receipts (``Exit``)
    keep their existing output, since neither needs a second explanation.
    """

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> None:
        try:
            func(*args, **kwargs)
        except (click.UsageError, click.exceptions.Exit):
            raise
        except Exception as error:
            if kwargs.get("json_output"):
                message = (
                    error.format_message()
                    if isinstance(error, click.ClickException)
                    else f"{type(error).__name__}: {error}"
                )
                # ``command_path`` leads with the program name, so the ``run retry`` alias reports itself.
                command = " ".join(click.get_current_context().command_path.split()[1:])
                emit_json("error", command=command, error_message=redact_urls(message))
            raise

    return wrapper  # type: ignore[return-value]


def confirm_overwrite(text: str, *, json_output: bool, force: bool) -> bool | None:
    """Resolve an overwrite decision without stranding non-interactive callers.

    Returns ``True`` to proceed, ``False`` when the operator declines, and
    ``None`` when no answer is obtainable, so the caller can emit an actionable
    blocked receipt instead of an opaque abort.
    """
    if force:
        return True

    try:
        if not json_output:
            return click.confirm(text)

        # Machine mode keeps stdout parseable, so the prompt itself goes to stderr.
        with redirect_stdout(StringIO()):
            return click.confirm(text, err=True)
    except click.Abort:
        return None


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
