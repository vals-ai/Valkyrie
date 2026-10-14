"""Call the Tracker from click commands.

`call()` goes through the SDK. `request_tracker()` covers the account routes the SDK doesn't wrap
(login, access keys, organizations, sandbox providers and saved settings).
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import click
import httpx
from valkyrie.sdk import ValkyrieClient, ValkyrieConfig, ValkyrieSDKError

from valkyrie.cli.display import terminal_safe

Result = TypeVar("Result")


def call(operation: Callable[[ValkyrieClient], Awaitable[Result]]) -> Result:
    """Run `operation` with a client for the Tracker `ValkyrieConfig` picks, and return its result.

    SDK, validation and file errors become click errors, so the command exits 1 with the message
    instead of a traceback.
    """

    async def run() -> Result:
        async with ValkyrieClient() as client:
            return await operation(client)

    try:
        return asyncio.run(run())
    except (ValkyrieSDKError, ValueError, OSError) as error:
        raise click.ClickException(terminal_safe(str(error), preserve_newlines=True)) from error


def _error_detail(response: httpx.Response) -> Any:
    try:
        body = response.json()
    except ValueError:
        return response.text
    detail = body.get("detail", response.text) if isinstance(body, dict) else response.text
    if isinstance(detail, list):
        return "; ".join(f"{error['loc'][-1]}: {error['msg']}" for error in detail)
    return detail


def request_tracker(
    method: str, path: str, auth: httpx.Auth | None = None, *, missing_ok: bool = False, **kwargs: Any
) -> Any:
    config = ValkyrieConfig()
    tracker_url = config.url
    auth = auth or config.http_auth()
    try:
        with httpx.Client(timeout=60, auth=auth) as client:
            response = client.request(method, f"{tracker_url}{path}", **kwargs)
    except httpx.HTTPError as error:
        raise click.ClickException(f"Could not reach the Tracker at {tracker_url}: {error}") from error
    if response.status_code == 401:
        problem = "You are not logged in." if auth is None else "The Tracker rejected your credentials."
        raise click.ClickException(f"{problem} Run `valkyrie login`.")
    if missing_ok and response.status_code == 404:
        return None
    if response.is_error:
        raise click.ClickException(f"{_error_detail(response)} ({response.status_code})")
    return response.json() if response.content else None
