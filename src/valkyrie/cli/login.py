from __future__ import annotations

from typing import Any
from urllib.parse import quote

import click
import httpx

from valkyrie.cli.tracker_client import request_tracker
from valkyrie.sdk import ValkyrieConfig
from valkyrie.sdk.login import LoginSession, SessionAuth, device_login


def show_organizations(listing: dict[str, Any]) -> None:
    if not listing["organizations"] and not listing["invitations"]:
        click.echo(
            "You're not in an organization yet. Create one with `valkyrie org create ID`, "
            "or ask an organization admin to run `valkyrie org invite EMAIL`."
        )
    for entry in listing["organizations"]:
        click.echo(f"{'*' if entry['current'] else ' '} {entry['id']}{' (admin)' if entry['admin'] else ''}")
    for organization in listing["invitations"]:
        click.echo(f"  {organization} (invited; run `valkyrie org accept {organization}`)")


@click.command()
@click.option("--no-browser", is_flag=True, help="Print the sign-in link without opening a browser.")
def login(no_browser: bool) -> None:
    """Sign in with your browser, save the login, and use this Tracker by default."""
    tracker_url = ValkyrieConfig().url

    def announce(link: str, user_code: str) -> None:
        click.echo(f"Open {link} and confirm the code {user_code}.", err=True)
        if not no_browser:
            click.launch(link)

    try:
        with httpx.Client(timeout=30) as http:
            session = device_login(tracker_url, http, announce)
    except httpx.HTTPError as error:
        raise click.ClickException(f"The login to {tracker_url} could not finish: {error}") from error
    session.save()
    session.remember()
    if ValkyrieConfig().api_key:
        click.echo("VALKYRIE_API_KEY is set and takes precedence over this login; unset it to use the login.", err=True)
    auth = SessionAuth(session)
    listing = request_tracker("GET", "/orgs", auth=auth, missing_ok=True)
    if listing is None:
        identity = request_tracker("GET", "/auth/me", auth=auth)
        who = identity.get("email") or identity.get("name") or "your account"
        click.echo(f"Logged in to {session.tracker_url} as {who} for {identity['organization']}.")
        return
    click.echo(f"Logged in to {session.tracker_url} as {listing['email'] or 'your account'}.")
    show_organizations(listing)


@click.command()
def logout() -> None:
    """Forget the saved login for this Tracker, and stop using it by default."""
    tracker_url = ValkyrieConfig().url
    if LoginSession.forget(tracker_url):
        click.echo(f"Logged out of {tracker_url}.")
    else:
        click.echo(f"You were not logged in to {tracker_url}.")


@click.command()
def whoami() -> None:
    """Show which Tracker, organization and account your requests use."""
    identity = request_tracker("GET", "/auth/me")
    credential = {"login": "a login", "access_key": "an access key"}.get(identity["credential"], "no credential")
    who = identity.get("email") or identity.get("name") or "unknown user"
    click.echo(f"{who} in {identity['organization']} on {ValkyrieConfig().url} (using {credential})")


@click.group()
def token() -> None:
    """Create and revoke access keys for CI and other headless use."""


@token.command("create")
@click.argument("name")
@click.option(
    "--expires-in-days",
    type=click.IntRange(1, 90),
    default=30,
    show_default=True,
    help="Days until the key stops working.",
)
def token_create(name: str, expires_in_days: int) -> None:
    """Create an access key for your organization; set it as VALKYRIE_API_KEY where you need it."""
    created = request_tracker("POST", "/access-keys", json={"name": name, "expires_in_days": expires_in_days})
    click.echo(f"Created access key {created['key_id']}, expiring {created['expires_at']}.", err=True)
    click.echo("It is shown only once:", err=True)
    click.echo(created["key"])


@token.command("revoke")
@click.argument("key_id")
def token_revoke(key_id: str) -> None:
    """Delete an access key you created. The Tracker may accept it for up to two more hours."""
    request_tracker("DELETE", f"/access-keys/{quote(key_id, safe='')}")
    click.echo(f"Revoked access key {key_id}.")
