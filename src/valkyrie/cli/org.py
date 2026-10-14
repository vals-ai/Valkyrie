from __future__ import annotations

from typing import Any
from urllib.parse import quote

import click

from valkyrie.cli.client import request_tracker
from valkyrie.cli.display import format_table
from valkyrie.cli.login import show_organizations
from valkyrie.sdk import ValkyrieConfig
from valkyrie.sdk.errors import ValkyrieConfigError
from valkyrie.sdk.login import LoginSession, SessionAuth


def _saved_login() -> LoginSession:
    session = LoginSession.load(ValkyrieConfig().url)
    if session is None:
        raise click.ClickException("Organizations are managed with your login. Run `valkyrie login`.")
    return session


def _as_login(method: str, path: str, **kwargs: Any) -> Any:
    return request_tracker(method, path, auth=SessionAuth(_saved_login()), **kwargs)


def _switch_organization(organization: str) -> None:
    session = _saved_login()
    switched = session.model_copy(update={"tenant": organization, "expires_at": 0.0})
    switched.save()
    try:
        request_tracker("GET", "/auth/me", auth=SessionAuth(switched))
    except (click.ClickException, ValkyrieConfigError):
        current = LoginSession.load(session.tracker_url)
        if current is None or current.access_token == session.access_token:
            current = session
        current.model_copy(update={"tenant": session.tenant}).save()
        raise


@click.group()
def org() -> None:
    """Create, join and switch organizations, and manage their members."""


@org.command("list")
def org_list() -> None:
    """List the organizations your login belongs to; * marks the one your requests act for."""
    show_organizations(_as_login("GET", "/orgs"))


@org.command("create")
@click.argument("organization")
@click.option("--name", "display_name", help="Display name; defaults to the organization ID.")
def org_create(organization: str, display_name: str | None) -> None:
    """Create an organization, make yourself its admin, and switch to it.

    ORGANIZATION is its ID: 3 to 63 lowercase letters, digits, dots or hyphens.
    """
    created = _as_login("POST", "/orgs", json={"id": organization, "name": display_name})
    _switch_organization(created["id"])
    click.echo(f"Created {created['id']} and switched to it. You're its admin.")


@org.command("use")
@click.argument("organization")
def org_use(organization: str) -> None:
    """Act for another organization your login belongs to."""
    _switch_organization(organization)
    click.echo(f"Now acting for {organization}.")


@org.command("invite")
@click.argument("email")
def org_invite(email: str) -> None:
    """Invite someone to your current organization. Only admins can invite."""
    invited = _as_login("POST", "/orgs/members", json={"email": email})
    click.echo(
        f"Invited {invited['email']}. They can join by signing in with `valkyrie login` using that address "
        f"and running `valkyrie org accept {invited['organization']}`."
    )


@org.command("accept")
@click.argument("organization")
def org_accept(organization: str) -> None:
    """Join an organization that invited you, and switch to it."""
    _as_login("POST", f"/orgs/invitations/{quote(organization, safe='')}/accept")
    _switch_organization(organization)
    click.echo(f"Joined {organization} and switched to it.")


@org.command("decline")
@click.argument("organization")
def org_decline(organization: str) -> None:
    """Decline an invitation to an organization."""
    _as_login("DELETE", f"/orgs/invitations/{quote(organization, safe='')}")
    click.echo(f"Declined the invitation to {organization}.")


@org.command("members")
def org_members() -> None:
    """List the members and pending invitations of your current organization."""
    rows = [
        {
            "Email": member.get("email") or "",
            "Name": member.get("name") or "",
            "Role": "admin" if member["admin"] else "member",
            "Status": member["status"],
        }
        for member in _as_login("GET", "/orgs/members")["members"]
    ]
    format_table(rows, ["Email", "Name", "Role", "Status"], item_name="member")


@org.command("remove")
@click.argument("email")
def org_remove(email: str) -> None:
    """Remove someone from your current organization, or cancel their invitation. Only admins can remove."""
    _as_login("DELETE", f"/orgs/members/{quote(email, safe='')}")
    click.echo(f"Removed {email}.")
