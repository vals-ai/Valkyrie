from __future__ import annotations

import base64
import functools
import json
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from click.testing import CliRunner

import valkyrie.cli.login as login_module
import valkyrie.cli.org as org_module
from valkyrie.cli.main import cli
from valkyrie.sdk import ValkyrieConfig
from valkyrie.sdk.config import DEFAULT_TRACKER_URL
from valkyrie.sdk.errors import ValkyrieConfigError
from valkyrie.sdk.login import LoginSession

TRACKER = "https://tracker.test"


@pytest.fixture
def memberships() -> list[dict[str, Any]]:
    return [{"id": "acme", "admin": True}, {"id": "beta", "admin": False}]


@pytest.fixture
def invitations() -> list[str]:
    return []


def _session_jwt() -> str:
    claims = base64.urlsafe_b64encode(json.dumps({"exp": time.time() + 600}).encode()).decode().rstrip("=")
    return f"header.{claims}.signature"


def _session(tracker_url: str, expires_in: float) -> LoginSession:
    return LoginSession(
        tracker_url=tracker_url,
        project_id="P1",
        refresh_url="https://login.test/v1/auth/refresh",
        access_token="access-1",
        expires_at=time.time() + expires_in,
    )


@pytest.fixture
def requests(
    monkeypatch: pytest.MonkeyPatch, memberships: list[dict[str, Any]], invitations: list[str]
) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        url = str(request.url)
        path = request.url.path
        if url == f"{TRACKER}/orgs" and request.method == "GET":
            current = request.headers.get("x-valkyrie-tenant")
            entries = [{"id": m["id"], "admin": m["admin"], "current": m["id"] == current} for m in memberships]
            listing = {"email": "alice@example.com", "organizations": entries, "invitations": invitations}
            return httpx.Response(200, json=listing)
        if url == f"{TRACKER}/orgs" and request.method == "POST":
            body = json.loads(request.content)
            memberships.append({"id": body["id"], "admin": True})
            return httpx.Response(201, json={"id": body["id"], "name": body["id"]})
        if url == f"{TRACKER}/orgs/members" and request.method == "POST":
            invited = {"organization": "acme", "email": json.loads(request.content)["email"]}
            return httpx.Response(200, json=invited)
        if url == f"{TRACKER}/orgs/members" and request.method == "GET":
            member = {"email": "bob@gmail.com", "name": None, "admin": False, "status": "invited"}
            return httpx.Response(200, json={"organization": "acme", "members": [member]})
        if path == "/orgs/members/bob@gmail.com":
            return httpx.Response(204)
        if path.startswith("/orgs/invitations/"):
            organization = path.removeprefix("/orgs/invitations/").removesuffix("/accept")
            if organization not in invitations:
                return httpx.Response(404, json={"detail": f"No invitation to {organization}"})
            invitations.remove(organization)
            if path.endswith("/accept"):
                memberships.append({"id": organization, "admin": False})
            return httpx.Response(204)
        if url == "https://login.test/v1/auth/refresh":
            return httpx.Response(200, json={"sessionJwt": _session_jwt(), "refreshJwt": "r-2"})
        if url == f"{TRACKER}/auth/me":
            if "authorization" not in request.headers and "x-api-key" not in request.headers:
                return httpx.Response(401, json={"detail": "Missing Authorization or x-api-key header"})
            organization = request.headers.get("x-valkyrie-tenant", "acme")
            if organization not in [membership["id"] for membership in memberships]:
                return httpx.Response(403, json={"detail": "This login is not a member of the requested organization"})
            credential = "login" if "authorization" in request.headers else "access_key"
            identity = {"organization": organization, "email": "alice@example.com", "credential": credential}
            return httpx.Response(200, json=identity)
        if url == f"{TRACKER}/access-keys":
            assert json.loads(request.content) == {"name": "ci", "expires_in_days": 7}
            return httpx.Response(200, json={"key_id": "K1", "key": "ci-secret", "expires_at": "2026-10-15T00:00:00Z"})
        if url == f"{TRACKER}/access-keys/..%2FK1":
            return httpx.Response(204)
        return httpx.Response(404)

    def device_login(tracker_url: str, http: httpx.Client, announce: Callable[[str, str], None]) -> LoginSession:
        announce("https://login.test/activate", "ABCD-EFGH")
        return _session(tracker_url, 600).model_copy(update={"refresh_token": "r-1"})

    monkeypatch.setattr(httpx, "Client", functools.partial(httpx.Client, transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(login_module, "device_login", device_login)
    monkeypatch.setattr(login_module.click, "launch", lambda _url: 0)
    monkeypatch.setenv("VALKYRIE_URL", TRACKER)
    return seen


@pytest.mark.parametrize(
    ("memberships", "invitations", "listing"),
    [
        ([{"id": "acme", "admin": True}, {"id": "beta", "admin": False}], [], "  acme (admin)\n  beta\n"),
        ([], ["acme"], "  acme (invited; run `valkyrie org accept acme`)\n"),
        ([], [], "You're not in an organization yet. Ask Vals AI to add you"),
    ],
)
def test_login_saves_a_session_and_lists_the_organizations(requests: list[httpx.Request], listing: str) -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["login"])
    listed = runner.invoke(cli, ["org", "list"])

    assert result.exit_code == 0, result.output
    assert "ABCD-EFGH" in result.stderr
    assert result.stdout.startswith(f"Logged in to {TRACKER} as alice@example.com.\n{listing}")
    assert listed.output.startswith(listing)
    assert LoginSession.load(TRACKER) is not None
    assert requests[-1].headers["authorization"] == "Bearer access-1"


def test_login_picks_the_tracker_until_logout(requests: list[httpx.Request], monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["login"])
    monkeypatch.delenv("VALKYRIE_URL")

    whoami = runner.invoke(cli, ["whoami"])
    assert whoami.exit_code == 0, whoami.output
    assert whoami.output == f"alice@example.com in acme on {TRACKER} (using a login)\n"

    runner.invoke(cli, ["logout"], env={"VALKYRIE_URL": "https://other.test"})
    assert ValkyrieConfig().url == TRACKER

    logout = runner.invoke(cli, ["logout"])
    assert logout.output == f"Logged out of {TRACKER}.\n"
    assert LoginSession.load(TRACKER) is None
    assert ValkyrieConfig().url == DEFAULT_TRACKER_URL

    monkeypatch.setenv("VALKYRIE_URL", TRACKER)

    signed_out = runner.invoke(cli, ["whoami"])
    assert signed_out.exit_code == 1
    assert "valkyrie login" in signed_out.output

    monkeypatch.setenv("VALKYRIE_API_KEY", "ci-secret")
    with_key = runner.invoke(cli, ["whoami"])
    assert with_key.output == f"alice@example.com in acme on {TRACKER} (using an access key)\n"
    assert requests[-1].headers["x-api-key"] == "ci-secret"


def test_token_create_prints_only_the_key_on_stdout(requests: list[httpx.Request]) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["login"])

    result = runner.invoke(cli, ["token", "create", "ci", "--expires-in-days", "7"])

    assert result.exit_code == 0, result.output
    assert result.stdout == "ci-secret\n"
    assert "K1" in result.stderr
    revoke = runner.invoke(cli, ["token", "revoke", "../K1"])
    assert revoke.output == "Revoked access key ../K1.\n"


def test_an_expired_login_is_reported_without_a_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKYRIE_URL", "http://tracker")
    _session("http://tracker", -10).save()

    result = CliRunner().invoke(cli, ["agent", "list"])

    assert result.exit_code == 1
    assert "valkyrie login" in result.output
    assert not isinstance(result.exception, ValkyrieConfigError)


def test_creating_an_organization_refreshes_the_login_and_switches_to_it(requests: list[httpx.Request]) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["login"])

    result = runner.invoke(cli, ["org", "create", "acme-labs"])

    assert result.exit_code == 0, result.output
    assert "Created acme-labs and switched to it. You're its admin." in result.output
    created = next(request for request in requests if request.method == "POST" and request.url.path == "/orgs")
    assert json.loads(created.content) == {"id": "acme-labs", "name": None}
    assert any(str(request.url) == "https://login.test/v1/auth/refresh" for request in requests)
    session = LoginSession.load(TRACKER)
    assert session is not None and session.tenant == "acme-labs"


def test_org_use_switches_only_to_an_organization_the_tracker_accepts(
    requests: list[httpx.Request], monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["login"])

    switched = runner.invoke(cli, ["org", "use", "beta"])
    listed = runner.invoke(cli, ["org", "list"])
    refused = runner.invoke(cli, ["org", "use", "gamma"])

    def unavailable(*args: Any, **kwargs: Any) -> None:
        raise ValkyrieConfigError("Refreshing your login failed (503)")

    monkeypatch.setattr(org_module, "request_tracker", unavailable)
    failed = runner.invoke(cli, ["org", "use", "acme"])

    assert switched.output == "Now acting for beta.\n"
    assert listed.output == "  acme (admin)\n* beta\n"
    assert refused.exit_code == 1 and "not a member" in refused.output
    assert failed.exit_code != 0
    session = LoginSession.load(TRACKER)
    assert session is not None and session.tenant == "beta"


def test_org_invite_members_and_remove(requests: list[httpx.Request]) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["login"])

    invited = runner.invoke(cli, ["org", "invite", "bob@gmail.com"])
    members = runner.invoke(cli, ["org", "members"])
    removed = runner.invoke(cli, ["org", "remove", "bob@gmail.com"])

    assert "`valkyrie org accept acme`" in invited.output
    assert "bob@gmail.com" in members.output and "invited" in members.output
    assert removed.output == "Removed bob@gmail.com.\n"
    assert requests[-1].url.raw_path == b"/orgs/members/bob%40gmail.com"


def test_invitations_can_be_accepted_or_declined(requests: list[httpx.Request], invitations: list[str]) -> None:
    invitations.extend(["gamma", "delta"])
    runner = CliRunner()
    runner.invoke(cli, ["login"])

    accepted = runner.invoke(cli, ["org", "accept", "gamma"])
    declined = runner.invoke(cli, ["org", "decline", "delta"])
    missing = runner.invoke(cli, ["org", "decline", "delta"])

    assert accepted.output == "Joined gamma and switched to it.\n"
    assert declined.output == "Declined the invitation to delta.\n"
    assert missing.exit_code == 1 and "No invitation to delta" in missing.output
    session = LoginSession.load(TRACKER)
    assert session is not None and session.tenant == "gamma"


def test_organization_commands_never_use_an_access_key(
    requests: list[httpx.Request], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VALKYRIE_API_KEY", "ci-key")

    result = CliRunner().invoke(cli, ["org", "list"])

    assert result.exit_code == 1
    assert "Organizations are managed with your login. Run `valkyrie login`." in result.output
    assert requests == []


def test_login_to_a_tracker_without_organizations_reports_its_organization(
    requests: list[httpx.Request], monkeypatch: pytest.MonkeyPatch
) -> None:
    request_tracker = login_module.request_tracker

    def without_organizations(method: str, path: str, **kwargs: Any) -> Any:
        return request_tracker(method, "/missing" if path == "/orgs" else path, **kwargs)

    monkeypatch.setattr(login_module, "request_tracker", without_organizations)
    result = CliRunner().invoke(cli, ["login"])

    assert result.exit_code == 0, result.output
    assert f"Logged in to {TRACKER} as alice@example.com for acme." in result.output
