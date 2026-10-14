from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from descope.exceptions import AuthException
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from main import app
from tracker.api import organizations
from tracker.auth import NO_ORGANIZATION, NOT_A_MEMBER, SEVERAL_ORGANIZATIONS, LoginIdentity, get_current_login
from tracker.database.models import Org, OrganizationInvite

_client = TestClient(app)
ADMIN = "Valkyrie Admin"
BOB = "bob@gmail.com"


class _Tenants:
    def __init__(self) -> None:
        self.tenants: dict[str, dict[str, Any]] = {}
        self.fail_delete = False
        self.unavailable = False

    def create(self, name: str, id: str) -> None:
        self.tenants[id] = {"id": id, "name": name}

    def delete(self, id: str) -> None:
        if self.fail_delete:
            raise RuntimeError("Descope is unavailable")
        self.tenants.pop(id)

    def search_all(self, ids: list[str] | None = None, names: list[str] | None = None) -> dict[str, Any]:
        if self.unavailable:
            raise ConnectionError("Descope is unavailable")
        found = [
            t
            for t in self.tenants.values()
            if (ids is None or t["id"] in ids) and (names is None or t["name"] in names)
        ]
        return {"tenants": found}


class _AccessKeys:
    def __init__(self) -> None:
        self.keys: dict[str, dict[str, Any]] = {}

    def search_all_access_keys(self, tenant_ids: list[str]) -> dict[str, Any]:
        found = [k for k in self.keys.values() if {t["tenantId"] for t in k["keyTenants"]} & set(tenant_ids)]
        return {"keys": found}

    def delete(self, id: str) -> None:
        del self.keys[id]


class _Users:
    def __init__(self) -> None:
        self.users: dict[str, dict[str, Any]] = {}
        self.logged_out: list[str] = []

    def logout_user_by_user_id(self, user_id: str) -> None:
        self.logged_out.append(user_id)

    def add(self, user_id: str, login_id: str, email: str) -> None:
        self.users[login_id] = {
            "userId": user_id,
            "loginIds": [login_id],
            "email": email,
            "name": None,
            "userTenants": [],
        }

    def _tenants(self, login_id: str) -> list[dict[str, Any]]:
        return self.users[login_id]["userTenants"]

    def add_tenant(self, login_id: str, tenant_id: str) -> None:
        if any(tenant["tenantId"] == tenant_id for tenant in self._tenants(login_id)):
            raise AuthException(400, "E112104", json.dumps({"errorDescription": "User already in tenant"}))
        self._tenants(login_id).append({"tenantId": tenant_id, "roleNames": []})

    def add_tenant_roles(self, login_id: str, tenant_id: str, role_names: list[str]) -> None:
        if not set(role_names) <= {ADMIN}:
            raise AuthException(400, "E024213", json.dumps({"errorDescription": "Role not found"}))
        for tenant in self._tenants(login_id):
            if tenant["tenantId"] == tenant_id:
                tenant["roleNames"] = [*tenant["roleNames"], *role_names]

    def remove_tenant(self, login_id: str, tenant_id: str) -> None:
        self.users[login_id]["userTenants"] = [t for t in self._tenants(login_id) if t["tenantId"] != tenant_id]

    def search_all(
        self,
        emails: list[str] | None = None,
        tenant_ids: list[str] | None = None,
        limit: int = 0,
        page: int = 0,
    ) -> dict[str, Any]:
        users = [
            user
            for user in self.users.values()
            if (emails is None or user["email"] in emails)
            and (tenant_ids is None or any(t["tenantId"] in tenant_ids for t in user["userTenants"]))
        ]
        return {"users": users[page * limit : (page + 1) * limit] if limit else users}


@pytest.fixture
def descope(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    management = SimpleNamespace(tenant=_Tenants(), user=_Users(), access_key=_AccessKeys())
    management.user.add("U-alice", "google-alice", "alice@acme.com")
    monkeypatch.setattr(organizations, "descope_management", lambda: management)
    return management


@pytest.fixture
def bob(descope: SimpleNamespace) -> None:
    descope.user.add("U-bob", BOB, BOB)


@pytest.fixture
def logs(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    monkeypatch.setattr("tracker.auth.logger.handlers", [caplog.handler])
    return caplog


def _as(
    monkeypatch: pytest.MonkeyPatch,
    organizations: dict[str, tuple[str, ...]],
    current: str | None,
    *,
    email: str = "alice@acme.com",
    verified: bool = True,
    user_id: str = "U-alice",
    login_id: str = "google-alice",
) -> None:
    login = LoginIdentity(user_id, login_id, email if verified else None, organizations, current)
    monkeypatch.setitem(app.dependency_overrides, get_current_login, lambda: login)


def _as_bob(
    monkeypatch: pytest.MonkeyPatch, organizations: dict[str, tuple[str, ...]] | None = None, *, verified: bool = True
) -> None:
    _as(monkeypatch, organizations or {}, None, email=BOB, verified=verified, user_id="U-bob", login_id=BOB)


def _invite(database_session: Session, email: str, organization: str = "acme", age: timedelta = timedelta()) -> None:
    created_at = datetime.now(UTC) - age
    database_session.add(
        OrganizationInvite(organization=organization, email=email, invited_by="U-alice", created_at=created_at)
    )
    database_session.commit()


@pytest.mark.parametrize("organization", ["acme", "interview"])
def test_creating_an_organization_makes_the_creator_its_admin(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, organization: str
) -> None:
    _as(monkeypatch, {}, None)

    response = _client.post("/orgs", json={"id": organization, "name": "Acme"})

    assert response.status_code == 201, response.text
    assert response.json() == {"id": organization, "name": "Acme"}
    assert descope.user.users["google-alice"]["userTenants"] == [{"tenantId": organization, "roleNames": [ADMIN]}]


def test_unverified_logins_cannot_create_organizations(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace
) -> None:
    _as(monkeypatch, {}, None, verified=False)

    assert _client.post("/orgs", json={"id": "acme"}).status_code == 403
    assert descope.tenant.tenants == {}


@pytest.mark.parametrize(
    "organization", ["default", "valkyrie-ci", "partner-evil", "legacy-team", "interview-candidate"]
)
def test_existing_and_reserved_organizations_cannot_be_taken_over(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session, organization: str
) -> None:
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "vals.ai,partner*")
    database_session.add(Org(name="legacy-team"))
    database_session.commit()
    _as(monkeypatch, {}, None)

    response = _client.post("/orgs", json={"id": organization})

    assert response.status_code == 409
    assert descope.tenant.tenants == {}


def test_existing_tenants_and_names_are_refused(monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace) -> None:
    descope.tenant.create(name="Acme", id="acme")
    _as(monkeypatch, {}, None)

    taken = _client.post("/orgs", json={"id": "acme"})
    named = _client.post("/orgs", json={"id": "acme-three", "name": "Acme"})

    assert (taken.status_code, named.status_code) == (409, 409)
    assert set(descope.tenant.tenants) == {"acme"}


def test_a_login_can_administer_a_limited_number_of_organizations(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace
) -> None:
    _as(monkeypatch, {f"org-{n}": (ADMIN,) for n in range(5)}, "org-0")

    response = _client.post("/orgs", json={"id": "acme"})

    assert response.status_code == 403
    assert descope.tenant.tenants == {}


@pytest.mark.parametrize(
    ("unavailable", "fail_delete"), [(True, False), (False, False), (False, True)], ids=["outage", "role", "rollback"]
)
def test_a_failed_create_is_rolled_back(
    monkeypatch: pytest.MonkeyPatch,
    descope: SimpleNamespace,
    logs: pytest.LogCaptureFixture,
    unavailable: bool,
    fail_delete: bool,
) -> None:
    monkeypatch.setattr(organizations, "DESCOPE_ORG_ADMIN_ROLE", "Missing Role")
    descope.tenant.unavailable = unavailable
    descope.tenant.fail_delete = fail_delete
    _as(monkeypatch, {}, None)

    assert _client.post("/orgs", json={"id": "acme"}).status_code == 503
    assert ("acme" in descope.tenant.tenants) is fail_delete
    failure = "ConnectionError" if unavailable else "AuthException 400 Role not found"
    assert f"Descope failed to create the organization: {failure}" in logs.text


def test_invites_need_the_invitees_acceptance(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    descope.tenant.create(name="Acme", id="acme")
    descope.user.add("U-carol", "carol-sso", "carol@other.com")
    _as(monkeypatch, {"acme": (ADMIN,)}, "acme")

    new = _client.post("/orgs/members", json={"email": "Bob@Gmail.com"})
    existing = _client.post("/orgs/members", json={"email": "carol@other.com"})
    invalid = _client.post("/orgs/members", json={"email": "bob"})

    assert new.json() == {"organization": "acme", "email": "bob@gmail.com"}
    assert existing.json() == {"organization": "acme", "email": "carol@other.com"}
    assert invalid.status_code == 422
    assert "bob@gmail.com" not in descope.user.users
    assert descope.user.users["carol-sso"]["userTenants"] == []

    _as(monkeypatch, {}, None, email="carol@other.com", user_id="U-carol", login_id="carol-sso")
    assert _client.get("/orgs").json()["invitations"] == ["acme"]
    accepted = _client.post("/orgs/invitations/acme/accept")

    assert accepted.status_code == 204, accepted.text
    assert accepted.content == b""
    assert descope.user.users["carol-sso"]["userTenants"] == [{"tenantId": "acme", "roleNames": []}]
    remaining = database_session.exec(select(OrganizationInvite.email)).all()
    assert remaining == ["bob@gmail.com"]


@pytest.mark.usefixtures("bob")
def test_operator_created_interview_organization_can_be_joined_and_selected(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    organization = "interview-candidate"
    descope.tenant.create(name="Interview Candidate", id=organization)
    descope.user.add_tenant("google-alice", organization)
    descope.user.add_tenant_roles("google-alice", organization, [ADMIN])
    _as(monkeypatch, {organization: (ADMIN,)}, organization)

    invited = _client.post("/orgs/members", json={"email": BOB})
    assert (invited.status_code, invited.json()) == (200, {"organization": organization, "email": BOB})

    _as_bob(monkeypatch)
    assert _client.get("/orgs").json()["invitations"] == [organization]
    accepted = _client.post(f"/orgs/invitations/{organization}/accept")
    assert accepted.status_code == 204, accepted.text
    assert descope.user.users[BOB]["userTenants"] == [{"tenantId": organization, "roleNames": []}]

    _as(monkeypatch, {organization: ()}, organization, email=BOB, user_id="U-bob", login_id=BOB)
    listed = _client.get("/orgs")
    members = _client.get("/orgs/members")
    assert listed.status_code == 200
    assert listed.json()["organizations"] == [{"id": organization, "admin": False, "current": True}]
    assert members.status_code == 200
    assert members.json()["organization"] == organization


@pytest.mark.usefixtures("bob")
def test_invitations_go_only_to_the_verified_invitee(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    _invite(database_session, BOB)

    _as(monkeypatch, {}, None, email="mallory@gmail.com", user_id="U-mallory", login_id="mallory")
    assert _client.post("/orgs/invitations/acme/accept").status_code == 404
    _as_bob(monkeypatch, verified=False)
    assert _client.post("/orgs/invitations/acme/accept").status_code == 403
    _as_bob(monkeypatch)
    assert _client.delete("/orgs/invitations/acme").status_code == 204

    assert database_session.exec(select(OrganizationInvite)).all() == []
    assert descope.user.users[BOB]["userTenants"] == []


@pytest.mark.parametrize(
    ("memberships", "current", "status", "detail"),
    [
        ({}, None, 400, NO_ORGANIZATION),
        ({"acme": (ADMIN,), "beta": ()}, None, 400, SEVERAL_ORGANIZATIONS),
        ({"beta": (ADMIN,)}, "acme", 403, NOT_A_MEMBER),
        ({"acme": ()}, "acme", 403, "Only organization admins can manage members"),
    ],
    ids=["none", "several", "removed", "member"],
)
def test_only_admins_of_the_chosen_organization_manage_members(
    monkeypatch: pytest.MonkeyPatch,
    descope: SimpleNamespace,
    memberships: dict[str, tuple[str, ...]],
    current: str | None,
    status: int,
    detail: str,
) -> None:
    _as(monkeypatch, memberships, current)

    responses = [_client.post("/orgs/members", json={"email": BOB}), _client.delete(f"/orgs/members/{BOB}")]

    assert {(response.status_code, response.json()["detail"]) for response in responses} == {(status, detail)}
    assert _client.get("/orgs").status_code == 200


def test_members_and_invitations_are_listed_and_removed(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    descope.user.add_tenant("google-alice", "acme")
    descope.user.add_tenant_roles("google-alice", "acme", [ADMIN])
    descope.user.add("U-carol", "carol-sso", "carol@other.com")
    descope.user.add_tenant("carol-sso", "acme")
    descope.access_key.keys = {
        key_id: {"id": key_id, "keyTenants": [{"tenantId": tenant}], "customClaims": {"user_id": user_id}}
        for key_id, tenant, user_id in [
            ("K-carol-acme", "acme", "U-carol"),
            ("K-carol-beta", "beta", "U-carol"),
            ("K-alice-acme", "acme", "U-alice"),
        ]
    }
    _invite(database_session, BOB)
    _as(monkeypatch, {"acme": (ADMIN,)}, "acme")

    listed = _client.get("/orgs/members").json()
    reinvited = _client.post("/orgs/members", json={"email": "carol@other.com"})
    removed = _client.delete("/orgs/members/carol@other.com")
    cancelled = _client.delete("/orgs/members/bob@gmail.com")
    yourself = _client.delete("/orgs/members/alice@acme.com")
    stranger = _client.delete("/orgs/members/nobody@acme.com")

    assert listed == {
        "organization": "acme",
        "members": [
            {"email": "alice@acme.com", "name": None, "admin": True, "status": "active"},
            {"email": "carol@other.com", "name": None, "admin": False, "status": "active"},
            {"email": "bob@gmail.com", "name": None, "admin": False, "status": "invited"},
        ],
    }
    assert reinvited.status_code == 409
    assert "already a member" in reinvited.json()["detail"]
    assert [r.status_code for r in (removed, cancelled, yourself, stranger)] == [204, 204, 400, 404]
    assert descope.user.users["carol-sso"]["userTenants"] == []
    assert set(descope.access_key.keys) == {"K-carol-beta", "K-alice-acme"}
    assert descope.user.logged_out == ["U-carol"]
    assert database_session.exec(select(OrganizationInvite)).all() == []


@pytest.mark.usefixtures("bob")
def test_invitations_expire_after_two_weeks(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    descope.tenant.create(name="Acme", id="acme")
    _invite(database_session, BOB, age=timedelta(days=15))
    _as_bob(monkeypatch)

    assert _client.get("/orgs").json()["invitations"] == []
    assert _client.post("/orgs/invitations/acme/accept").status_code == 404
    assert descope.user.users[BOB]["userTenants"] == []

    _as(monkeypatch, {"acme": (ADMIN,)}, "acme")
    assert _client.post("/orgs/members", json={"email": BOB}).status_code == 200
    _as_bob(monkeypatch)
    assert _client.get("/orgs").json()["invitations"] == ["acme"]


@pytest.mark.usefixtures("bob")
def test_an_invitation_to_a_deleted_organization_is_dropped(
    monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace, database_session: Session
) -> None:
    _invite(database_session, BOB)
    _as_bob(monkeypatch)

    response = _client.post("/orgs/invitations/acme/accept")

    assert response.status_code == 404
    assert "does not exist" in response.json()["detail"]
    assert database_session.exec(select(OrganizationInvite)).all() == []
    assert descope.user.users[BOB]["userTenants"] == []


def test_members_are_listed_across_pages(monkeypatch: pytest.MonkeyPatch, descope: SimpleNamespace) -> None:
    monkeypatch.setattr(organizations, "_MEMBERS_PAGE_SIZE", 2)
    for n in range(5):
        descope.user.add(f"U-{n}", f"user-{n}", f"user-{n}@acme.com")
        descope.user.add_tenant(f"user-{n}", "acme")
    _as(monkeypatch, {"acme": ()}, "acme")

    members = _client.get("/orgs/members").json()["members"]

    assert [member["email"] for member in members] == [f"user-{n}@acme.com" for n in range(5)]
