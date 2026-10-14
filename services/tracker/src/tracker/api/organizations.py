from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, StringConstraints
from sqlalchemy import ColumnElement
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, delete, select

from tracker import config
from tracker.auth import (
    LoginIdentity,
    descope_call,
    descope_management,
    find_org_by_tenant,
    get_current_login,
    primary_login_id,
    user_tenant_roles,
)
from tracker.config import DESCOPE_ORG_ADMIN_ROLE
from tracker.database.models import DEFAULT_ORG_NAME, OrganizationInvite
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.secret_access import may_use_shared_secrets

logger = get_logger(__name__)
router = APIRouter(tags=["organizations"])

_RESERVED_IDS = frozenset({DEFAULT_ORG_NAME, "admin", "api", "root", "support", "system"})
_RESERVED_PREFIXES = ("vals", "valkyrie", "interview-")
_MEMBERS_PAGE_SIZE = 100
_MAX_ADMIN_ORGANIZATIONS = 5
_INVITATION_LIFETIME = timedelta(days=14)


class OrganizationEntry(BaseModel):
    id: str
    admin: bool
    current: bool


class OrganizationsResponse(BaseModel):
    email: str | None
    organizations: list[OrganizationEntry]
    invitations: list[str]


class CreateOrganizationRequest(BaseModel):
    id: str = Field(
        pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
        description="Lowercase letters, digits, dots and hyphens; 3 to 63 characters",
    )
    name: str | None = Field(default=None, max_length=100)


class OrganizationResponse(BaseModel):
    id: str
    name: str


class InviteRequest(BaseModel):
    email: Annotated[
        str,
        StringConstraints(strip_whitespace=True, to_lower=True, max_length=254, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    ]


class InviteResponse(BaseModel):
    organization: str
    email: str


class MemberEntry(BaseModel):
    email: str | None
    name: str | None
    admin: bool
    status: Literal["active", "invited"]


class MembersResponse(BaseModel):
    organization: str
    members: list[MemberEntry]


def _find_user(management: Any, email: str) -> dict[str, Any] | None:
    users = management.user.search_all(emails=[email], limit=1).get("users")
    return users[0] if users else None


def _require_admin(login: LoginIdentity) -> str:
    organization = login.current_organization
    if DESCOPE_ORG_ADMIN_ROLE not in login.organizations[organization]:
        raise HTTPException(status_code=403, detail="Only organization admins can manage members")
    return organization


def _verified_email(login: LoginIdentity) -> str:
    if login.email is None:
        raise HTTPException(status_code=403, detail="Verify your email address with Descope first")
    return login.email


def _reserved(organization: str) -> bool:
    return (
        organization in _RESERVED_IDS
        or organization.startswith(_RESERVED_PREFIXES)
        or may_use_shared_secrets(organization)
    )


def _live() -> ColumnElement[bool]:
    return col(OrganizationInvite.created_at) > datetime.now(UTC) - _INVITATION_LIFETIME


def _delete_invitation(session: Session, organization: str, email: str) -> bool:
    statement = delete(OrganizationInvite).where(
        col(OrganizationInvite.organization) == organization, col(OrganizationInvite.email) == email, _live()
    )
    return session.exec(statement).rowcount > 0


@router.get("/orgs", response_model=OrganizationsResponse)
def list_organizations(
    login: LoginIdentity = Depends(get_current_login), session: Session = Depends(get_session)
) -> OrganizationsResponse:
    """List this login's organizations and the invitations waiting for its verified email."""
    invitations = select(OrganizationInvite.organization).where(OrganizationInvite.email == login.email, _live())
    return OrganizationsResponse(
        email=login.email,
        organizations=[
            OrganizationEntry(
                id=organization,
                admin=DESCOPE_ORG_ADMIN_ROLE in roles,
                current=organization == login.organization,
            )
            for organization, roles in login.organizations.items()
        ],
        invitations=list(session.exec(invitations)),
    )


@router.post("/orgs", response_model=OrganizationResponse, status_code=201)
def create_organization(
    request: CreateOrganizationRequest,
    login: LoginIdentity = Depends(get_current_login),
    session: Session = Depends(get_session),
) -> OrganizationResponse:
    """Create an organization with the caller as its admin, on a Tracker that allows self-serve organizations."""
    if not config.SELF_SERVE_ORGS_ENABLED:
        raise HTTPException(
            status_code=403,
            detail="Valkyrie doesn't create organizations on request. Ask Vals AI to add you, "
            "or ask an organization admin to run `valkyrie org invite EMAIL`.",
        )
    _verified_email(login)
    if _reserved(request.id) or find_org_by_tenant(request.id, session) is not None:
        raise HTTPException(status_code=409, detail=f"Organization {request.id!r} already exists")
    if sum(DESCOPE_ORG_ADMIN_ROLE in roles for roles in login.organizations.values()) >= _MAX_ADMIN_ORGANIZATIONS:
        raise HTTPException(
            status_code=403, detail=f"A login can be the admin of at most {_MAX_ADMIN_ORGANIZATIONS} organizations"
        )
    name = request.name or request.id
    management: Any = descope_management()
    with descope_call("create the organization"):
        if management.tenant.search_all(ids=[request.id]).get("tenants"):
            raise HTTPException(status_code=409, detail=f"Organization {request.id!r} already exists")
        if management.tenant.search_all(names=[name]).get("tenants"):
            raise HTTPException(status_code=409, detail=f"An organization is already named {name!r}")
        management.tenant.create(name=name, id=request.id)
        try:
            management.user.add_tenant(login.login_id, request.id)
            management.user.add_tenant_roles(login.login_id, request.id, [DESCOPE_ORG_ADMIN_ROLE])
        except Exception:
            try:
                management.tenant.delete(request.id)
            except Exception as rollback_error:
                logger.error(
                    "Failed to delete an organization left without an admin: %s",
                    type(rollback_error).__name__,
                    extra={"organization": request.id},
                )
            raise
    logger.info("Created organization", extra={"organization": request.id})
    return OrganizationResponse(id=request.id, name=name)


@router.get("/orgs/members", response_model=MembersResponse)
def list_members(
    login: LoginIdentity = Depends(get_current_login), session: Session = Depends(get_session)
) -> MembersResponse:
    """List the members and pending invitations of the organization this login acts for."""
    organization = login.current_organization
    management: Any = descope_management()
    members: list[MemberEntry] = []
    with descope_call("list the members"):
        for page in itertools.count():
            response = management.user.search_all(tenant_ids=[organization], limit=_MEMBERS_PAGE_SIZE, page=page)
            users = response.get("users") or []
            members.extend(
                MemberEntry(
                    email=user.get("email"),
                    name=user.get("name"),
                    admin=DESCOPE_ORG_ADMIN_ROLE in user_tenant_roles(user).get(organization, ()),
                    status="active",
                )
                for user in users
            )
            if len(users) < _MEMBERS_PAGE_SIZE:
                break
    invited = select(OrganizationInvite.email).where(OrganizationInvite.organization == organization, _live())
    members.extend(
        MemberEntry(email=email, name=None, admin=False, status="invited") for email in session.exec(invited)
    )
    return MembersResponse(organization=organization, members=members)


@router.post("/orgs/members", response_model=InviteResponse)
def invite_member(
    request: InviteRequest,
    login: LoginIdentity = Depends(get_current_login),
    session: Session = Depends(get_session),
) -> InviteResponse:
    """Invite someone to the organization this admin acts for. They join only after signing in and accepting.

    Inviting again restarts the invitation's 14 days.
    """
    organization = _require_admin(login)
    with descope_call("invite the member"):
        user = _find_user(descope_management(), request.email)
    if user is not None and organization in user_tenant_roles(user):
        raise HTTPException(status_code=409, detail=f"{request.email} is already a member")
    session.merge(OrganizationInvite(organization=organization, email=request.email, invited_by=login.user_id))
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
    return InviteResponse(organization=organization, email=request.email)


@router.delete("/orgs/members/{email}", status_code=204)
def remove_member(
    email: str, login: LoginIdentity = Depends(get_current_login), session: Session = Depends(get_session)
) -> None:
    """Remove a member, or cancel an invitation, in the organization this admin acts for.

    A removed member's access keys for the organization are deleted and their login sessions ended.
    """
    organization = _require_admin(login)
    email = email.strip().lower()
    cancelled = _delete_invitation(session, organization, email)
    management: Any = descope_management()
    with descope_call("remove the member"):
        user = _find_user(management, email)
        if user is not None and organization in user_tenant_roles(user):
            if user["userId"] == login.user_id:
                raise HTTPException(status_code=400, detail="You can't remove yourself from the organization")
            for key in management.access_key.search_all_access_keys(tenant_ids=[organization]).get("keys") or []:
                if (key.get("customClaims") or {}).get("user_id") == user["userId"]:
                    management.access_key.delete(key["id"])
            management.user.logout_user_by_user_id(user["userId"])
            management.user.remove_tenant(primary_login_id(user) or email, organization)
        elif not cancelled:
            raise HTTPException(status_code=404, detail=f"{email} is not a member of this organization")
    session.commit()


@router.post("/orgs/invitations/{organization}/accept", status_code=204)
def accept_invitation(
    organization: str, login: LoginIdentity = Depends(get_current_login), session: Session = Depends(get_session)
) -> None:
    """Join an organization that invited this login's verified email."""
    email = _verified_email(login)
    if not _delete_invitation(session, organization, email):
        raise HTTPException(status_code=404, detail=f"No invitation to {organization} for {email}")
    management: Any = descope_management()
    with descope_call("join the organization"):
        exists = bool(management.tenant.search_all(ids=[organization]).get("tenants"))
        if exists and organization not in login.organizations:
            management.user.add_tenant(login.login_id, organization)
    session.commit()
    if not exists:
        raise HTTPException(status_code=404, detail=f"Organization {organization!r} does not exist")


@router.delete("/orgs/invitations/{organization}", status_code=204)
def decline_invitation(
    organization: str, login: LoginIdentity = Depends(get_current_login), session: Session = Depends(get_session)
) -> None:
    """Decline an invitation to this login's verified email."""
    email = _verified_email(login)
    if not _delete_invitation(session, organization, email):
        raise HTTPException(status_code=404, detail=f"No invitation to {organization} for {email}")
    session.commit()
