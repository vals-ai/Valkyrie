from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Literal, cast

from descope.exceptions import AuthException
from descope.management.common import AssociatedTenant
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from tracker.auth import (
    USER_KEY_CLAIM,
    LoginIdentity,
    RequestIdentity,
    descope_access_keys,
    descope_call,
    get_current_login,
    get_current_starter,
)
from tracker.config import AUTH_REQUIRED, DESCOPE_BASE_URL, DESCOPE_CLI_CLIENT_ID, DESCOPE_PROJECT_ID

router = APIRouter(tags=["login"])


class LoginSettingsResponse(BaseModel):
    device_url: str
    token_url: str
    client_id: str
    project_id: str
    refresh_url: str


class IdentityResponse(BaseModel):
    organization: str
    email: str | None
    name: str | None
    credential: Literal["login", "access_key", "none"]


class CreateAccessKeyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    expires_in_days: int = Field(default=30, ge=1, le=90)


class AccessKeyResponse(BaseModel):
    key_id: str
    key: str = Field(repr=False)
    expires_at: datetime


@router.get("/auth/cli", response_model=LoginSettingsResponse)
def login_settings() -> LoginSettingsResponse:
    """Tell the CLI where to sign in; 404 when this Tracker has no CLI login."""
    if not (AUTH_REQUIRED and DESCOPE_PROJECT_ID and DESCOPE_CLI_CLIENT_ID):
        raise HTTPException(status_code=404, detail="This Tracker does not support `valkyrie login`")
    return LoginSettingsResponse(
        device_url=f"{DESCOPE_BASE_URL}/oauth2/v1/{DESCOPE_PROJECT_ID}/device",
        token_url=f"{DESCOPE_BASE_URL}/oauth2/v1/{DESCOPE_PROJECT_ID}/token",
        client_id=DESCOPE_CLI_CLIENT_ID,
        project_id=DESCOPE_PROJECT_ID,
        refresh_url=f"{DESCOPE_BASE_URL}/v1/auth/refresh",
    )


@router.get("/auth/me", response_model=IdentityResponse)
def current_identity(identity: RequestIdentity = Depends(get_current_starter)) -> IdentityResponse:
    """Return the organization and user behind the request's credential."""
    return IdentityResponse(
        organization=identity.org.name,
        email=identity.email,
        name=identity.name,
        credential="access_key" if identity.access_key_id else "login" if identity.user_id else "none",
    )


@router.post("/access-keys", response_model=AccessKeyResponse)
async def create_access_key(
    request: CreateAccessKeyRequest, login: LoginIdentity = Depends(get_current_login)
) -> AccessKeyResponse:
    """Create an expiring access key for the logged-in user's organization, for CI and other headless use."""
    organization = login.current_organization
    access_keys: Any = descope_access_keys()
    expires_at = int(time.time()) + request.expires_in_days * 86400
    claims = {"user_id": login.user_id, USER_KEY_CLAIM: True, **({"email": login.email} if login.email else {})}
    with descope_call("create the access key"):
        response = await asyncio.to_thread(
            access_keys.create,
            name=request.name,
            expire_time=expires_at,
            key_tenants=[AssociatedTenant(organization)],
            custom_claims=claims,
            description="Created with `valkyrie token create`",
        )
    return AccessKeyResponse(
        key_id=str(response["key"]["id"]),
        key=str(response["cleartext"]),
        expires_at=datetime.fromtimestamp(expires_at).astimezone(),
    )


@router.delete("/access-keys/{key_id}", status_code=204)
async def revoke_access_key(key_id: str, login: LoginIdentity = Depends(get_current_login)) -> None:
    """Delete an access key the logged-in user created for this organization."""
    organization = login.current_organization
    access_keys: Any = descope_access_keys()
    with descope_call("load the access key"):
        try:
            key = cast(dict[str, Any], (await asyncio.to_thread(access_keys.load, key_id)).get("key") or {})
        except AuthException as exc:
            if exc.status_code != 404:
                raise
            raise HTTPException(status_code=404, detail="Access key does not exist") from exc
    claims = key.get("customClaims")
    tenants = {tenant.get("tenantId") for tenant in key.get("keyTenants") or [] if isinstance(tenant, dict)}
    if not isinstance(claims, dict) or claims.get("user_id") != login.user_id or tenants != {organization}:
        raise HTTPException(status_code=404, detail="Access key does not exist")
    with descope_call("delete the access key"):
        await asyncio.to_thread(access_keys.delete, key_id)
