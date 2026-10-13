"""Store an organization's own secrets for its runs to reference."""

import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from tracker.auth import get_current_login, get_current_org
from tracker.aws.resolver import resolve_org_aws_runtime
from tracker.aws.secrets import SecretBeingDeletedError, SecretsManagerStore
from tracker.aws.tags import ORG_ID_TAG
from tracker.database.models import Org
from tracker.config import AUTH_REQUIRED, DESCOPE_ORG_ADMIN_ROLE, VALKYRIE_STAGE
from tracker.exceptions import SecretNotFoundError
from tracker.local import config as local_config
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax
from tracker.secret_access import may_use_shared_secrets, resolve_secret_name, tenant_secret_prefix
from tracker.types import SecretEntry, SecretsResponse, SecretValueRequest

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/secrets")
SecretName = Annotated[str, Path(pattern=r"^[A-Za-z0-9_+=.@-]{1,128}$")]
_MAX_VALUE_BYTES = 65536
_PROVIDER_URL_FIELDS = ("DAYTONA_API_URL",)
ADMINS_ONLY = {"description": "This organization lets only signed-in admins change its secrets"}


def require_secret_writer(request: Request, org: Org = Depends(get_current_org)) -> Org:
    if (VALKYRIE_STAGE, org.name) != ("bench", "vals.ai"):
        return org
    if DESCOPE_ORG_ADMIN_ROLE not in get_current_login(request).organizations.get(org.name, ()):
        logger.info("Refused a secret change from a non-admin", extra={"org_id": str(org.id)})
        raise HTTPException(
            status_code=403, detail=f"Only {DESCOPE_ORG_ADMIN_ROLE}s of {org.name} can change its secrets."
        )
    return org


def _check_provider_urls(value: str, org: Org) -> None:
    """Keep sandbox-provider endpoints in a tenant's own secrets off private and internal hosts."""
    try:
        fields = json.loads(value)
    except ValueError:
        return
    if not isinstance(fields, dict):
        return
    for field in _PROVIDER_URL_FIELDS:
        url = fields.get(field)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if url is None:
            continue
        try:
            validate_custom_service_destination(
                validate_service_url_syntax(str(url)),  # pyright: ignore[reportUnknownArgumentType]
                org_name=org.name,
                auth_required=AUTH_REQUIRED,
                restrict_vals_hosts=False,
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=f"{field} is not an allowed destination") from error


def _entry(name: str, org: Org) -> SecretEntry:
    """Report the name runs use: the short name wherever it resolves to this secret, else the full one."""
    stored = f"{tenant_secret_prefix(org.id)}{name}"
    resolved = resolve_secret_name(name, org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
    return SecretEntry(name=name, reference=name if resolved == stored else stored)


def secret_store(org: Org) -> SecretsManagerStore:
    if local_config.resources is not None:
        raise HTTPException(status_code=400, detail="Local runs read secrets from their secrets file")
    return SecretsManagerStore(resolve_org_aws_runtime(org).clients)


async def require_own_secret(name: str, org: Org) -> None:
    """Refuse a secret name that is not in the organization's own store, unless it may use shared secrets."""
    if not AUTH_REQUIRED or may_use_shared_secrets(org.name):
        return
    reference = resolve_secret_name(name, org_id=org.id, org_name=org.name, auth_required=True)
    try:
        if not reference.startswith(tenant_secret_prefix(org.id)):
            raise SecretNotFoundError(reference)
        await secret_store(org).get(reference)
    except SecretNotFoundError as error:
        raise HTTPException(
            status_code=404, detail=f"Secret '{name}' does not exist; create it with `valkyrie secret set {name}`"
        ) from error


@router.get("", response_model=SecretsResponse)
async def list_secrets_endpoint(org: Org = Depends(get_current_org)) -> SecretsResponse:
    """List the organization's secrets by name, never their values."""
    prefix = tenant_secret_prefix(org.id)
    references = await secret_store(org).list_names(prefix)
    return SecretsResponse(secrets=[_entry(reference.removeprefix(prefix), org) for reference in references])


@router.put(
    "/{name}",
    response_model=SecretEntry,
    responses={
        400: {"description": "Invalid secret value"},
        403: ADMINS_ONLY,
        409: {"description": "A secret with this name is still being deleted"},
    },
)
async def set_secret_endpoint(
    name: SecretName, body: SecretValueRequest, org: Org = Depends(require_secret_writer)
) -> SecretEntry:
    """Create the secret or replace its value. Runs reference it by the returned reference."""
    try:
        size = len(body.value.encode())
    except UnicodeEncodeError:
        size = 0
    if not 0 < size <= _MAX_VALUE_BYTES:
        raise HTTPException(status_code=400, detail=f"Secret values are 1 to {_MAX_VALUE_BYTES} bytes of UTF-8")
    _check_provider_urls(body.value, org)
    reference = f"{tenant_secret_prefix(org.id)}{name}"
    try:
        await secret_store(org).put(reference, body.value, tags={ORG_ID_TAG: str(org.id)})
    except SecretBeingDeletedError as error:
        raise HTTPException(status_code=409, detail=f"Secret '{name}' is still being deleted; retry shortly") from error
    logger.info("Saved secret %s", reference)
    return _entry(name, org)


@router.delete(
    "/{name}", response_model=SecretEntry, responses={403: ADMINS_ONLY, 404: {"description": "Secret does not exist"}}
)
async def remove_secret_endpoint(name: SecretName, org: Org = Depends(require_secret_writer)) -> SecretEntry:
    """Delete the secret immediately; it cannot be recovered.

    Without a tenant access role, AWS can't tell a missing name apart, so deleting one also succeeds.
    """
    reference = f"{tenant_secret_prefix(org.id)}{name}"
    if not await secret_store(org).delete(reference):
        raise HTTPException(status_code=404, detail=f"Secret '{name}' does not exist")
    logger.info("Deleted secret %s", reference)
    return _entry(name, org)
