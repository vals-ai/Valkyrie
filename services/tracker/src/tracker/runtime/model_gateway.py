"""Task-scoped Model Gateway credentials.

A contract that asks for `MODEL_GATEWAY_API_KEY` gets the static key resolved
from its secret, which lets a sandboxed agent call any model the gateway
serves. When the tracker resolved the agent's inference settings itself it
knows which model the task was assigned, and can hand the sandbox a token
scoped to that model instead, revoked once the task is done.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from tracker.config import AUTH_REQUIRED
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax


logger = get_logger(__name__)

URL_ENV = "MODEL_GATEWAY_URL"
KEY_ENV = "MODEL_GATEWAY_API_KEY"

MINT_PATH = "/service-auth"
REVOKE_PATH = "/service-auth/revoke"
REQUEST_TIMEOUT_SECONDS = 30.0
# Teardown holds the task's slot, so revoking gets a short leash rather than
# the patience minting needs: at worst REVOKE_ATTEMPTS of these plus backoff.
REVOKE_TIMEOUT_SECONDS = 5.0

# Revoking on teardown is what ends a credential's life. This is only the
# backstop for a tracker that died before it could, so it is the longest the
# gateway allows rather than a guess at how long the task needs: a sandbox
# waits for a creation permit, sets up, and may run an agent with no timeout of
# its own, and a credential that expires mid-task breaks the run.
TOKEN_TTL_SECONDS = 24 * 60 * 60

# Revoking is what makes that backstop irrelevant, so it is worth more than one
# attempt: the sandbox saw the token, and a transient failure at teardown would
# otherwise leave it usable for the rest of its life.
REVOKE_ATTEMPTS = 3
REVOKE_RETRY_DELAY_SECONDS = 0.5


def _control_plane_url(env_vars: dict[str, str], org_name: str) -> str:
    """Validate the gateway address before the tracker sends its key there.

    The address is a resolved secret, and a contract chooses which secret each
    of its variables reads, so this is a caller-influenced destination like any
    other the tracker talks to. Vals-owned hosts stay reachable because the
    gateway is one; private and link-local ones do not.
    """
    url = validate_service_url_syntax(env_vars[URL_ENV])
    validate_custom_service_destination(
        url,
        org_name=org_name,
        auth_required=AUTH_REQUIRED,
        restrict_vals_hosts=False,
    )
    return url


async def _post(
    url: str, path: str, api_key: str, payload: dict[str, Any], *, timeout: float = REQUEST_TIMEOUT_SECONDS
) -> httpx.Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{url.rstrip('/')}{path}",
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )
        response.raise_for_status()
        return response


@asynccontextmanager
async def task_scoped_gateway_key(
    env_vars: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    attested_model: str | None,
    variant: str,
    identity: dict[str, str],
    org_name: str,
) -> AsyncIterator[dict[str, str]]:
    """Yield the sandbox environment with its gateway key scoped to this task.

    Everything the token is scoped by is passed in rather than read back out of
    `env_vars`, whose keys a contract's own secrets can choose: only a model the
    tracker resolved itself is safe to scope a credential to. The environment is
    returned untouched when there is no attested model, or when the contract did
    not ask for a gateway credential at all.

    Minting failures are left to propagate. A task whose control plane is
    unreachable cannot reach the gateway to run either, and silently falling
    back to the static key would make the scoping unreliable and invisible.
    """
    api_key = env_vars.get(KEY_ENV, "")
    if not env_vars.get(URL_ENV) or not api_key or not attested_model:
        yield env_vars
        return

    url = _control_plane_url(env_vars, org_name)
    lease = (
        await _post(
            url,
            MINT_PATH,
            api_key,
            {
                "run_id": run_id,
                "task_id": task_id,
                "allowed_models": [attested_model],
                "identity": identity,
                "variant": variant or None,
                "ttl_seconds": TOKEN_TTL_SECONDS,
            },
        )
    ).json()
    logger.info(f"Scoped gateway credential to {attested_model} for task {task_id} (lease {lease['lease_id']})")

    try:
        yield {**env_vars, KEY_ENV: lease["token"]}
    finally:
        await _revoke(url, api_key, lease["lease_id"])


async def _revoke(url: str, api_key: str, lease_id: str) -> None:
    """End the credential's life, retrying a gateway that is briefly unwell.

    A completed task must not fail, or mask its own error, over its credential
    teardown, so this never raises. The response body is not read: an empty one
    would raise a decoding error that is not an `httpx.HTTPError`.
    """
    last_error: httpx.HTTPError | None = None
    for attempt in range(REVOKE_ATTEMPTS):
        try:
            _ = await _post(url, REVOKE_PATH, api_key, {"lease_id": lease_id}, timeout=REVOKE_TIMEOUT_SECONDS)
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                # Already gone, or never ours to revoke; retrying cannot help.
                logger.warning(f"Gateway refused to revoke lease {lease_id}: {e}")
                return
            last_error = e
        except httpx.HTTPError as e:
            last_error = e
        if attempt + 1 < REVOKE_ATTEMPTS:
            await asyncio.sleep(REVOKE_RETRY_DELAY_SECONDS * (attempt + 1))
    logger.warning(f"Could not revoke gateway lease {lease_id}, it stays live until it expires: {last_error}")
