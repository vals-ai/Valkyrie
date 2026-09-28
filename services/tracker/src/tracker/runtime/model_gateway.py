"""Task-scoped Model Gateway credentials.

A contract asking for `MODEL_GATEWAY_API_KEY` gets the static key, which
reaches every model the gateway serves. When the tracker resolved the agent's
model itself, the sandbox gets a token scoped to that model instead.
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

# Shared so tasks reuse connections rather than handshake one at a time.
_client = httpx.AsyncClient()


def _ttl_seconds(agent_timeout: float | None) -> int:
    """The agent's timeout plus room for the permit, build, install and eval.

    No renew, and expiring mid-task breaks the run, so an unbounded agent gets
    the gateway's ceiling, which is also the cap.
    """
    ceiling = 7 * 24 * 60 * 60
    if agent_timeout is None:
        return ceiling
    return min(int(agent_timeout) + 2 * 60 * 60, ceiling)


async def _post(url: str, path: str, api_key: str, payload: dict[str, Any], timeout: float) -> httpx.Response:
    response = await _client.post(
        f"{url}{path}",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=timeout,
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
    agent_timeout: float | None,
) -> AsyncIterator[dict[str, str]]:
    """Yield the sandbox environment with its gateway key scoped to this task.

    Scope comes from the tracker's values, never from `env_vars`, whose names a
    contract's secrets choose. A failed mint propagates: falling back to the
    static key would make the scoping silently unreliable.
    """
    api_key = env_vars.get("MODEL_GATEWAY_API_KEY", "")
    if not env_vars.get("MODEL_GATEWAY_URL") or not api_key or not attested_model:
        yield env_vars
        return

    # The address is a resolved secret, so check it before sending the key.
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)

    lease = (
        await _post(
            url,
            "/service-auth",
            api_key,
            {
                "run_id": run_id,
                "task_id": task_id,
                "allowed_models": [attested_model],
                "identity": identity,
                "variant": variant or None,
                "ttl_seconds": _ttl_seconds(agent_timeout),
            },
            30.0,
        )
    ).json()
    logger.info(f"Scoped gateway credential to {attested_model} for task {task_id} (lease {lease['lease_id']})")

    try:
        yield {**env_vars, "MODEL_GATEWAY_API_KEY": lease["token"]}
    finally:
        await _revoke(url, api_key, lease["lease_id"])


async def _revoke(url: str, api_key: str, lease_id: str) -> None:
    """End the credential, retrying a gateway that is briefly unwell.

    Never raises, and never reads the body: a finished task must not fail, or
    mask its own error, over teardown.
    """
    last_error: httpx.HTTPError | None = None
    for attempt in range(3):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            # Short timeout: teardown holds the task's sandbox slot.
            _ = await _post(url, "/service-auth/revoke", api_key, {"lease_id": lease_id}, 5.0)
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                # Already gone, or never ours; retrying cannot help.
                logger.warning(f"Gateway refused to revoke lease {lease_id}: {e}")
                return
            last_error = e
        except httpx.HTTPError as e:
            last_error = e
    logger.warning(f"Could not revoke gateway lease {lease_id}, it stays live until it expires: {last_error}")
