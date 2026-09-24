"""Task-scoped Model Gateway credentials.

A contract that asks for `MODEL_GATEWAY_API_KEY` gets the static key resolved
from its secret, which lets a sandboxed agent call any model the gateway
serves. When the tracker resolved the agent's inference settings itself it
knows which model the task was assigned, and can hand the sandbox a token
scoped to that model instead, revoked once the task is done.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from tracker.logging import get_logger


logger = get_logger(__name__)

URL_ENV = "MODEL_GATEWAY_URL"
KEY_ENV = "MODEL_GATEWAY_API_KEY"

MINT_PATH = "/service-auth"
REVOKE_PATH = "/service-auth/revoke"
REQUEST_TIMEOUT_SECONDS = 30.0

# Revoking on teardown is what ends a credential's life. This is only the
# backstop for a tracker that died before it could, so it is the longest the
# gateway allows rather than a guess at how long the task needs: a sandbox
# waits for a creation permit, sets up, and may run an agent with no timeout of
# its own, and a credential that expires mid-task breaks the run.
TOKEN_TTL_SECONDS = 24 * 60 * 60


async def _post(url: str, path: str, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        response = await client.post(
            f"{url.rstrip('/')}{path}",
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )
        response.raise_for_status()
        return response.json()


@asynccontextmanager
async def task_scoped_gateway_key(
    env_vars: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    attested_model: str | None,
    variant: str,
    identity: dict[str, str],
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
    url = env_vars.get(URL_ENV, "")
    api_key = env_vars.get(KEY_ENV, "")
    if not url or not api_key or not attested_model:
        yield env_vars
        return

    lease = await _post(
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
    logger.info(f"Scoped gateway credential to {attested_model} for task {task_id} (lease {lease['lease_id']})")

    try:
        yield {**env_vars, KEY_ENV: lease["token"]}
    finally:
        try:
            await _post(url, REVOKE_PATH, api_key, {"lease_id": lease["lease_id"]})
        except httpx.HTTPError as e:
            # Best effort: the token expires on its own TTL, and a completed
            # task should not fail over its credential teardown.
            logger.warning(f"Could not revoke gateway lease {lease['lease_id']}: {e}")
