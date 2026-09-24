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
MODEL_ENV = "VALKYRIE_AGENT_MODEL"
VARIANT_ENV = "VALKYRIE_AGENT_VARIANT"
RUN_ID_ENV = "RUN_ID"
TASK_ID_ENV = "TASK_ID"

MINT_PATH = "/service-auth"
REVOKE_PATH = "/service-auth/revoke"
REQUEST_TIMEOUT_SECONDS = 30.0

# A token cannot be renewed, so it has to outlive the agent command by enough
# to cover setup and evaluation. The slack also keeps every value comfortably
# above the gateway's 60s floor; its ceiling is 24h.
MAX_TTL_SECONDS = 24 * 60 * 60
DEFAULT_TTL_SECONDS = 60 * 60
TTL_SLACK_SECONDS = 30 * 60


def _ttl_seconds(agent_timeout: float | None) -> int:
    if agent_timeout is None:
        return DEFAULT_TTL_SECONDS
    return int(min(agent_timeout + TTL_SLACK_SECONDS, MAX_TTL_SECONDS))


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
    identity: dict[str, str],
    agent_timeout: float | None,
) -> AsyncIterator[dict[str, str]]:
    """Yield the sandbox environment with its gateway key scoped to this task.

    The environment is returned untouched unless the contract asked for a
    gateway credential and the tracker attested the agent's model: only a model
    we resolved ourselves is safe to scope a token to.

    Minting failures are left to propagate. A task whose control plane is
    unreachable cannot reach the gateway to run either, and silently falling
    back to the static key would make the scoping unreliable and invisible.
    """
    url = env_vars.get(URL_ENV, "")
    api_key = env_vars.get(KEY_ENV, "")
    model = env_vars.get(MODEL_ENV, "")
    if not url or not api_key or not model:
        yield env_vars
        return

    lease = await _post(
        url,
        MINT_PATH,
        api_key,
        {
            "run_id": env_vars[RUN_ID_ENV],
            "task_id": env_vars[TASK_ID_ENV],
            "allowed_models": [model],
            "identity": identity,
            "variant": env_vars.get(VARIANT_ENV) or None,
            "ttl_seconds": _ttl_seconds(agent_timeout),
        },
    )
    logger.info(f"Scoped gateway credential to {model} for task {env_vars[TASK_ID_ENV]} (lease {lease['lease_id']})")

    try:
        yield {**env_vars, KEY_ENV: lease["token"]}
    finally:
        try:
            await _post(url, REVOKE_PATH, api_key, {"lease_id": lease["lease_id"]})
        except httpx.HTTPError as e:
            # Best effort: the token expires on its own TTL, and a completed
            # task should not fail over its credential teardown.
            logger.warning(f"Could not revoke gateway lease {lease['lease_id']}: {e}")
