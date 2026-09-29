"""Task-scoped Model Gateway credentials."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx

from tracker.config import AUTH_REQUIRED
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax


logger = get_logger(__name__)
_client = httpx.AsyncClient()


@asynccontextmanager
async def task_scoped_gateway_key(
    env_vars: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    attested_model: str | None,
    identity: dict[str, str],
    org_name: str,
    agent_timeout: float | None,
) -> AsyncGenerator[dict[str, str]]:
    """Yield the sandbox environment with a task-scoped gateway key."""
    api_key = env_vars.get("MODEL_GATEWAY_API_KEY", "")
    if not env_vars.get("MODEL_GATEWAY_URL") or not api_key or not attested_model:
        yield env_vars
        return

    # The address is a resolved secret; validate before sending the static key.
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)

    # Tokens cannot renew; an unbounded agent needs the gateway's full cap.
    ttl_seconds = 7 * 24 * 60 * 60
    if agent_timeout is not None:
        ttl_seconds = min(int(agent_timeout) + 2 * 60 * 60, ttl_seconds)
    headers = {"Authorization": f"Bearer {api_key}"}
    response = await _client.post(
        f"{url}/service-auth",
        headers=headers,
        json={
            "run_id": run_id,
            "task_id": task_id,
            "allowed_models": [attested_model],
            "identity": identity,
            "ttl_seconds": ttl_seconds,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    lease = response.json()
    expires_at = datetime.fromtimestamp(lease["expires_at"], UTC).isoformat()
    logger.info(
        f"Scoped gateway credential to {attested_model} for task {task_id} "
        f"(lease {lease['lease_id']}, expires {expires_at})"
    )

    try:
        yield {**env_vars, "MODEL_GATEWAY_API_KEY": lease["token"]}
    finally:
        # Teardown must not mask a task error or hold the sandbox slot long.
        try:
            response = await _client.post(
                f"{url}/service-auth/revoke",
                headers=headers,
                json={"lease_id": lease["lease_id"]},
                timeout=5.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(f"Could not revoke gateway lease {lease['lease_id']}: {exc}")
