"""Task-scoped Model Gateway credentials."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from math import ceil, isfinite

import httpx

from tracker.config import AUTH_REQUIRED
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax


logger = get_logger(__name__)
_client = httpx.AsyncClient()


def controlled_gateway_ttl_seconds(agent_timeout: float, credit_cap_seconds: float) -> int:
    """Cover the maximum controlled deadline and the existing two-hour teardown grace."""
    ttl = agent_timeout + credit_cap_seconds + 2 * 60 * 60
    gateway_max_seconds = 7 * 24 * 60 * 60
    if not isfinite(ttl) or ttl > gateway_max_seconds:
        raise ValueError(f"Controlled scoped Model Gateway TTL exceeds {gateway_max_seconds} seconds")
    return ceil(ttl)


@asynccontextmanager
async def task_scoped_gateway_key(
    env_vars: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    attested_model: str | None,
    companion_models: str | None,
    identity: dict[str, str],
    org_name: str,
    agent_timeout: float | None,
    accounting_session_id: str | None = None,
    accounting_gateway_url: str | None = None,
    credit_cap_seconds: float | None = None,
) -> AsyncGenerator[dict[str, str]]:
    """Yield the sandbox environment with a task-scoped gateway key."""
    api_key = env_vars.get("MODEL_GATEWAY_API_KEY", "")
    if not env_vars.get("MODEL_GATEWAY_URL") or not api_key or not attested_model:
        if accounting_session_id is not None:
            raise RuntimeError("Controlled task requires a native Model Gateway URL, API key, and attested model")
        yield env_vars
        return

    # Both the native Gateway and the selected mint endpoint must be safe for the static key.
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    if accounting_session_id is not None:
        if not accounting_gateway_url:
            raise RuntimeError("Controlled task requires an external service gateway URL")
        url = validate_service_url_syntax(accounting_gateway_url)
        validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)

    # Models other agents in the sandbox call alongside the main one.
    allowed_models = [attested_model, *(companion_models.split(",") if companion_models else [])]

    # Tokens cannot renew; unselected unbounded tasks retain the gateway's full cap.
    ttl_seconds = 7 * 24 * 60 * 60
    if accounting_session_id is not None:
        if agent_timeout is None or credit_cap_seconds is None:
            raise ValueError("Controlled scoped Model Gateway TTL requires a base timeout and credit cap")
        ttl_seconds = controlled_gateway_ttl_seconds(agent_timeout, credit_cap_seconds)
    elif agent_timeout is not None:
        ttl_seconds = min(int(agent_timeout) + 2 * 60 * 60, ttl_seconds)
    headers = {"Authorization": f"Bearer {api_key}"}
    mint_headers = (
        {**headers, "X-SSP-Session-ID": accounting_session_id} if accounting_session_id is not None else headers
    )
    response = await _client.post(
        f"{url}/service-auth",
        headers=mint_headers,
        json={
            "run_id": run_id,
            "task_id": task_id,
            "allowed_models": allowed_models,
            "identity": identity,
            "ttl_seconds": ttl_seconds,
        },
        timeout=30.0,
    )
    response.raise_for_status()
    lease = response.json()
    expires_at = datetime.fromtimestamp(lease["expires_at"], UTC).isoformat()
    logger.info(
        f"Scoped gateway credential to {', '.join(allowed_models)} for task {task_id} "
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
