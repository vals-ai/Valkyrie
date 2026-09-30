"""Task-scoped Model Gateway credentials."""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx

from tracker.config import AUTH_REQUIRED
from tracker.exceptions import CreditedTaskWallTimeExceeded
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax


logger = get_logger(__name__)
_client = httpx.AsyncClient()


CONTROLLED_GATEWAY_TTL_SECONDS = 7 * 24 * 60 * 60
CONTROLLED_TASK_WALL_SECONDS = CONTROLLED_GATEWAY_TTL_SECONDS - 2 * 60 * 60


def _wall_timeout_at(deadline_at: datetime) -> float:
    if deadline_at.tzinfo is None:
        deadline_at = deadline_at.replace(tzinfo=UTC)
    remaining = (deadline_at - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        raise CreditedTaskWallTimeExceeded("Credited task wall-time limit reached")
    return asyncio.get_running_loop().time() + remaining


def controlled_gateway_ttl_seconds() -> int:
    """Keep a two-hour teardown margin on a nonrenewable scoped credential."""
    # Trusted per-interval minting could replace this task-wide wall bound later.
    return CONTROLLED_GATEWAY_TTL_SECONDS


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
    accounting_control_token: str | None = None,
    credited_generation: bool = False,
    wall_deadline_at: datetime | None = None,
    on_wall_timeout_started: Callable[[asyncio.Timeout], None] | None = None,
) -> AsyncGenerator[dict[str, str]]:
    """Yield the sandbox environment with a task-scoped gateway key."""
    api_key = env_vars.get("MODEL_GATEWAY_API_KEY", "")
    if credited_generation and wall_deadline_at is None:
        raise ValueError("Credited generation requires a durable wall deadline")
    if not env_vars.get("MODEL_GATEWAY_URL") or not api_key or not attested_model:
        if accounting_session_id is not None:
            raise RuntimeError("Controlled task requires a native Model Gateway URL, API key, and attested model")
        if credited_generation:
            try:
                assert wall_deadline_at is not None
                async with asyncio.timeout_at(_wall_timeout_at(wall_deadline_at)) as wall:
                    if on_wall_timeout_started is not None:
                        on_wall_timeout_started(wall)
                    yield env_vars
            except TimeoutError as exc:
                if wall.expired():
                    raise CreditedTaskWallTimeExceeded("Credited task wall-time limit reached") from exc
                raise
        else:
            yield env_vars
        return

    # Both the native Gateway and the selected mint endpoint must be safe for the static key.
    url = validate_service_url_syntax(env_vars["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    if accounting_session_id is not None:
        if not accounting_gateway_url:
            raise RuntimeError("Controlled task requires an external service gateway URL")
        url = validate_service_url_syntax(accounting_gateway_url)
        validate_custom_service_destination(
            url, org_name=org_name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False
        )

    # Models other agents in the sandbox call alongside the main one.
    allowed_models = [attested_model, *(companion_models.split(",") if companion_models else [])]

    # Tokens cannot renew; unselected unbounded tasks retain the gateway's full cap.
    ttl_seconds = 7 * 24 * 60 * 60
    if credited_generation:
        ttl_seconds = controlled_gateway_ttl_seconds()
    elif agent_timeout is not None:
        ttl_seconds = min(int(agent_timeout) + 2 * 60 * 60, ttl_seconds)
    if accounting_session_id is not None and not accounting_control_token:
        raise RuntimeError("Controlled scoped Model Gateway mint requires a gateway control token")
    headers = {"Authorization": f"Bearer {api_key}"}
    mint_headers = headers
    if accounting_session_id is not None:
        assert accounting_control_token is not None
        mint_headers = {
            **headers,
            "X-SSP-Session-ID": accounting_session_id,
            "X-SSP-Control-Token": accounting_control_token,
        }
    wall_deadline = _wall_timeout_at(wall_deadline_at) if credited_generation and wall_deadline_at is not None else None
    mint_request = _client.post(
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
    if wall_deadline is None:
        response = await mint_request
    else:
        try:
            async with asyncio.timeout_at(wall_deadline) as mint_wall:
                response = await mint_request
        except TimeoutError as error:
            if mint_wall.expired():
                raise CreditedTaskWallTimeExceeded("Credited task wall-time limit reached") from error
            raise
    response.raise_for_status()
    lease = response.json()
    expires_at = datetime.fromtimestamp(lease["expires_at"], UTC).isoformat()
    logger.info(
        f"Scoped gateway credential to {', '.join(allowed_models)} for task {task_id} "
        f"(lease {lease['lease_id']}, expires {expires_at})"
    )

    try:
        if credited_generation:
            try:
                assert wall_deadline is not None
                async with asyncio.timeout_at(wall_deadline) as wall:
                    if on_wall_timeout_started is not None:
                        on_wall_timeout_started(wall)
                    yield {**env_vars, "MODEL_GATEWAY_API_KEY": lease["token"]}
            except TimeoutError as exc:
                if wall.expired():
                    raise CreditedTaskWallTimeExceeded("Credited task wall-time limit reached") from exc
                raise
        else:
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
