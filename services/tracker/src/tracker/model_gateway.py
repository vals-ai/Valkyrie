"""Administrative client for task-scoped model gateway capabilities."""

import asyncio
import hashlib
import math
import os
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from tracker.exceptions import TrackerServiceError


CAPABILITY_FINALIZATION_GRACE_SECONDS = 5 * 60
MAX_CAPABILITY_LIFETIME_SECONDS = 24 * 60 * 60
CAPABILITY_DRAIN_TIMEOUT_SECONDS = 60.0
CAPABILITY_ACTION_TIMEOUT_SECONDS = 140.0
CAPABILITY_ACTION_MAX_ATTEMPTS = 2
CAPABILITY_ACTION_RETRY_SECONDS = 1.0

assert 2 * CAPABILITY_ACTION_TIMEOUT_SECONDS < CAPABILITY_FINALIZATION_GRACE_SECONDS


class ModelGatewayError(TrackerServiceError):
    """A task capability could not be safely created or finalized."""


class CapabilityMintRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    run_id: str
    task_id: str
    model: str
    config: dict[str, bool | float | int | str]
    sandbox_id: str
    identity: dict[str, object]
    expires_at: int
    max_queries: int = Field(ge=1, le=2000)
    max_sessions: int = Field(ge=1, le=16)


class CapabilityMintResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    capability_id: str
    token: str = Field(repr=False)
    state: Literal["active"]
    expires_at: int

    @field_validator("capability_id")
    @classmethod
    def validate_capability_id(cls, value: str) -> str:
        if not value.startswith("cap_"):
            raise ValueError("invalid capability id")
        return value

    @field_validator("token")
    @classmethod
    def validate_token(cls, value: str) -> str:
        if not value.startswith("mgc_"):
            raise ValueError("invalid capability token")
        return value


class CapabilityUsageSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    capability_id: str
    state: Literal["sealed", "revoked"]
    drained: bool
    session_count: int = Field(ge=0)
    query_count: int = Field(ge=0)
    completed_queries: int = Field(ge=0)
    total_input_tokens: int = Field(ge=0)
    total_output_tokens: int = Field(ge=0)
    cost_usd: Decimal = Field(ge=0)


class CapabilityEvalResumeState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["model_gateway_eval_resume"]
    capability_id: str = Field(pattern=r"^cap_[A-Za-z0-9_-]+$")
    benchmark_state: dict[str, Any]


def capability_expires_at(agent_timeout: float, now: float) -> int:
    if not math.isfinite(agent_timeout) or agent_timeout <= 0:
        raise ModelGatewayError("Task capability requires a finite positive agent_timeout")

    lifetime = math.ceil(agent_timeout) + CAPABILITY_FINALIZATION_GRACE_SECONDS
    if lifetime > MAX_CAPABILITY_LIFETIME_SECONDS:
        raise ModelGatewayError("Task capability agent_timeout exceeds the 24-hour capability lifetime")
    return math.floor(now) + lifetime


def _model_gateway_origin() -> str:
    gateway_url = os.environ.get("MODEL_GATEWAY_URL", "")
    parsed_url = httpx.URL(gateway_url)
    if (
        parsed_url.scheme != "https"
        or not parsed_url.host
        or parsed_url.userinfo
        or parsed_url.path != "/"
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise ModelGatewayError("MODEL_GATEWAY_URL must be an absolute HTTPS URL")
    if parsed_url.port == 443:
        parsed_url = parsed_url.copy_with(port=None)
    return str(parsed_url).rstrip("/")


def model_gateway_origin_sha256() -> str:
    return hashlib.sha256(_model_gateway_origin().encode("utf-8")).hexdigest()


class ModelGatewayAdminClient:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @classmethod
    def from_environment(cls) -> "ModelGatewayAdminClient":
        gateway_url = _model_gateway_origin()
        admin_key = os.environ.get("MODEL_GATEWAY_ADMIN_API_KEY", "")
        if not admin_key or admin_key != admin_key.strip():
            raise ModelGatewayError("MODEL_GATEWAY_ADMIN_API_KEY is required for task capabilities")

        client = httpx.AsyncClient(
            base_url=gateway_url,
            headers={"Authorization": f"Bearer {admin_key}"},
            timeout=65.0,
        )
        return cls(client)

    @property
    def gateway_url(self) -> str:
        return str(self._client.base_url).rstrip("/")

    async def __aenter__(self) -> "ModelGatewayAdminClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self._client.aclose()

    async def mint(self, request: CapabilityMintRequest) -> CapabilityMintResponse:
        response = await self._post("/capabilities/mint", request.model_dump(mode="json"))
        try:
            minted = CapabilityMintResponse.model_validate_json(response.content)
        except ValidationError:
            raise ModelGatewayError("Model gateway returned an invalid mint response") from None
        return minted

    async def finalize(self, capability_id: str) -> CapabilityUsageSummary:
        try:
            await self._action("seal", capability_id, "sealed")
        except ModelGatewayError:
            pass

        revoked = await self._action("revoke", capability_id, "revoked")
        if not revoked.drained:
            raise ModelGatewayError("Task capability did not drain before revocation")
        return revoked

    async def _action(
        self,
        action: Literal["seal", "revoke"],
        capability_id: str,
        expected_state: Literal["sealed", "revoked"],
    ) -> CapabilityUsageSummary:
        try:
            async with asyncio.timeout(CAPABILITY_ACTION_TIMEOUT_SECONDS):
                for attempt in range(CAPABILITY_ACTION_MAX_ATTEMPTS):
                    try:
                        response = await self._client.post(
                            f"/capabilities/{action}",
                            json={
                                "capability_id": capability_id,
                                "wait_timeout_seconds": CAPABILITY_DRAIN_TIMEOUT_SECONDS,
                            },
                        )
                    except httpx.TransportError:
                        if attempt + 1 == CAPABILITY_ACTION_MAX_ATTEMPTS:
                            raise ModelGatewayError(f"Model gateway {action} request failed after retries") from None
                    else:
                        if response.status_code == 200:
                            try:
                                summary = CapabilityUsageSummary.model_validate_json(response.content)
                            except ValidationError:
                                raise ModelGatewayError(
                                    f"Model gateway returned an invalid {action} response"
                                ) from None
                            if summary.capability_id != capability_id or summary.state != expected_state:
                                raise ModelGatewayError(f"Model gateway returned a mismatched {action} response")
                            return summary
                        if response.status_code != 503 or attempt + 1 == CAPABILITY_ACTION_MAX_ATTEMPTS:
                            raise ModelGatewayError(
                                f"Model gateway administrative request failed with HTTP {response.status_code}"
                            )

                    await asyncio.sleep(CAPABILITY_ACTION_RETRY_SECONDS)
        except TimeoutError:
            raise ModelGatewayError(f"Model gateway {action} exceeded its finalization deadline") from None

        raise AssertionError("unreachable")

    async def _post(self, path: str, body: dict[str, object]) -> httpx.Response:
        try:
            response = await self._client.post(path, json=body)
        except httpx.HTTPError:
            raise ModelGatewayError("Model gateway administrative request failed") from None
        if response.status_code == 200:
            return response

        raise ModelGatewayError(f"Model gateway administrative request failed with HTTP {response.status_code}")


async def finalize_capability_uninterruptibly(
    client: ModelGatewayAdminClient,
    capability_id: str,
    write_usage: Callable[[CapabilityUsageSummary], Awaitable[None]],
) -> CapabilityUsageSummary:
    async def finalize_and_write() -> CapabilityUsageSummary:
        summary = await client.finalize(capability_id)
        await write_usage(summary)
        return summary

    task = asyncio.create_task(finalize_and_write())
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error

    summary = task.result()
    if cancellation is not None:
        raise cancellation
    return summary


async def mint_capability_uninterruptibly(
    client: ModelGatewayAdminClient,
    request: CapabilityMintRequest,
    write_usage: Callable[[CapabilityUsageSummary], Awaitable[None]],
) -> CapabilityMintResponse:
    task = asyncio.create_task(client.mint(request))
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error

    minted = task.result()
    if cancellation is not None:
        await finalize_capability_uninterruptibly(client, minted.capability_id, write_usage)
        raise cancellation
    return minted
