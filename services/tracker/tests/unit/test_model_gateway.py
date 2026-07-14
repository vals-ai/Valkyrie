import asyncio
import hashlib
import json
import math
from decimal import Decimal

import httpx
import pytest

from tracker.model_gateway import (
    CAPABILITY_FINALIZATION_GRACE_SECONDS,
    MAX_CAPABILITY_LIFETIME_SECONDS,
    CapabilityMintRequest,
    CapabilityUsageSummary,
    ModelGatewayAdminClient,
    ModelGatewayError,
    capability_expires_at,
    finalize_capability_uninterruptibly,
    mint_capability_uninterruptibly,
    model_gateway_origin_sha256,
)


def _mint_request() -> CapabilityMintRequest:
    return CapabilityMintRequest(
        run_id="run-1",
        task_id="task-1",
        model="openai/gpt-5.5",
        config={"client_scope": "shared", "max_tokens": 8192},
        sandbox_id="sandbox-1",
        identity={"org_id": "org-1", "agent_name": "osworld_agent"},
        expires_at=10_000,
        max_queries=800,
        max_sessions=4,
    )


def _summary(capability_id: str, state: str, *, drained: bool = True) -> dict[str, object]:
    return {
        "capability_id": capability_id,
        "state": state,
        "drained": drained,
        "session_count": 2,
        "query_count": 3,
        "completed_queries": 3,
        "total_input_tokens": 100,
        "total_output_tokens": 40,
        "cost_usd": "1.25",
    }


async def test_admin_client_mints_exact_claims_without_exposing_admin_key() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer admin-secret-sentinel"
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "capability_id": "cap_123",
                "token": "mgc_task-token",
                "state": "active",
                "expires_at": 10_000,
            },
        )

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
        headers={"Authorization": "Bearer admin-secret-sentinel"},
    )
    async with ModelGatewayAdminClient(http_client) as client:
        minted = await client.mint(_mint_request())

    assert minted.capability_id == "cap_123"
    assert minted.token == "mgc_task-token"
    assert requests == [_mint_request().model_dump(mode="json")]
    assert "admin-secret-sentinel" not in repr(minted)


async def test_finalize_seals_then_revokes_and_returns_usage() -> None:
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        summary = await client.finalize("cap_123")

    assert actions == ["seal", "revoke"]
    assert summary.state == "revoked"
    assert summary.drained is True
    assert summary.cost_usd == Decimal("1.25")


async def test_finalize_accepts_drained_revoke_after_seal_outage() -> None:
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        if action == "seal":
            return httpx.Response(503, json={"code": "admin-secret-sentinel"})
        return httpx.Response(200, json=_summary("cap_123", "revoked"))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        summary = await client.finalize("cap_123")

    assert actions == ["seal", "seal", "revoke"]
    assert summary.state == "revoked"
    assert summary.drained is True


async def test_finalize_accepts_drained_revoke_after_undrained_seal() -> None:
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state, drained=action != "seal"))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        summary = await client.finalize("cap_123")

    assert actions == ["seal", "revoke"]
    assert summary.state == "revoked"
    assert summary.drained is True


async def test_finalize_rejects_undrained_revoke() -> None:
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state, drained=action == "seal"))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        with pytest.raises(ModelGatewayError, match="did not drain"):
            await client.finalize("cap_123")

    assert actions == ["seal", "revoke"]


async def test_finalize_retries_transient_transport_and_503_failures() -> None:
    attempts = {"seal": 0, "revoke": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        attempts[action] += 1
        if action == "seal" and attempts[action] == 1:
            raise httpx.ConnectError("transient", request=request)
        if action == "revoke" and attempts[action] == 1:
            return httpx.Response(503)
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        summary = await client.finalize("cap_123")

    assert attempts == {"seal": 2, "revoke": 2}
    assert summary.state == "revoked"


async def test_finalize_fails_after_bounded_persistent_outage() -> None:
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        actions.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(503)

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        with pytest.raises(ModelGatewayError, match="HTTP 503"):
            await client.finalize("cap_123")

    assert actions == ["seal", "seal", "revoke", "revoke"]


async def test_mint_does_not_retry_503() -> None:
    requests = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503)

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        with pytest.raises(ModelGatewayError, match="HTTP 503"):
            await client.mint(_mint_request())

    assert requests == 1


async def test_finalization_finishes_before_delivering_cancellation() -> None:
    write_started = asyncio.Event()
    release_write = asyncio.Event()
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state))

    async def write_usage(summary: CapabilityUsageSummary) -> None:
        assert summary.state == "revoked"
        actions.append("write")
        write_started.set()
        await release_write.wait()

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )
    async with ModelGatewayAdminClient(http_client) as client:
        finalization = asyncio.create_task(finalize_capability_uninterruptibly(client, "cap_123", write_usage))
        await write_started.wait()
        finalization.cancel()
        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await finalization

    assert actions == ["seal", "revoke", "write"]


async def test_stop_during_mint_finalizes_before_delivering_cancellation() -> None:
    mint_started = asyncio.Event()
    release_mint = asyncio.Event()
    actions: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        action = request.url.path.rsplit("/", 1)[-1]
        actions.append(action)
        if action == "mint":
            mint_started.set()
            await release_mint.wait()
            return httpx.Response(
                200,
                json={
                    "capability_id": "cap_123",
                    "token": "mgc_task-token",
                    "state": "active",
                    "expires_at": 10_000,
                },
            )
        state = "sealed" if action == "seal" else "revoked"
        return httpx.Response(200, json=_summary("cap_123", state))

    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gateway.example.test",
    )

    async def write_usage(summary: CapabilityUsageSummary) -> None:
        assert summary.state == "revoked"
        actions.append("write")

    async with ModelGatewayAdminClient(http_client) as client:
        mint = asyncio.create_task(mint_capability_uninterruptibly(client, _mint_request(), write_usage))
        await mint_started.wait()
        mint.cancel()
        release_mint.set()
        with pytest.raises(asyncio.CancelledError):
            await mint

    assert actions == ["mint", "seal", "revoke", "write"]


@pytest.mark.parametrize("agent_timeout", [0.0, -1.0, math.inf, -math.inf, math.nan])
def test_capability_expiry_requires_finite_positive_timeout(agent_timeout: float) -> None:
    with pytest.raises(ModelGatewayError, match="finite positive"):
        capability_expires_at(agent_timeout, 1_000.5)


def test_capability_expiry_includes_finalization_grace() -> None:
    assert capability_expires_at(120.1, 1_000.9) == 1_000 + 121 + CAPABILITY_FINALIZATION_GRACE_SECONDS


def test_capability_expiry_rejects_more_than_twenty_four_hours() -> None:
    timeout = MAX_CAPABILITY_LIFETIME_SECONDS - CAPABILITY_FINALIZATION_GRACE_SECONDS + 0.1
    with pytest.raises(ModelGatewayError, match="24-hour"):
        capability_expires_at(timeout, 1_000)


def test_environment_requires_https_gateway_and_admin_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "http://gateway.example.test")
    monkeypatch.setenv("MODEL_GATEWAY_ADMIN_API_KEY", "admin-key")
    with pytest.raises(ModelGatewayError, match="absolute HTTPS"):
        ModelGatewayAdminClient.from_environment()

    monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gateway.example.test")
    monkeypatch.delenv("MODEL_GATEWAY_ADMIN_API_KEY")
    with pytest.raises(ModelGatewayError, match="is required"):
        ModelGatewayAdminClient.from_environment()


def test_model_gateway_origin_sha256_hashes_canonical_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "HTTPS://Gateway.Example.Test:443/")

    expected = hashlib.sha256(b"https://gateway.example.test").hexdigest()

    assert model_gateway_origin_sha256() == expected


@pytest.mark.parametrize(
    "gateway_url",
    [
        "http://gateway.example.test",
        "https://gateway.example.test/path",
        "https://gateway.example.test?query=value",
        "https://user@gateway.example.test",
    ],
)
def test_model_gateway_origin_sha256_requires_https_origin(
    monkeypatch: pytest.MonkeyPatch,
    gateway_url: str,
) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", gateway_url)

    with pytest.raises(ModelGatewayError, match="absolute HTTPS"):
        model_gateway_origin_sha256()


async def test_environment_builds_authenticated_admin_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "HTTPS://Gateway.Example.Test:443/")
    monkeypatch.setenv("MODEL_GATEWAY_ADMIN_API_KEY", "admin-secret-sentinel")

    async with ModelGatewayAdminClient.from_environment() as client:
        http_client = getattr(client, "_client")
        assert isinstance(http_client, httpx.AsyncClient)
        assert client.gateway_url == "https://gateway.example.test"
        assert http_client.headers["Authorization"] == "Bearer admin-secret-sentinel"
