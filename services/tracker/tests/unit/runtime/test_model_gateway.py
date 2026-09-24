"""Task-scoped Model Gateway credential tests."""

import json
from typing import Any

import httpx
import pytest

from tracker.runtime.model_gateway import (
    DEFAULT_TTL_SECONDS,
    MAX_TTL_SECONDS,
    TTL_SLACK_SECONDS,
    task_scoped_gateway_key,
)


IDENTITY = {"benchmark_name": "swebench", "agent_name": "opencode"}
STATIC_KEY = "sk-static"
TOKEN = "mgwt_scoped"


def _env(**overrides: str) -> dict[str, str]:
    env = {
        "MODEL_GATEWAY_URL": "https://gateway.test",
        "MODEL_GATEWAY_API_KEY": STATIC_KEY,
        "VALKYRIE_AGENT_MODEL": "openai/gpt-4o",
        "VALKYRIE_AGENT_VARIANT": "xhigh",
        "RUN_ID": "run-1",
        "TASK_ID": "task_0",
    }
    env.update(overrides)
    return env


class RecordingGateway:
    """Answers the mint and revoke calls, recording what was asked."""

    def __init__(self, *, mint_status: int = 200, revoke_status: int = 200) -> None:
        self.mint_status = mint_status
        self.revoke_status = revoke_status
        self.requests: list[tuple[str, dict[str, Any], str | None]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_client = httpx.AsyncClient
        transport = httpx.MockTransport(self)

        def build_client(*, timeout: float) -> httpx.AsyncClient:
            return original_client(transport=transport, timeout=timeout)

        monkeypatch.setattr(httpx, "AsyncClient", build_client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        authorization = request.headers.get("Authorization")
        self.requests.append((request.url.path, payload, authorization))
        if request.url.path == "/service-auth":
            return httpx.Response(self.mint_status, json={"token": TOKEN, "lease_id": "lease-1"})
        return httpx.Response(self.revoke_status, json={"revoked": 1})

    @property
    def paths(self) -> list[str]:
        return [path for path, _, _ in self.requests]

    def payload_for(self, path: str) -> dict[str, Any]:
        return next(payload for request_path, payload, _ in self.requests if request_path == path)


async def test_scoped_credential_replaces_the_static_key_and_is_revoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sandbox gets a token for its own model; the static key stays home."""
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with task_scoped_gateway_key(_env(), identity=IDENTITY, agent_timeout=600) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN
        assert gateway.paths == ["/service-auth"]

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert gateway.payload_for("/service-auth") == {
        "run_id": "run-1",
        "task_id": "task_0",
        "allowed_models": ["openai/gpt-4o"],
        "identity": IDENTITY,
        "variant": "xhigh",
        "ttl_seconds": 600 + TTL_SLACK_SECONDS,
    }
    assert gateway.payload_for("/service-auth/revoke") == {"lease_id": "lease-1"}
    # Both control-plane calls authenticate as the executor, never as the token.
    assert {authorization for _, _, authorization in gateway.requests} == {f"Bearer {STATIC_KEY}"}


async def test_the_rest_of_the_environment_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(SOME_OTHER_SECRET="untouched")

    async with task_scoped_gateway_key(env, identity=IDENTITY, agent_timeout=None) as scoped:
        assert scoped == {**env, "MODEL_GATEWAY_API_KEY": TOKEN}
    # The caller's mapping is not mutated, so a retry still has the static key.
    assert env["MODEL_GATEWAY_API_KEY"] == STATIC_KEY


@pytest.mark.parametrize(
    "missing",
    ["MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY", "VALKYRIE_AGENT_MODEL"],
)
async def test_environments_we_cannot_scope_are_passed_through(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    """No gateway credential, or no model the tracker attested, means no token.

    `VALKYRIE_AGENT_MODEL` is absent exactly when the tracker did not resolve
    the agent's inference settings itself, and a model the caller supplied is
    not one we can scope to.
    """
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(**{missing: ""})

    async with task_scoped_gateway_key(env, identity=IDENTITY, agent_timeout=None) as scoped:
        assert scoped is env

    assert gateway.requests == []


async def test_a_failed_mint_stops_the_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falling back to the static key would make the scoping unreliable."""
    gateway = RecordingGateway(mint_status=503)
    gateway.install(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError):
        async with task_scoped_gateway_key(_env(), identity=IDENTITY, agent_timeout=None):
            pytest.fail("the sandbox must not start without a scoped credential")


async def test_a_failed_revoke_does_not_fail_a_finished_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """The token expires on its own TTL, so teardown is best effort."""
    gateway = RecordingGateway(revoke_status=500)
    gateway.install(monkeypatch)

    async with task_scoped_gateway_key(_env(), identity=IDENTITY, agent_timeout=None) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


async def test_the_credential_is_revoked_when_the_task_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(RuntimeError):
        async with task_scoped_gateway_key(_env(), identity=IDENTITY, agent_timeout=None):
            raise RuntimeError("agent blew up")

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.parametrize(
    "agent_timeout,expected",
    [
        (None, DEFAULT_TTL_SECONDS),
        (600, 600 + TTL_SLACK_SECONDS),
        (0, TTL_SLACK_SECONDS),
        (MAX_TTL_SECONDS, MAX_TTL_SECONDS),
    ],
)
async def test_the_token_outlives_the_agent_within_the_gateway_bounds(
    monkeypatch: pytest.MonkeyPatch, agent_timeout: float | None, expected: int
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with task_scoped_gateway_key(_env(), identity=IDENTITY, agent_timeout=agent_timeout):
        pass

    assert gateway.payload_for("/service-auth")["ttl_seconds"] == expected
