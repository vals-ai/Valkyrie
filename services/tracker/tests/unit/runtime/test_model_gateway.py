"""Task-scoped Model Gateway credential tests."""

import json
from typing import Any

import httpx
import pytest

from tracker.runtime.model_gateway import TOKEN_TTL_SECONDS, task_scoped_gateway_key


IDENTITY = {"benchmark_name": "swebench", "agent_name": "opencode"}
STATIC_KEY = "sk-static"
TOKEN = "mgwt_scoped"
MODEL = "openai/gpt-4o"


def _env(**overrides: str) -> dict[str, str]:
    env = {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": STATIC_KEY}
    env.update(overrides)
    return env


def _scoped(env: dict[str, str], **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "run_id": "run-1",
        "task_id": "task_0",
        "attested_model": MODEL,
        "variant": "xhigh",
        "identity": IDENTITY,
    }
    kwargs.update(overrides)
    return task_scoped_gateway_key(env, **kwargs)


class RecordingGateway:
    """Answers the mint and revoke calls, recording what was asked."""

    def __init__(self, *, mint_status: int = 200, revoke_status: int = 200) -> None:
        self.mint_status = mint_status
        self.revoke_status = revoke_status
        self.requests: list[tuple[str, dict[str, Any], str | None]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_client = httpx.AsyncClient
        transport = httpx.MockTransport(self)

        def build_client(**kwargs: Any) -> httpx.AsyncClient:
            return original_client(transport=transport, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", build_client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.url.path, json.loads(request.content), request.headers.get("Authorization")))
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

    async with _scoped(_env()) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN
        assert gateway.paths == ["/service-auth"]

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert gateway.payload_for("/service-auth") == {
        "run_id": "run-1",
        "task_id": "task_0",
        "allowed_models": [MODEL],
        "identity": IDENTITY,
        "variant": "xhigh",
        "ttl_seconds": TOKEN_TTL_SECONDS,
    }
    assert gateway.payload_for("/service-auth/revoke") == {"lease_id": "lease-1"}
    # Both control-plane calls authenticate as the executor, never as the token.
    assert {authorization for _, _, authorization in gateway.requests} == {f"Bearer {STATIC_KEY}"}


async def test_the_scope_ignores_what_a_contract_puts_in_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contract chooses its own secrets' variable names, so the environment is
    not a trustworthy source for anything the credential is scoped by."""
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(
        VALKYRIE_AGENT_MODEL="anthropic/claude-4-opus",
        VALKYRIE_AGENT_VARIANT="max",
        RUN_ID="someone-elses-run",
        TASK_ID="someone-elses-task",
    )

    async with _scoped(env):
        pass

    minted = gateway.payload_for("/service-auth")
    assert minted["allowed_models"] == [MODEL]
    assert minted["variant"] == "xhigh"
    assert (minted["run_id"], minted["task_id"]) == ("run-1", "task_0")


async def test_the_rest_of_the_environment_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(SOME_OTHER_SECRET="untouched")

    async with _scoped(env) as scoped:
        assert scoped == {**env, "MODEL_GATEWAY_API_KEY": TOKEN}
    # The caller's mapping is not mutated, so a retry still has the static key.
    assert env["MODEL_GATEWAY_API_KEY"] == STATIC_KEY


async def test_an_unattested_contract_keeps_the_static_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """No model the tracker resolved itself means nothing safe to scope to."""
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env()

    async with _scoped(env, attested_model=None) as scoped:
        assert scoped is env

    assert gateway.requests == []


@pytest.mark.parametrize("missing", ["MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"])
async def test_contracts_that_never_asked_for_the_gateway_are_untouched(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(**{missing: ""})

    async with _scoped(env) as scoped:
        assert scoped is env

    assert gateway.requests == []


async def test_a_failed_mint_stops_the_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falling back to the static key would make the scoping unreliable."""
    gateway = RecordingGateway(mint_status=503)
    gateway.install(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError):
        async with _scoped(_env()):
            pytest.fail("the sandbox must not start without a scoped credential")


async def test_a_failed_revoke_does_not_fail_a_finished_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """The token expires on its own TTL, so teardown is best effort."""
    gateway = RecordingGateway(revoke_status=500)
    gateway.install(monkeypatch)

    async with _scoped(_env()) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


async def test_the_credential_is_revoked_when_the_task_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(RuntimeError):
        async with _scoped(_env()):
            raise RuntimeError("agent blew up")

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
