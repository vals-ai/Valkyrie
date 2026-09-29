"""Task-scoped Model Gateway credential tests."""

import json
from typing import Any

import httpx
import pytest

import tracker.runtime.model_gateway as model_gateway
from tracker.runtime.model_gateway import task_scoped_gateway_key


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
        "companion_models": None,
        "identity": IDENTITY,
        "org_name": "vals.ai",
        "agent_timeout": 600,
    }
    kwargs.update(overrides)
    return task_scoped_gateway_key(env, **kwargs)


class RecordingGateway:
    def __init__(self, *, mint_status: int = 200, revoke_status: int = 200) -> None:
        self.mint_status = mint_status
        self.revoke_status = revoke_status
        self.requests: list[tuple[str, dict[str, Any], str | None]] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self))
        monkeypatch.setattr(model_gateway, "_client", client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.url.path, json.loads(request.content), request.headers.get("Authorization")))
        if request.url.path == "/service-auth":
            return httpx.Response(
                self.mint_status, json={"token": TOKEN, "lease_id": "lease-1", "expires_at": 1_800_000_000.0}
            )
        return httpx.Response(self.revoke_status)

    @property
    def paths(self) -> list[str]:
        return [path for path, _, _ in self.requests]

    def payload_for(self, path: str) -> dict[str, Any]:
        return next(payload for request_path, payload, _ in self.requests if request_path == path)


async def test_scoped_credential_replaces_the_static_key_and_is_revoked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    env = _env(
        VALKYRIE_AGENT_MODEL="anthropic/claude-4-opus",
        VALKYRIE_AGENT_VARIANT="max",
        RUN_ID="someone-elses-run",
        TASK_ID="someone-elses-task",
        SOME_OTHER_SECRET="untouched",
    )
    async with _scoped(env) as scoped:
        assert scoped == {**env, "MODEL_GATEWAY_API_KEY": TOKEN}
        assert gateway.paths == ["/service-auth"]
    assert env["MODEL_GATEWAY_API_KEY"] == STATIC_KEY

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert gateway.payload_for("/service-auth") == {
        "run_id": "run-1",
        "task_id": "task_0",
        "allowed_models": [MODEL],
        "identity": IDENTITY,
        "ttl_seconds": 600 + 2 * 60 * 60,
    }
    assert gateway.payload_for("/service-auth/revoke") == {"lease_id": "lease-1"}
    assert {authorization for _, _, authorization in gateway.requests} == {f"Bearer {STATIC_KEY}"}


async def test_companion_models_are_allowed_alongside_the_run_model(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env(), companion_models="openai/gpt-5-2025-08-07,anthropic/claude-sonnet-5"):
        pass

    assert gateway.payload_for("/service-auth")["allowed_models"] == [
        MODEL,
        "openai/gpt-5-2025-08-07",
        "anthropic/claude-sonnet-5",
    ]


@pytest.mark.parametrize(
    "attested_model,missing_env_key",
    [(None, None), (MODEL, "MODEL_GATEWAY_URL"), (MODEL, "MODEL_GATEWAY_API_KEY")],
)
async def test_unattested_or_missing_gateway_configuration_is_untouched(
    monkeypatch: pytest.MonkeyPatch, attested_model: str | None, missing_env_key: str | None
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(**{missing_env_key: ""}) if missing_env_key else _env()

    async with _scoped(env, attested_model=attested_model) as scoped:
        assert scoped == env

    assert gateway.requests == []


async def test_a_failed_mint_stops_the_task(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway(mint_status=503)
    gateway.install(monkeypatch)

    with pytest.raises(httpx.HTTPStatusError):
        async with _scoped(_env()):
            pytest.fail("the sandbox must not start without a scoped credential")


async def test_revoke_failure_is_logged_and_does_not_fail_a_finished_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway(revoke_status=503)
    gateway.install(monkeypatch)
    warnings: list[str] = []
    monkeypatch.setattr(model_gateway.logger, "warning", warnings.append)

    async with _scoped(_env()) as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]
    assert len(warnings) == 1
    assert "Could not revoke gateway lease lease-1" in warnings[0]


async def test_the_credential_is_revoked_when_the_task_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(RuntimeError):
        async with _scoped(_env()):
            raise RuntimeError("agent blew up")

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


async def test_the_key_is_not_sent_to_a_private_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "AUTH_REQUIRED", True)
    env = _env(MODEL_GATEWAY_URL="http://169.254.169.254")

    with pytest.raises(ValueError):
        async with _scoped(env, org_name="tenant.example"):
            pytest.fail("the executor's key must not leave for a private host")

    assert gateway.requests == []


async def test_the_gateway_stays_reachable_for_every_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "AUTH_REQUIRED", True)
    env = _env(MODEL_GATEWAY_URL="https://model-gateway.vals.ai")

    async with _scoped(env, org_name="tenant.example") as scoped:
        assert scoped["MODEL_GATEWAY_API_KEY"] == TOKEN

    assert gateway.paths == ["/service-auth", "/service-auth/revoke"]


@pytest.mark.parametrize(
    "agent_timeout,expected",
    [
        (600, 600 + 2 * 60 * 60),
        (5 * 24 * 60 * 60, 5 * 24 * 60 * 60 + 2 * 60 * 60),
        # Unbounded tasks use the gateway ceiling.
        (None, 7 * 24 * 60 * 60),
        (30 * 24 * 60 * 60, 7 * 24 * 60 * 60),
    ],
)
async def test_the_credential_follows_the_benchmark_timeout(
    monkeypatch: pytest.MonkeyPatch, agent_timeout: float | None, expected: int
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(_env(), agent_timeout=agent_timeout):
        pass

    assert gateway.payload_for("/service-auth")["ttl_seconds"] == expected
