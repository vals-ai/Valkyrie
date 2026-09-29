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
        self.session_headers: list[str | None] = []
        self.urls: list[str] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(self))
        monkeypatch.setattr(model_gateway, "_client", client)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.url.path, json.loads(request.content), request.headers.get("Authorization")))
        self.session_headers.append(request.headers.get("X-SSP-Session-ID"))
        self.urls.append(str(request.url))
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
    assert gateway.session_headers == [None, None]
    assert gateway.urls == ["https://gateway.test/service-auth", "https://gateway.test/service-auth/revoke"]


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


async def test_controlled_mint_routes_through_ssp_and_revoke_uses_same_endpoint_without_session_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env()

    async with _scoped(
        env, accounting_session_id="session-123", accounting_gateway_url="https://ssp.test", credit_cap_seconds=5.0
    ) as scoped:
        assert scoped == {**env, "MODEL_GATEWAY_API_KEY": TOKEN}
        assert "X-SSP-Session-ID" not in scoped
        assert gateway.urls == ["https://ssp.test/service-auth"]

    assert gateway.urls == ["https://ssp.test/service-auth", "https://ssp.test/service-auth/revoke"]
    assert gateway.session_headers == ["session-123", None]
    assert [authorization for _, _, authorization in gateway.requests] == [f"Bearer {STATIC_KEY}"] * 2


@pytest.mark.parametrize(
    ("agent_timeout", "credit_cap_seconds", "expected_ttl"),
    [
        (600.0, 7201.25, 15002),
        (3600.0, 594000.0, 604800),
    ],
)
async def test_controlled_mint_covers_base_credit_and_grace_without_truncating(
    monkeypatch: pytest.MonkeyPatch, agent_timeout: float, credit_cap_seconds: float, expected_ttl: int
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    async with _scoped(
        _env(),
        agent_timeout=agent_timeout,
        credit_cap_seconds=credit_cap_seconds,
        accounting_session_id="session-123",
        accounting_gateway_url="https://ssp.test",
    ):
        pass

    assert gateway.payload_for("/service-auth")["ttl_seconds"] == expected_ttl


async def test_controlled_mint_rejects_ttl_above_gateway_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(ValueError, match="604800"):
        async with _scoped(
            _env(),
            agent_timeout=3600.0,
            credit_cap_seconds=594000.1,
            accounting_session_id="session-123",
            accounting_gateway_url="https://ssp.test",
        ):
            pytest.fail("controlled sandbox must not receive a truncated token")

    assert gateway.requests == []


@pytest.mark.parametrize(
    "attested_model,missing_env_key",
    [(None, None), (MODEL, "MODEL_GATEWAY_URL"), (MODEL, "MODEL_GATEWAY_API_KEY")],
)
async def test_controlled_task_without_native_mint_prerequisites_fails_before_sandbox(
    monkeypatch: pytest.MonkeyPatch, attested_model: str | None, missing_env_key: str | None
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    env = _env(**{missing_env_key: ""}) if missing_env_key else _env()

    with pytest.raises(RuntimeError, match="Controlled task requires a native Model Gateway"):
        async with _scoped(
            env,
            attested_model=attested_model,
            accounting_session_id="session-123",
            accounting_gateway_url="https://ssp.test",
        ):
            pytest.fail("controlled sandbox must not start with the static key")

    assert gateway.requests == []


@pytest.mark.parametrize("accounting_gateway_url", [None, ""])
async def test_controlled_task_without_ssp_url_fails_before_mint(
    monkeypatch: pytest.MonkeyPatch, accounting_gateway_url: str | None
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)

    with pytest.raises(RuntimeError, match="Controlled task requires an external service gateway URL"):
        async with _scoped(_env(), accounting_session_id="session-123", accounting_gateway_url=accounting_gateway_url):
            pytest.fail("controlled sandbox must not start without an SSP")

    assert gateway.requests == []


async def test_controlled_task_does_not_send_static_key_to_disallowed_ssp_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = RecordingGateway()
    gateway.install(monkeypatch)
    monkeypatch.setattr(model_gateway, "AUTH_REQUIRED", True)

    with pytest.raises(ValueError):
        async with _scoped(
            _env(),
            org_name="tenant.example",
            accounting_session_id="session-123",
            accounting_gateway_url="http://169.254.169.254",
        ):
            pytest.fail("controlled sandbox must not start with an unsafe SSP")

    assert gateway.requests == []
