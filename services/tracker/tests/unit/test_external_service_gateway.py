"""Focused tests for the local External Service Gateway control client."""

from collections.abc import Callable

import httpx
import pytest

from tracker import config as tracker_config
from tracker.external_service_gateway import (
    AccountingSessionSnapshot,
    AccountingSessionState,
    ArbitrationDecision,
    ExternalServiceAccountingSummary,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1"])
def test_credit_cap_must_be_positive_and_finite(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("TEST_CREDIT_CAP", value)

    with pytest.raises(ValueError, match="positive finite"):
        tracker_config._positive_float_setting("TEST_CREDIT_CAP")  # pyright: ignore[reportPrivateUsage]


def _snapshot(
    *,
    session_id: str = "session-1",
    state: str = "OPEN",
    overhead_ms: int = 0,
    revision: int = 0,
    epoch: int = 0,
) -> dict[str, str | int]:
    return {
        "session_id": session_id,
        "state": state,
        "cumulative_neutral_overhead_ms": overhead_ms,
        "revision": revision,
        "accounting_epoch": epoch,
    }


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[ExternalServiceGatewayClient, httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return (
        ExternalServiceGatewayClient(
            "http://gateway.test/",
            client=http_client,
        ),
        http_client,
    )


async def test_control_client_uses_the_session_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        state = "ARBITRATING" if request.url.path.endswith("/begin") else "OPEN"
        if request.url.path.endswith("/resolve"):
            state = "SEALED"
        return httpx.Response(200, json=_snapshot(state=state), request=request)

    client, http_client = _client(handler)
    try:
        created = await client.create_session(
            session_id="session-1",
            model="model-1",
            config={"variant": "variant-1"},
        )
        read = await client.read_session("session-1")
        frozen = await client.begin_arbitration("session-1")
        sealed = await client.resolve_arbitration("session-1", ArbitrationDecision.SEAL)
    finally:
        await http_client.aclose()

    assert created.state == AccountingSessionState.OPEN
    assert read.state == AccountingSessionState.OPEN
    assert frozen.state == AccountingSessionState.ARBITRATING
    assert sealed.state == AccountingSessionState.SEALED
    assert [request.url.path for request in requests] == [
        "/sessions",
        "/sessions/session-1",
        "/sessions/session-1/arbitration/begin",
        "/sessions/session-1/arbitration/resolve",
    ]
    assert requests[0].method == "POST"
    assert requests[0].read().decode() == (
        '{"session_id":"session-1","adapter":"model_gateway","model":"model-1","config":{"variant":"variant-1"}}'
    )
    assert requests[-1].read().decode() == '{"decision":"SEAL"}'


def test_deadline_controller_applies_the_cap_from_the_immutable_start() -> None:
    snapshot = AccountingSessionSnapshot.model_validate(_snapshot(overhead_ms=12_500, revision=4, epoch=2))
    controller = ExternalServiceDeadlineController(
        client=ExternalServiceGatewayClient("http://gateway.test"),
        snapshot=snapshot,
        base_allowance_seconds=30.0,
        credit_cap_seconds=10.0,
    )

    assert controller.applied_credit_seconds() == 10.0
    assert controller.effective_allowance_seconds() == 40.0
    assert controller.deadline(100.0) == 140.0
    assert controller.summary() == ExternalServiceAccountingSummary(
        accounting_session_id="session-1",
        base_generation_allowance_seconds=30.0,
        cumulative_time_credit_cap_seconds=10.0,
        external_service_overhead_seconds=12.5,
        external_service_credit_applied_seconds=10.0,
        effective_generation_allowance_seconds=40.0,
        external_service_credit_revision=4,
    )


def test_deadline_controller_rejects_a_snapshot_for_another_session() -> None:
    controller = ExternalServiceDeadlineController(
        client=ExternalServiceGatewayClient("http://gateway.test"),
        snapshot=AccountingSessionSnapshot.model_validate(_snapshot()),
        base_allowance_seconds=30.0,
        credit_cap_seconds=10.0,
    )

    with pytest.raises(ValueError, match="different accounting session"):
        controller._accept(  # pyright: ignore[reportPrivateUsage]
            AccountingSessionSnapshot.model_validate(_snapshot(session_id="session-2"))
        )
