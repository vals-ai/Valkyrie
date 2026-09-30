"""Focused tests for the local External Service Gateway control client."""

from collections.abc import Callable

import json

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
    active: bool = False,
    interval_index: int = 0,
) -> dict[str, str | int | bool]:
    return {
        "session_id": session_id,
        "state": state,
        "cumulative_neutral_overhead_ms": overhead_ms,
        "revision": revision,
        "accounting_epoch": epoch,
        "generation_active": active,
        "interval_index": interval_index,
    }


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[ExternalServiceGatewayClient, httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ExternalServiceGatewayClient(
        "http://gateway.test/", control_token="tracker-control", client=http_client
    ), http_client


async def test_control_client_authenticates_and_transmits_generation_phases() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["X-SSP-Control-Token"] == "tracker-control"
        path = request.url.path
        state = (
            "SEALED" if path.endswith("/resolve") else "ARBITRATING" if path.endswith("/arbitration/begin") else "OPEN"
        )
        active = path.endswith("/generation/begin")
        interval_index = 1 if "/generation/" in path else 0
        return httpx.Response(200, json=_snapshot(state=state, active=active, interval_index=interval_index))

    client, http_client = _client(handler)
    try:
        created = await client.create_session(session_id="session-1")
        begun = await client.begin_generation("session-1", 1)
        ended = await client.end_generation("session-1", 1)
        read = await client.read_session("session-1")
        frozen = await client.begin_arbitration("session-1")
        sealed = await client.resolve_arbitration("session-1", ArbitrationDecision.SEAL)
    finally:
        await http_client.aclose()

    assert created.generation_active is False
    assert begun.generation_active is True
    assert begun.interval_index == 1
    assert ended.generation_active is False
    assert read.state == AccountingSessionState.OPEN
    assert frozen.state == AccountingSessionState.ARBITRATING
    assert sealed.state == AccountingSessionState.SEALED
    assert [(r.method, r.url.path) for r in requests] == [
        ("POST", "/sessions"),
        ("POST", "/sessions/session-1/generation/begin"),
        ("POST", "/sessions/session-1/generation/end"),
        ("GET", "/sessions/session-1"),
        ("POST", "/sessions/session-1/arbitration/begin"),
        ("POST", "/sessions/session-1/arbitration/resolve"),
    ]
    assert json.loads(requests[0].content) == {"session_id": "session-1"}
    assert [json.loads(request.content) for request in requests[1:3]] == [{"interval_index": 1}, {"interval_index": 1}]
    assert json.loads(requests[-1].content) == {"decision": "SEAL"}


async def test_control_client_rejects_unauthorized_session_operations() -> None:
    with pytest.raises(ValueError):
        ExternalServiceGatewayClient("http://gateway.test", control_token="")

    client, http_client = _client(lambda _request: httpx.Response(401))
    try:
        with pytest.raises(httpx.HTTPStatusError) as exc:
            await client.create_session(session_id="session-1")
        assert exc.value.response.status_code == 401
    finally:
        await http_client.aclose()


async def test_one_active_interval_excludes_setup_and_caps_external_credit() -> None:
    snapshot = AccountingSessionSnapshot.model_validate(_snapshot(overhead_ms=12_500, revision=4, epoch=2))
    controller = ExternalServiceDeadlineController(
        snapshot=snapshot, base_allowance_seconds=30.0, credit_cap_seconds=10.0
    )

    assert controller.deadline(100.0) == 140.0
    await controller.begin_generation(now=105.0)
    assert controller.elapsed_seconds(115.0) == 10.0
    assert controller.deadline(115.0) == 145.0
    await controller.end_generation(now=117.0)
    assert controller.elapsed_seconds(500.0) == 12.0
    assert controller.deadline(500.0) == 528.0
    assert controller.summary() == ExternalServiceAccountingSummary(
        accounting_session_id="session-1",
        base_generation_allowance_seconds=30.0,
        cumulative_time_credit_cap_seconds=10.0,
        external_service_overhead_seconds=12.5,
        external_service_credit_applied_seconds=10.0,
        effective_generation_allowance_seconds=40.0,
        external_service_credit_revision=4,
    )


async def test_two_active_intervals_preserve_remaining_budget_across_idle_gap() -> None:
    controller = ExternalServiceDeadlineController(base_allowance_seconds=30.0)
    await controller.begin_generation(now=100.0)
    assert controller.deadline(110.0) == 130.0
    await controller.end_generation(now=112.0)
    assert controller.deadline(300.0) == 318.0
    await controller.begin_generation(now=300.0)
    assert controller.deadline(305.0) == 318.0
    await controller.end_generation(now=310.0)
    assert controller.elapsed_seconds(1000.0) == 22.0
    assert controller.deadline(1000.0) == 1008.0
    assert controller.interval_index == 2


def test_deadline_controller_rejects_a_snapshot_for_another_session() -> None:
    controller = ExternalServiceDeadlineController(
        snapshot=AccountingSessionSnapshot.model_validate(_snapshot()),
        base_allowance_seconds=30.0,
        credit_cap_seconds=10.0,
    )

    with pytest.raises(ValueError):
        controller._accept(  # pyright: ignore[reportPrivateUsage]
            AccountingSessionSnapshot.model_validate(_snapshot(session_id="session-2"))
        )
