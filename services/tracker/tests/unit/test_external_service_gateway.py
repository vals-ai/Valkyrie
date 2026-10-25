"""Counter reads and Tracker-owned cumulative generation accounting."""

from collections.abc import Callable

import httpx
import pytest

from tracker import config as tracker_config
from tracker.external_service_gateway import (
    AccountingSnapshot,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
    GenerationAccountingSummary,
)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1"])
def test_credit_cap_must_be_positive_and_finite(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("TEST_CREDIT_CAP", value)
    with pytest.raises(ValueError, match="positive finite"):
        tracker_config._positive_float_setting("TEST_CREDIT_CAP")  # pyright: ignore[reportPrivateUsage]


def _snapshot(overhead_ms: int = 0, revision: int = 0, accounting_id: str = "digest") -> AccountingSnapshot:
    return AccountingSnapshot(
        accounting_id=accounting_id, cumulative_neutral_overhead_ms=overhead_ms, revision=revision
    )


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[ExternalServiceGatewayClient, httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return ExternalServiceGatewayClient("http://gateway.test", "tracker-control", client=http_client), http_client


async def test_authenticated_counter_read_extends_tracker_deadline() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["X-SSP-Control-Token"] == "tracker-control"
        return httpx.Response(200, json=_snapshot(1200, 7).model_dump())

    client, http_client = _client(handler)
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=0.0)
        assert controller.deadline(0.0) == 10.0
        await controller.refresh()
        assert controller.deadline(0.0) == 11.2
        assert controller.summary().external_service_credit_applied_seconds == 1.2
    finally:
        await http_client.aclose()
    assert [(request.method, request.url.path) for request in requests] == [("GET", "/counters/digest")]


async def test_counter_read_propagates_storage_errors_instead_of_zero_credit() -> None:
    client, http_client = _client(lambda _request: httpx.Response(503))
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        with pytest.raises(httpx.HTTPStatusError):
            await controller.refresh()
        assert controller.effective_allowance_seconds() == 10.0
    finally:
        await http_client.aclose()


async def test_active_intervals_exclude_idle_gaps_and_preserve_credit_across_rounds() -> None:
    controller = ExternalServiceDeadlineController(
        base_allowance_seconds=30.0, credit_cap_seconds=10.0, snapshot=_snapshot(12_500, 4)
    )
    controller.begin_generation(now=105.0)
    assert controller.deadline(115.0) == 145.0
    controller.end_generation(now=117.0)
    assert controller.elapsed_seconds(500.0) == 12.0
    controller.begin_generation(now=300.0)
    assert controller.deadline(305.0) == 328.0
    controller.end_generation(now=310.0)
    assert controller.effective_allowance_seconds() == 40.0
    assert controller.summary() == GenerationAccountingSummary(
        accounting_id="digest",
        base_generation_allowance_seconds=30.0,
        cumulative_time_credit_cap_seconds=10.0,
        external_service_overhead_seconds=12.5,
        external_service_credit_applied_seconds=10.0,
        external_service_credit_revision=4,
        generation_elapsed_seconds=22.0,
    )


async def test_late_counter_after_cutoff_cannot_change_persisted_credit_or_observation() -> None:
    requests = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json=_snapshot(4000 if requests == 1 else 8000, requests).model_dump())

    client, http_client = _client(handler)
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=10.0, credit_cap_seconds=10.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=0.0)
        await controller.refresh()
        assert controller.deadline(12.0) == 14.0
        controller.freeze_credit()
        await controller.finalize_after_confirmed_stop(15.0)
        assert requests == 1
        assert controller.summary().external_service_credit_applied_seconds == 4.0
        assert controller.summary().external_service_overhead_seconds == 4.0
        assert controller.summary().external_service_credit_revision == 1
        assert controller.summary().generation_elapsed_seconds == 15.0
    finally:
        await http_client.aclose()


async def test_natural_terminal_read_applies_credit_and_unlimited_clock_remains_unbounded() -> None:
    client, http_client = _client(lambda _request: httpx.Response(200, json=_snapshot(10_000, 3).model_dump()))
    try:
        controller = ExternalServiceDeadlineController(
            base_allowance_seconds=None, credit_cap_seconds=5.0, client=client, snapshot=_snapshot()
        )
        controller.begin_generation(now=5.0)
        assert controller.deadline(10.0) is None
        await controller.finalize_after_confirmed_stop(17.0)
        summary = controller.summary()
        assert summary.external_service_credit_applied_seconds == 5.0
        assert controller.effective_allowance_seconds() is None
        assert summary.generation_elapsed_seconds == 12.0
    finally:
        await http_client.aclose()


async def test_counter_for_another_token_is_rejected() -> None:
    client, http_client = _client(
        lambda _request: httpx.Response(200, json=_snapshot(accounting_id="other").model_dump())
    )
    try:
        controller = ExternalServiceDeadlineController(base_allowance_seconds=10.0, client=client, snapshot=_snapshot())
        with pytest.raises(ValueError, match="different accounting ID"):
            await controller.refresh()
    finally:
        await http_client.aclose()
