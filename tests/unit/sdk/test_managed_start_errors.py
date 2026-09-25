"""Recover accepted managed starts without treating ordinary API failures as runs."""

from uuid import UUID

import httpx
import pytest

from tests.unit.sdk.conftest import ClientFactory, SDKConfigFactory
from valkyrie.sdk import ValkyrieAPIError, ValkyrieRunAcceptedError, ValkyrieRunError

_ACCEPTED_DETAIL = {
    "message": "Executor dispatch enqueue acknowledgement failed; use Retry to continue",
    "benchmark_id": "11111111-1111-4111-8111-111111111111",
    "executor_dispatch_id": "22222222-2222-4222-8222-222222222222",
}


async def test_managed_start_preserves_accepted_run_identity_and_api_cause(
    make_client: ClientFactory, sdk_config: SDKConfigFactory
) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(503, json={"detail": _ACCEPTED_DETAIL})

    config = sdk_config(AWS_ACCESS_KEY_ID=None, AWS_SECRET_ACCESS_KEY=None, AWS_SESSION_TOKEN=None)
    async with make_client(handler, config=config) as client:
        with pytest.raises(ValkyrieRunAcceptedError) as raised:
            await client.runs.start("sweagent", "swebench", managed_s3_bucket="vs-dev-acme-123")

    assert isinstance(raised.value, ValkyrieRunError)
    assert raised.value.run_id == UUID("11111111-1111-4111-8111-111111111111")
    assert str(raised.value) == (
        "Run 11111111-1111-4111-8111-111111111111 was accepted but dispatch was not confirmed; "
        "use the existing run ID to reconcile or retry execution"
    )
    cause = raised.value.__cause__
    assert isinstance(cause, ValkyrieAPIError)
    assert cause.status_code == 503
    assert cause.detail == _ACCEPTED_DETAIL
    assert paths == ["/runs"]


@pytest.mark.parametrize(
    ("status_code", "managed", "detail"),
    [
        pytest.param(404, True, "Not Found", id="precreation-404"),
        pytest.param(401, True, _ACCEPTED_DETAIL, id="wrong-status-401"),
        pytest.param(500, True, _ACCEPTED_DETAIL, id="wrong-status-500"),
        pytest.param(503, False, _ACCEPTED_DETAIL, id="ordinary-start"),
        pytest.param(503, True, "temporarily unavailable", id="unrelated-503"),
        pytest.param(503, True, None, id="null-detail"),
        pytest.param(503, True, [_ACCEPTED_DETAIL], id="list-detail"),
        pytest.param(503, True, {}, id="empty-detail"),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "message": "other failure"}, id="wrong-message"),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "extra": "unexpected"}, id="unexpected-field"),
        pytest.param(
            503,
            True,
            {key: value for key, value in _ACCEPTED_DETAIL.items() if key != "benchmark_id"},
            id="missing-run",
        ),
        pytest.param(
            503,
            True,
            {key: value for key, value in _ACCEPTED_DETAIL.items() if key != "executor_dispatch_id"},
            id="missing-dispatch",
        ),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "benchmark_id": "bad-id"}, id="invalid-run"),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "executor_dispatch_id": "bad-id"}, id="invalid-dispatch"),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "benchmark_id": 123}, id="nonstring-run"),
        pytest.param(503, True, {**_ACCEPTED_DETAIL, "executor_dispatch_id": None}, id="null-dispatch"),
    ],
)
async def test_start_keeps_unconfirmed_api_failures_distinct(
    make_client: ClientFactory,
    sdk_config: SDKConfigFactory,
    status_code: int,
    managed: bool,
    detail: object,
) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(status_code, json={"detail": detail})

    config = sdk_config(AWS_ACCESS_KEY_ID=None, AWS_SECRET_ACCESS_KEY=None, AWS_SESSION_TOKEN=None)
    async with make_client(handler, config=config) as client:
        with pytest.raises(ValkyrieAPIError) as raised:
            await client.runs.start("sweagent", "swebench", managed_s3_bucket="vs-dev-acme-123" if managed else None)

    assert raised.value.status_code == status_code
    assert raised.value.detail == detail
    assert raised.value.__cause__ is None
    assert paths == ["/runs"]
