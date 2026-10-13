"""Secret storage SDK requests and typed responses.

Run: uv run pytest tests/unit/sdk/test_secrets_resource.py
"""

import json
from collections.abc import Callable

import httpx
import pytest

from valkyrie.sdk import SecretEntry, SecretsResponse, ValkyrieClient

_REFERENCE = "valkyrie/tenants/00000000-0000-0000-0000-000000000001/daytona"


async def test_requests_send_the_value_in_the_body_and_reject_bad_names(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    requests: list[tuple[str, str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.raw_path.decode(), request.content))
        if request.method == "GET":
            return httpx.Response(200, json={"secrets": [{"name": "daytona", "reference": _REFERENCE}]})
        return httpx.Response(200, json={"name": "daytona", "reference": _REFERENCE})

    async with make_client(handler) as client:
        entry = await client.secrets.set("daytona", '{"api_key": "x"}')
        listed = await client.secrets.list()
        for bad_name in ("..", ".", "../agents/foo", "a#b?c"):
            with pytest.raises(ValueError, match="Invalid secret name"):
                await client.secrets.remove(bad_name)

    assert entry == SecretEntry(name="daytona", reference=_REFERENCE)
    assert listed == SecretsResponse(secrets=[entry])
    assert [(method, path) for method, path, _ in requests] == [
        ("PUT", "/secrets/daytona"),
        ("GET", "/secrets"),
    ]
    assert json.loads(requests[0][2]) == {"value": '{"api_key": "x"}'}
