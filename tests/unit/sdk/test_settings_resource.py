"""Sandbox provider and run settings SDK requests and typed responses.

Run: uv run pytest tests/unit/sdk/test_settings_resource.py
"""

import json
from collections.abc import Callable

import httpx
import pytest

from valkyrie.sdk import ProvidersResponse, RunSettingsResponse, SavedBenchmarkService, ValkyrieClient

_PROVIDERS = {"providers": {"daytona": "daytona"}}
_SETTINGS = {
    "default_provider": "modal",
    "benchmark_services": {
        "swebench": {"url": "https://bench.example.com", "auth_header_name": "Authorization", "auth_secret_name": "key"}
    },
}


async def test_requests_hit_the_settings_routes_and_reject_dot_names(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    requests: list[tuple[str, str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.raw_path.decode(), body))
        return httpx.Response(200, json=_PROVIDERS if request.url.path.startswith("/sandbox-providers") else _SETTINGS)

    async with make_client(handler) as client:
        providers = await client.providers.set("daytona", "daytona")
        await client.providers.list()
        await client.providers.remove("modal")
        settings = await client.settings.set_benchmark_service(
            "swebench", "https://bench.example.com", auth_secret="key"
        )
        await client.settings.set_default_provider("modal")
        await client.settings.remove_benchmark_service("a/b")
        await client.settings.get()
        for bad_name in ("", ".", ".."):
            with pytest.raises(ValueError, match="Invalid name"):
                await client.providers.remove(bad_name)

    assert providers == ProvidersResponse(providers={"daytona": "daytona"})
    assert settings == RunSettingsResponse(
        default_provider="modal",
        benchmark_services={
            "swebench": SavedBenchmarkService(
                url="https://bench.example.com", auth_header_name="Authorization", auth_secret_name="key"
            )
        },
    )
    assert requests == [
        ("PUT", "/sandbox-providers/daytona", {"secret_name": "daytona"}),
        ("GET", "/sandbox-providers", None),
        ("DELETE", "/sandbox-providers/modal", None),
        (
            "PUT",
            "/settings/benchmark-services/swebench",
            {"url": "https://bench.example.com", "auth_secret_name": "key", "auth_header_name": None},
        ),
        ("PUT", "/settings/default-provider", {"provider": "modal"}),
        ("DELETE", "/settings/benchmark-services/a%2Fb", None),
        ("GET", "/settings", None),
    ]
