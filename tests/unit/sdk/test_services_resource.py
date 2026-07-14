"""Tests for benchmark-service discovery and task ID workflows."""

from __future__ import annotations

import json

import httpx
import pytest


async def test_catalog_returns_typed_services_and_normalizes_urls(make_client) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/benchmark-services"
        return httpx.Response(
            200,
            json={
                "services": [
                    {
                        "name": "swebench",
                        "url": "https://swebench.test/",
                        "auth_header_name": "Authorization",
                        "auth_secret_name": "SWE_BENCH_TOKEN",
                    }
                ]
            },
        )

    async with make_client(handler) as client:
        result = await client.services.catalog()

    assert result.services[0].name == "swebench"
    assert result.services[0].url == "https://swebench.test"


async def test_list_catalogs_then_checks_services(make_client) -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json={"services": [{"name": "swebench", "url": "https://swe.test"}]})
        assert json.loads(request.content) == {
            "services": [
                {
                    "name": "swebench",
                    "url": "https://swe.test",
                    "auth_header_name": None,
                    "auth_secret_name": None,
                }
            ]
        }
        return httpx.Response(
            200,
            json={
                "services": [
                    {
                        "name": "swebench",
                        "url": "https://swe.test",
                        "healthy": False,
                        "latency_ms": None,
                        "error": "timeout",
                    }
                ]
            },
        )

    async with make_client(handler) as client:
        result = await client.services.list()

    assert methods == ["GET", "POST"]
    assert result.services[0].error == "timeout"


async def test_list_skips_health_check_for_an_empty_catalog(make_client) -> None:
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        assert request.method == "GET"
        return httpx.Response(200, json={"services": []})

    async with make_client(handler) as client:
        result = await client.services.list()

    assert request_count == 1
    assert result.services == []


async def test_task_ids_builds_configured_service_request(make_client) -> None:
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/fetch-benchmark-tasks"
        captured_body.update(json.loads(request.content))
        return httpx.Response(200, json={"task_ids": ["task-1", "task-2"]})

    async with make_client(handler) as client:
        result = await client.services.task_ids(
            "swebench",
            dataset="default",
            service_headers={"Authorization": "explicit-token", "X-Custom": "value"},
        )

    assert result == ["task-1", "task-2"]
    assert captured_body == {
        "benchmark_name": "swebench",
        "dataset": "default",
        "custom_benchmark_service": "https://local.swebench",
        "service_headers": {"Authorization": "explicit-token", "X-Custom": "value"},
    }


async def test_task_ids_can_ignore_custom_service_configuration(make_client) -> None:
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(200, json={"task_ids": []})

    async with make_client(handler) as client:
        await client.services.task_ids("swebench", ignore_custom_services=True)

    assert captured_body["custom_benchmark_service"] is None
    assert captured_body["service_headers"] == {"Authorization": "benchmark-token"}


async def test_task_ids_rejects_blank_benchmark_name(make_client) -> None:
    async with make_client(lambda _request: pytest.fail("request should not be sent")) as client:
        with pytest.raises(ValueError, match="benchmark must not be blank"):
            await client.services.task_ids("  ")
