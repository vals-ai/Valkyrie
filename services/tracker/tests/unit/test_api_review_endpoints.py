from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Task,
    TaskStatus,
)
from tracker.types import BenchmarkServiceEntry

client = TestClient(app)


def test_list_agents_uses_harness_config(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.agents as agents_api

    async def fake_list_agents(**_kwargs: object) -> list[tuple[str, datetime]]:
        return [("agent-a", datetime(2026, 1, 2, tzinfo=timezone.utc))]

    monkeypatch.setattr(agents_api, "list_agents", fake_list_agents)

    response = client.get("/agents")

    assert response.status_code == 200
    assert response.json() == {"agents": [{"name": "agent-a", "last_modified": "2026-01-02 00:00:00+00:00"}]}


def test_agent_download_url_uses_module_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.agents as agents_api

    captured_expiration: list[int] = []

    async def fake_presigned_url(*_args, expiration: int, **_kwargs) -> str:
        captured_expiration.append(expiration)
        return "https://example.test/agent-a.zip"

    async def fake_exists(*_args, **_kwargs) -> bool:
        return True

    monkeypatch.setattr(agents_api, "s3_object_exists", fake_exists)
    monkeypatch.setattr(agents_api, "create_presigned_url", fake_presigned_url)

    response = client.get("/agents/agent-a/download-url")

    assert response.status_code == 200
    assert response.json() == {
        "name": "agent-a",
        "download_url": "https://example.test/agent-a.zip",
        "expires_in": agents_api.PRESIGNED_URL_EXPIRES_SECONDS,
    }
    assert captured_expiration == [agents_api.PRESIGNED_URL_EXPIRES_SECONDS]


def test_agent_download_url_returns_404_for_missing_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.agents as agents_api

    async def fake_exists(*_args, **_kwargs) -> bool:
        return False

    monkeypatch.setattr(agents_api, "s3_object_exists", fake_exists)

    response = client.get("/agents/missing/download-url")

    assert response.status_code == 404
    assert response.json()["detail"] == "Agent 'missing' not found in S3"


async def test_ping_service_appends_health_path() -> None:
    import tracker.api.benchmark_services as benchmark_services_api

    fake_client = SimpleNamespace(requested_url=None)

    async def fake_get(url: str) -> httpx.Response:
        fake_client.requested_url = url
        return httpx.Response(200, request=httpx.Request("GET", url))

    fake_client.get = fake_get

    result = await benchmark_services_api._ping_service(
        cast(httpx.AsyncClient, fake_client),
        "swebench",
        "http://benchmark-service",
    )

    assert fake_client.requested_url == "http://benchmark-service/health"
    assert result.healthy is True
    assert result.error is None


async def test_ping_service_reports_request_errors() -> None:
    import tracker.api.benchmark_services as benchmark_services_api

    async def fake_get(url: str) -> httpx.Response:
        raise httpx.ConnectError("boom", request=httpx.Request("GET", url))

    fake_client = SimpleNamespace(get=fake_get)

    result = await benchmark_services_api._ping_service(
        cast(httpx.AsyncClient, fake_client),
        "swebench",
        "http://benchmark-service",
    )

    assert result.healthy is False
    assert result.latency_ms is None
    assert result.error == "boom"


def test_benchmark_services_endpoint_reuses_ping_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.benchmark_services as benchmark_services_api

    async def fake_ping(_client: httpx.AsyncClient, name: str, url: str, source: str = "custom"):
        return {"name": name, "url": url, "healthy": True, "latency_ms": 1, "error": None, "source": source}

    ping_mock = AsyncMock(side_effect=fake_ping)
    monkeypatch.setattr(benchmark_services_api, "_ping_service", ping_mock)

    response = client.post(
        "/benchmark-services",
        json={
            "services": [
                {"name": "swebench", "url": "http://swebench/"},
                {"name": "fab", "url": "http://fab"},
            ]
        },
    )

    assert response.status_code == 200
    assert [service["name"] for service in response.json()["services"]] == ["swebench", "fab"]
    assert response.json()["services"][0]["url"] == "http://swebench"
    assert ping_mock.await_count == 2


def test_benchmark_services_endpoint_returns_empty_services(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.benchmark_services as benchmark_services_api

    ping_mock = AsyncMock()
    monkeypatch.setattr(benchmark_services_api, "_ping_service", ping_mock)

    response = client.post("/benchmark-services", json={"services": []})

    assert response.status_code == 200
    assert response.json() == {"services": []}
    ping_mock.assert_not_awaited()


async def test_list_catalog_services_reads_catalog_or_local_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Catalog listing must be server-filtered, with local configured names as a fallback.

    Test cases:
    - Catalog URL is set and the tracker forwards the caller's auth header.
    - Catalog URL is unset and the tracker uses locally configured service names.
    """
    import tracker.api.benchmark_services as benchmark_services_api

    class CatalogClient:
        def __init__(self, *, timeout: float) -> None:
            assert timeout == benchmark_services_api.CATALOG_TIMEOUT_SECONDS

        async def __aenter__(self) -> CatalogClient:
            return self

        async def __aexit__(self, _exc_type: object, _exc_value: object, _traceback: object) -> None:
            return None

        async def get(self, url: str, headers: dict[str, str]) -> httpx.Response:
            assert url == "https://catalog.example/benchmark-services"
            assert headers == {"x-api-key": "tracker-key"}
            return httpx.Response(
                200,
                json={
                    "services": [
                        {
                            "name": "swebench",
                            "url": "https://swebench.benchmarks.vals.ai/",
                        }
                    ]
                },
                request=httpx.Request("GET", url),
            )

    monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
    monkeypatch.setattr(benchmark_services_api.httpx, "AsyncClient", CatalogClient)
    list_catalog_services = cast(
        Callable[[dict[str, str]], Awaitable[list[BenchmarkServiceEntry]]],
        getattr(benchmark_services_api, "_list_catalog_services"),
    )

    catalog_services = await list_catalog_services({"x-api-key": "tracker-key"})

    assert [(service.name, service.url) for service in catalog_services] == [
        ("swebench", "https://swebench.benchmarks.vals.ai")
    ]

    monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "")
    monkeypatch.setattr(benchmark_services_api, "list_benchmark_service_names", lambda: ["localbench"])

    def create_local_benchmark_service_url(name: str) -> str:
        return f"https://{name}.local"

    monkeypatch.setattr(
        benchmark_services_api,
        "create_benchmark_service_url",
        create_local_benchmark_service_url,
    )

    fallback_services = await list_catalog_services({})

    assert [(service.name, service.url) for service in fallback_services] == [
        ("localbench", "https://localbench.local")
    ]


def test_benchmark_services_get_lists_registry_catalog_services(monkeypatch: pytest.MonkeyPatch) -> None:
    import tracker.api.benchmark_services as benchmark_services_api

    async def fake_ping(_client: httpx.AsyncClient, name: str, url: str, source: str = "custom"):
        return {
            "name": name,
            "url": url,
            "healthy": name == "swebench",
            "latency_ms": 7,
            "error": None if name == "swebench" else "HTTP 503",
            "source": source,
        }

    async def fake_catalog_services(headers: dict[str, str]):
        assert headers == {"x-api-key": "tracker-key"}
        return [
            benchmark_services_api.BenchmarkServiceEntry(
                name="swebench",
                url="https://swebench.benchmarks.vals.ai",
            ),
            benchmark_services_api.BenchmarkServiceEntry(
                name="fab",
                url="https://fab.benchmarks.vals.ai",
            ),
        ]

    monkeypatch.setattr(benchmark_services_api, "_list_catalog_services", fake_catalog_services)
    monkeypatch.setattr(benchmark_services_api, "_ping_service", AsyncMock(side_effect=fake_ping))

    response = client.get("/benchmark-services", headers={"x-api-key": "tracker-key"})

    assert response.status_code == 200
    assert response.json()["services"] == [
        {
            "name": "swebench",
            "url": "https://swebench.benchmarks.vals.ai",
            "healthy": True,
            "latency_ms": 7,
            "error": None,
            "source": "hosted",
        },
        {
            "name": "fab",
            "url": "https://fab.benchmarks.vals.ai",
            "healthy": False,
            "latency_ms": 7,
            "error": "HTTP 503",
            "source": "hosted",
        },
    ]


def test_benchmarks_status_empty_ids_returns_empty_entries() -> None:
    from uuid import UUID

    from tracker.api.parsing import parse_csv

    assert parse_csv(" , ", UUID) == []

    response = client.get("/benchmarks/status?ids=")

    assert response.status_code == 200
    assert response.json() == {"entries": []}


def test_benchmarks_status_unknown_id_returns_empty_entries() -> None:
    response = client.get(f"/benchmarks/status?ids={uuid4()}")

    assert response.status_code == 200
    assert response.json() == {"entries": []}


def test_benchmarks_status_counts_stopped_tasks(
    database_session: Session,
    contract: AgentContractRequest,
) -> None:
    benchmark_id = uuid4()
    database_session.add(
        Benchmark(
            org_id=TEST_ORG_ID,
            id=benchmark_id,
            name="swebench",
            status=BenchmarkStatus.IN_PROGRESS,
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
        )
    )
    database_session.add_all(
        [
            Task(org_id=TEST_ORG_ID, task_id="finished", benchmark=benchmark_id, status=TaskStatus.FINISHED),
            Task(org_id=TEST_ORG_ID, task_id="error", benchmark=benchmark_id, status=TaskStatus.ERROR),
            Task(org_id=TEST_ORG_ID, task_id="stopped", benchmark=benchmark_id, status=TaskStatus.STOPPED),
            Task(org_id=TEST_ORG_ID, task_id="running", benchmark=benchmark_id, status=TaskStatus.IN_PROGRESS),
        ]
    )
    database_session.commit()

    response = client.get(f"/benchmarks/status?ids={benchmark_id},not-a-uuid")

    assert response.status_code == 200
    assert response.json()["entries"] == [
        {
            "id": str(benchmark_id),
            "status": "IN_PROGRESS",
            "finished_at": None,
            "total_tasks": 4,
            "finished_tasks": 3,
            "task_state_counts": {"FINISHED": 1, "ERROR": 1, "STOPPED": 1, "IN_PROGRESS": 1},
        }
    ]
