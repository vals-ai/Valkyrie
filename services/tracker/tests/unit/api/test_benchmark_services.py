"""Cover benchmark-service catalog and health routes.

Run with `uv run pytest tests/unit/api/test_benchmark_services.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import httpx
import pytest
from fastapi.testclient import TestClient

import tracker.api.benchmark_services as benchmark_services_api
from main import app

_client = TestClient(app)
_ResponseHandler = Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]


def _install_http_transport(monkeypatch: pytest.MonkeyPatch, handler: _ResponseHandler) -> None:
    """Route outbound HTTP requests through the supplied in-process handler."""
    transport = httpx.MockTransport(handler)
    original_client = httpx.AsyncClient

    def build_client(*, timeout: float) -> httpx.AsyncClient:
        return original_client(transport=transport, timeout=timeout)

    monkeypatch.setattr(httpx, "AsyncClient", build_client)


class TestBenchmarkServicesCatalog:
    """Benchmark service catalog responses and errors."""

    def test_benchmark_services_endpoint_fetches_catalog(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tracker should own catalog lookup so clients only need the tracker API.

        Test cases:
        - The endpoint forwards the caller API key to the catalog API.
        - Catalog responses are returned as benchmark service entries.
        """
        requests: list[httpx.Request] = []

        async def handle_request(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                json={"services": [{"name": "swebench", "url": "https://swebench.benchmarks.vals.ai/"}]},
            )

        monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
        _install_http_transport(monkeypatch, handle_request)

        response = _client.get("/benchmark-services", headers={"X-Api-Key": "tenant-key"})

        assert response.status_code == 200
        assert [str(request.url) for request in requests] == ["https://catalog.example/benchmark-services"]
        assert requests[0].headers["X-Api-Key"] == "tenant-key"
        assert response.json() == {
            "services": [
                {
                    "name": "swebench",
                    "url": "https://swebench.benchmarks.vals.ai",
                    "auth_header_name": None,
                    "auth_secret_name": None,
                }
            ]
        }

    def test_benchmark_services_endpoint_defaults_missing_services_to_empty(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A catalog payload without services must preserve the empty result contract.

        Test cases:
        - A successful object with no services key returns an empty list.
        """

        async def handle_request(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={})

        monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
        _install_http_transport(monkeypatch, handle_request)

        response = _client.get("/benchmark-services", headers={"X-Api-Key": "tenant-key"})

        assert response.status_code == 200
        assert response.json() == {"services": []}

    def test_benchmark_services_endpoint_hides_catalog_error_detail(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Verify downstream catalog failures retain only a stable public error.

        Test cases:
        - The downstream status remains available to the caller.
        - The downstream response body is not reflected.
        """
        sensitive_detail = "sensitive-catalog-provider-detail"

        async def handle_request(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"detail": sensitive_detail})

        monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
        _install_http_transport(monkeypatch, handle_request)

        response = _client.get("/benchmark-services", headers={"X-Api-Key": "tenant-key"})

        assert response.status_code == 503
        assert response.json() == {"detail": "Failed to list benchmark services"}
        assert sensitive_detail not in response.text

    def test_benchmark_services_endpoint_hides_catalog_transport_error_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Catalog connection failures must not reflect provider exception text.

        Test cases:
        - A transport failure returns a stable 502 and hides its sensitive detail.
        """
        sensitive_detail = "sensitive-catalog-transport-detail"

        async def handle_request(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(sensitive_detail, request=request)

        monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
        _install_http_transport(monkeypatch, handle_request)

        response = _client.get("/benchmark-services", headers={"X-Api-Key": "tenant-key"})

        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to list benchmark services"}
        assert sensitive_detail not in response.text

    @pytest.mark.parametrize(
        "catalog_response",
        [
            pytest.param(httpx.Response(200, content=b"sensitive-not-json"), id="invalid-json"),
            pytest.param(httpx.Response(200, json=[]), id="non-object-json"),
            pytest.param(
                httpx.Response(200, json={"services": [{"name": "swebench", "url": "not-a-url"}]}),
                id="invalid-service-entry",
            ),
            pytest.param(httpx.Response(200, json={"services": None}), id="non-list-services"),
        ],
    )
    def test_benchmark_services_endpoint_hides_malformed_catalog_response_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
        catalog_response: httpx.Response,
    ) -> None:
        """Malformed successful catalog responses must map to one stable client error.

        Test cases:
        - Invalid JSON, shape, service entries, and services values all return 502.
        """

        async def handle_request(_request: httpx.Request) -> httpx.Response:
            return catalog_response

        monkeypatch.setattr(benchmark_services_api, "BENCHMARK_CATALOG_URL", "https://catalog.example")
        _install_http_transport(monkeypatch, handle_request)

        response = _client.get("/benchmark-services", headers={"X-Api-Key": "tenant-key"})

        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to list benchmark services"}


class TestPingService:
    """Benchmark service health request behavior."""

    async def test_ping_service_appends_health_path(self) -> None:
        """Health checks must append the service contract's health path exactly once.

        Test cases:
        - A healthy response records the expected URL and returns healthy status.
        """
        mock_client = SimpleNamespace(requested_url=None)

        async def mock_get(url: str) -> httpx.Response:
            mock_client.requested_url = url
            return httpx.Response(200, request=httpx.Request("GET", url))

        mock_client.get = mock_get

        result = await benchmark_services_api._ping_service(  # pyright: ignore[reportPrivateUsage]
            cast(httpx.AsyncClient, mock_client),
            "swebench",
            "http://benchmark-service",
        )

        assert mock_client.requested_url == "http://benchmark-service/health"
        assert result.healthy is True
        assert result.error is None

    async def test_ping_service_hides_request_errors_and_logs_detail(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Health transport failures must stay useful in logs without leaking to API clients.

        Test cases:
        - The public result is stable while the original exception is logged.
        """
        sensitive_detail = "sensitive-benchmark-service-transport-detail"
        error = httpx.ConnectError(
            sensitive_detail,
            request=httpx.Request("GET", "http://benchmark-service/health"),
        )

        async def mock_get(_url: str) -> httpx.Response:
            raise error

        mock_client = SimpleNamespace(get=mock_get)
        warning = Mock()
        monkeypatch.setattr(benchmark_services_api.logger, "warning", warning)

        result = await benchmark_services_api._ping_service(  # pyright: ignore[reportPrivateUsage]
            cast(httpx.AsyncClient, mock_client),
            "swebench",
            "http://benchmark-service",
        )

        assert result.healthy is False
        assert result.latency_ms is None
        assert result.error == "Benchmark service request failed"
        warning.assert_called_once_with(
            "Benchmark service health check failed for %s: %s",
            "swebench",
            error,
        )


class TestBenchmarkServicesHealth:
    """Benchmark service endpoint health aggregation."""

    def test_benchmark_services_endpoint_hides_health_transport_error_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The health route must hide provider transport details in its response.

        Test cases:
        - A failed service ping returns an unhealthy entry with a stable error.
        """
        sensitive_detail = "sensitive-benchmark-service-transport-detail"

        async def handle_request(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(sensitive_detail, request=request)

        _install_http_transport(monkeypatch, handle_request)

        response = _client.post(
            "/benchmark-services",
            json={"services": [{"name": "swebench", "url": "http://swebench"}]},
        )

        assert response.status_code == 200
        assert response.json() == {
            "services": [
                {
                    "name": "swebench",
                    "url": "http://swebench",
                    "healthy": False,
                    "latency_ms": None,
                    "error": "Benchmark service request failed",
                }
            ]
        }
        assert sensitive_detail not in response.text

    def test_benchmark_services_endpoint_blocks_external_internal_destination(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        requests: list[httpx.Request] = []

        async def handle_request(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, request=request)

        monkeypatch.setattr(benchmark_services_api, "AUTH_REQUIRED", True)
        _install_http_transport(monkeypatch, handle_request)

        response = _client.post(
            "/benchmark-services",
            json={"services": [{"name": "swebench", "url": "http://127。0。0。1:8001"}]},
        )

        assert response.status_code == 403
        assert response.json() == {"detail": "Custom benchmark destination is not allowed"}
        assert requests == []

    def test_benchmark_services_endpoint_allows_vals_catalog_destination(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, request=request)

        monkeypatch.setattr(benchmark_services_api, "AUTH_REQUIRED", True)
        _install_http_transport(monkeypatch, handle_request)

        response = _client.post(
            "/benchmark-services",
            json={"services": [{"name": "swebench", "url": "https://swebench.benchmarks.vals.ai"}]},
        )

        assert response.status_code == 200
        assert response.json()["services"][0]["healthy"]

    def test_benchmark_services_endpoint_returns_empty_services(self) -> None:
        """An empty health-check request must preserve the successful empty contract.

        Test cases:
        - Empty input returns an empty result.
        """
        response = _client.post("/benchmark-services", json={"services": []})

        assert response.status_code == 200
        assert response.json() == {"services": []}
