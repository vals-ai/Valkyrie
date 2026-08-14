"""Tests for Tracker outbound benchmark-service trust boundaries.

Run: uv run pytest tests/unit/test_outbound_security.py
"""

import pytest
from pydantic import ValidationError

from tracker.config import (
    BenchmarkServiceDestination,
    classify_benchmark_service_destination,
    create_benchmark_service_url,
)
from tracker.database.models import AgentContractRequest
from tracker.outbound_security import validate_custom_service_destination
from tracker.types import (
    BenchmarkServiceEntry,
    FetchBenchmarkTasksRequest,
    HarnessConfig,
    StartBenchmarkRequest,
)
from tracker.utils.resources import create_benchmark_service_client

_ASCII_CONTROL_URLS = [
    pytest.param(
        f"http://service.example/{chr(codepoint)}path",
        id=f"control-u{codepoint:04x}",
    )
    for codepoint in (*range(0x20), 0x7F)
]


def _start_benchmark_request(harness_config: HarnessConfig, **overrides: object) -> StartBenchmarkRequest:
    values: dict[str, object] = {
        "contract": AgentContractRequest(name="agent"),
        "benchmark_name": "swebench",
        "harness_config": harness_config,
    }
    values.update(overrides)
    return StartBenchmarkRequest.model_validate(values)


class TestBenchmarkServiceNameValidation:
    """Benchmark service names used to construct outbound URLs."""

    @pytest.mark.parametrize(
        "benchmark_name",
        [
            "127.0.0.1/",
            "service/path",
            "service#fragment",
            "service@example.invalid",
            "../service",
            "-service",
            "service-",
            "service..other",
        ],
    )
    def test_benchmark_name_rejects_url_parser_control(
        self,
        benchmark_name: str,
        harness_config: HarnessConfig,
    ) -> None:
        """Reject names that can alter the derived benchmark-service destination."""
        with pytest.raises(ValueError, match="Invalid benchmark name"):
            create_benchmark_service_url(benchmark_name)

        with pytest.raises(ValidationError):
            FetchBenchmarkTasksRequest(benchmark_name=benchmark_name)

        with pytest.raises(ValidationError):
            _start_benchmark_request(harness_config, benchmark_name=benchmark_name)

    def test_benchmark_name_preserves_supported_dns_label(
        self,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """Keep a normal hosted benchmark name unchanged in the derived URL."""
        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_BASE_URL", None)

        assert create_benchmark_service_url("swebench") == "http://swebench.local:8001"
        assert FetchBenchmarkTasksRequest(benchmark_name="swebench").benchmark_name == "swebench"
        assert _start_benchmark_request(harness_config).benchmark_name == "swebench"


class TestBenchmarkServiceDestination:
    """Credential forwarding trust derives from the exact hosted-service origin."""

    @pytest.mark.parametrize(
        ("service_url", "expected"),
        [
            (None, BenchmarkServiceDestination.HOSTED),
            ("https://swebench.benchmarks.vals.ai", BenchmarkServiceDestination.HOSTED),
            ("https://swebench.benchmarks.vals.ai:443/path", BenchmarkServiceDestination.HOSTED),
            ("http://swebench.benchmarks.vals.ai", BenchmarkServiceDestination.CUSTOM),
            ("https://other.benchmarks.vals.ai", BenchmarkServiceDestination.CUSTOM),
            ("https://swebench.benchmarks.vals.ai:8443", BenchmarkServiceDestination.CUSTOM),
            ("https://swebench.benchmarks.vals.ai:0", BenchmarkServiceDestination.CUSTOM),
        ],
    )
    def test_classifies_only_exact_hosted_origin(
        self,
        service_url: str | None,
        expected: BenchmarkServiceDestination,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_BASE_URL", "benchmarks.vals.ai")

        assert classify_benchmark_service_destination("swebench", service_url) is expected

    def test_normalizes_default_http_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_BASE_URL", None)
        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "local")
        monkeypatch.setattr("tracker.config._CLOUDMAP_PORT", 80)

        assert (
            classify_benchmark_service_destination("swebench", "http://swebench.local")
            is BenchmarkServiceDestination.HOSTED
        )


class TestCustomServiceDestinationPolicy:
    """Tenant-aware authorization for caller-supplied benchmark destinations."""

    @pytest.mark.parametrize(
        "service_url",
        [
            "https://vals.ai",
            "https://benchmarks-dev.vals.ai",
            "http://localhost:8001",
            "http://local:8001",
            "http://internal:8001",
            "http://service.local:8001",
            "http://service.internal:8001",
            "http://127.0.0.1:8001",
            "http://127。0。0。1:8001",
            "http://127．0．0．1:8001",
            "http://127｡0｡0｡1:8001",
            "http://localhost。:8001",
            "http://127.1:8001",
            "http://2130706433:8001",
            "http://0x7f000001:8001",
            "http://0177.0.0.1:8001",
            "http://10.0.0.1:8001",
            "http://100.64.0.1:8001",
            "http://169.254.1.1:8001",
            "http://224.0.0.1:8001",
            "http://0.0.0.0:8001",
            "http://[::1]:8001",
        ],
    )
    def test_external_tenant_rejects_internal_destination(self, service_url: str) -> None:
        with pytest.raises(ValueError, match="Custom benchmark destination is not allowed"):
            validate_custom_service_destination(
                service_url,
                org_name="external-tenant",
                auth_required=True,
            )

    @pytest.mark.parametrize(
        "service_url",
        [
            "http://team.example",
            "http://team。example",
            "https://evilvals.ai",
            "https://vals.ai.evil.com",
        ],
    )
    def test_external_tenant_allows_public_destination(self, service_url: str) -> None:
        validate_custom_service_destination(
            service_url,
            org_name="external-tenant",
            auth_required=True,
        )

    @pytest.mark.parametrize(
        ("org_name", "auth_required"),
        [("vals.ai", True), ("default", False)],
    )
    def test_trusted_caller_keeps_internal_destination_access(
        self,
        org_name: str,
        auth_required: bool,
    ) -> None:
        validate_custom_service_destination(
            "http://service.internal:8001",
            org_name=org_name,
            auth_required=auth_required,
        )


class TestCustomServiceUrlValidation:
    """Custom benchmark service URL validation."""

    @pytest.mark.parametrize(
        "service_url",
        [
            "ftp://service.example",
            "http://user:password@service.example",
            "http://service.example?target=other",
            "http://service.example#fragment",
            "http://service.example\\@other.example",
            "http://service.example/ bad",
            "http://serv\x00ice.example",
            "http://serv\x7fice.example",
            *_ASCII_CONTROL_URLS,
        ],
    )
    def test_custom_service_url_rejects_parser_and_credential_controls(
        self,
        service_url: str,
        harness_config: HarnessConfig,
    ) -> None:
        """Reject custom service URLs whose syntax can obscure routing or credentials."""
        with pytest.raises(ValidationError):
            FetchBenchmarkTasksRequest(benchmark_name="swebench", custom_benchmark_service=service_url)

        with pytest.raises(ValidationError):
            _start_benchmark_request(harness_config, custom_benchmark_service=service_url)

        with pytest.raises(ValidationError):
            BenchmarkServiceEntry(name="swebench", url=service_url)

    @pytest.mark.parametrize("service_url", _ASCII_CONTROL_URLS)
    def test_client_factory_rejects_url_controls(self, service_url: str) -> None:
        """Reject URL controls when constructing a client outside request-model validation."""
        with pytest.raises(ValueError, match="Invalid benchmark service URL"):
            create_benchmark_service_client(service_url)

    def test_custom_service_url_preserves_intentional_internal_http_service(
        self,
        harness_config: HarnessConfig,
    ) -> None:
        """Preserve the documented custom/internal benchmark-service capability."""
        request = FetchBenchmarkTasksRequest(
            benchmark_name="swebench",
            custom_benchmark_service="http://internal-swebench.example.com:8001/",
        )

        start_request = _start_benchmark_request(
            harness_config,
            custom_benchmark_service="http://internal-swebench.example.com:8001/",
        )
        service_entry = BenchmarkServiceEntry(
            name="swebench",
            url="http://internal-swebench.example.com:8001/",
        )

        assert request.custom_benchmark_service == "http://internal-swebench.example.com:8001"
        assert start_request.custom_benchmark_service == "http://internal-swebench.example.com:8001"
        assert service_entry.url == "http://internal-swebench.example.com:8001"
