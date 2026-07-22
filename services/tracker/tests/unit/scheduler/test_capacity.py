from __future__ import annotations

from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import ClientPayloadError, InvalidURL
from benchmark_service import Resources
from daytona_api_client_async import ApiException, OrganizationUsageOverview, RegionUsageOverview, SandboxClass

from tracker.scheduler.capacity import (
    CapacityObservationUnavailableError,
    CapacitySnapshot,
    InvalidCapacityObservationError,
    ResourceVector,
    daytona_pool_key,
    normalize_task_resources,
    observe_daytona_capacity,
)


def _usage_row(**updates: object) -> RegionUsageOverview:
    values: dict[str, object] = {
        "region_id": "us",
        "sandbox_class": SandboxClass.CONTAINER,
        "total_cpu_quota": 8,
        "current_cpu_usage": 2,
        "total_memory_quota": 32,
        "current_memory_usage": 4,
        "total_disk_quota": 100,
        "current_disk_usage": 25,
    }
    values.update(updates)
    return RegionUsageOverview.model_construct(**values)


def _overview(*rows: RegionUsageOverview) -> OrganizationUsageOverview:
    return OrganizationUsageOverview.model_construct(region_usage=list(rows))


def test_resources_cover_dimensions_and_reject_invalid_capacity() -> None:
    demand = normalize_task_resources(Resources(vcpu=2, memory=3, disk=5))
    assert demand == ResourceVector(cpu_millis=2000, memory_mib=3072, storage_mib=5120)

    snapshot = CapacitySnapshot(
        total=ResourceVector(cpu_millis=8000, memory_mib=8192, storage_mib=10240),
        used=ResourceVector(cpu_millis=6000, memory_mib=5120, storage_mib=5120),
    )
    assert snapshot.available == demand
    assert snapshot.fits(demand)
    assert not snapshot.fits(ResourceVector(cpu_millis=2001, memory_mib=3072, storage_mib=5120))
    assert not snapshot.fits(ResourceVector(cpu_millis=2000, memory_mib=3073, storage_mib=5120))
    assert not snapshot.fits(ResourceVector(cpu_millis=2000, memory_mib=3072, storage_mib=5121))
    with pytest.raises(ValueError, match="non-negative"):
        ResourceVector(cpu_millis=-1)
    with pytest.raises(InvalidCapacityObservationError, match="exceeds"):
        CapacitySnapshot(total=ResourceVector(cpu_millis=1), used=ResourceVector(cpu_millis=2))


def test_pool_key_canonicalizes_endpoint() -> None:
    first = daytona_pool_key(
        organization_id="org",
        target="us",
        api_url="HTTPS://Daytona.Example:443/control/",
    )
    second = daytona_pool_key(
        organization_id="org",
        target="us",
        api_url="https://daytona.example/control",
    )
    assert first == second
    assert first == daytona_pool_key(
        organization_id="org",
        target="us",
        api_url="https://daytona.example/control#client-only-fragment",
    )
    assert "daytona.example" not in first
    assert first != daytona_pool_key(organization_id="other", target="us", api_url="https://daytona.example/control")
    assert first != daytona_pool_key(organization_id="org", target="eu", api_url="https://daytona.example/control")
    assert first != daytona_pool_key(organization_id="org", target="us", api_url="https://other.example/control")


@pytest.mark.asyncio
async def test_observes_exact_daytona_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    overview = _overview(_usage_row(region_id="eu"), _usage_row())
    requested: list[str] = []
    _patch_observation(monkeypatch, lambda: overview, requested)

    result = await observe_daytona_capacity(
        organization_id="org-1",
        target="us",
        api_url="https://daytona.example",
        api_key="secret",
    )

    assert requested == ["org-1"]
    assert result == CapacitySnapshot(
        total=ResourceVector(cpu_millis=8000, memory_mib=32768, storage_mib=102400),
        used=ResourceVector(cpu_millis=2000, memory_mib=4096, storage_mib=25600),
    )


@pytest.mark.parametrize(
    ("result", "expected_error"),
    [
        (TimeoutError(), CapacityObservationUnavailableError),
        (ClientPayloadError("truncated response"), CapacityObservationUnavailableError),
        (ApiException(status=503), CapacityObservationUnavailableError),
        (ApiException(status=401), InvalidCapacityObservationError),
        (InvalidURL("not a valid endpoint"), InvalidCapacityObservationError),
        (_overview(_usage_row(), _usage_row()), InvalidCapacityObservationError),
    ],
)
@pytest.mark.asyncio
async def test_classifies_transient_and_fatal_observation_failures(
    monkeypatch: pytest.MonkeyPatch,
    result: Exception | OrganizationUsageOverview,
    expected_error: type[Exception],
) -> None:
    def load() -> OrganizationUsageOverview:
        if isinstance(result, Exception):
            raise result
        return result

    _patch_observation(monkeypatch, load)

    with pytest.raises(expected_error):
        await _observe()


def _patch_observation(
    monkeypatch: pytest.MonkeyPatch,
    load: Callable[[], OrganizationUsageOverview],
    requested: list[str] | None = None,
) -> None:
    async def observe(organization_id: str) -> OrganizationUsageOverview:
        if requested is not None:
            requested.append(organization_id)
        return load()

    client = AsyncMock()
    client.__aenter__.return_value = client
    organizations = Mock()
    organizations.get_organization_usage_overview = observe
    monkeypatch.setattr("tracker.scheduler.capacity.ApiClient", Mock(return_value=client))
    monkeypatch.setattr("tracker.scheduler.capacity.OrganizationsApi", Mock(return_value=organizations))


async def _observe() -> CapacitySnapshot:
    return await observe_daytona_capacity(
        organization_id="org",
        target="us",
        api_url="https://daytona.example",
        api_key="secret",
    )
