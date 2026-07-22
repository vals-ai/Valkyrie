"""Minimal Daytona capacity boundary for queued sandbox starts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from hashlib import sha256
from math import isfinite
from typing import NoReturn
from urllib.parse import SplitResult, urlsplit, urlunsplit

from aiohttp import ClientError, ClientResponseError, InvalidURL
from benchmark_service import Resources
from daytona_api_client_async import ApiClient, ApiException, Configuration, OrganizationsApi, SandboxClass

_CAPACITY_TIMEOUT_SECONDS = 30


class CapacityObservationUnavailableError(RuntimeError):
    """Daytona capacity could not be observed because of a transient failure."""


class InvalidCapacityObservationError(RuntimeError):
    """Daytona capacity configuration or returned data is invalid."""


class ImpossibleResourceDemandError(RuntimeError):
    """A sandbox request exceeds the total capacity of its Daytona pool."""


@dataclass(frozen=True, slots=True)
class ResourceVector:
    cpu_millis: int = 0
    memory_mib: int = 0
    storage_mib: int = 0

    def __post_init__(self) -> None:
        if min(self.cpu_millis, self.memory_mib, self.storage_mib) < 0:
            raise ValueError("Resource components must be non-negative")


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    total: ResourceVector
    used: ResourceVector

    def __post_init__(self) -> None:
        if (
            self.used.cpu_millis > self.total.cpu_millis
            or self.used.memory_mib > self.total.memory_mib
            or self.used.storage_mib > self.total.storage_mib
        ):
            raise InvalidCapacityObservationError("Daytona capacity usage exceeds its quota")

    @property
    def available(self) -> ResourceVector:
        return ResourceVector(
            cpu_millis=self.total.cpu_millis - self.used.cpu_millis,
            memory_mib=self.total.memory_mib - self.used.memory_mib,
            storage_mib=self.total.storage_mib - self.used.storage_mib,
        )

    def fits(self, demand: ResourceVector) -> bool:
        available = self.available
        return (
            demand.cpu_millis <= available.cpu_millis
            and demand.memory_mib <= available.memory_mib
            and demand.storage_mib <= available.storage_mib
        )

    def fits_total(self, demand: ResourceVector) -> bool:
        return (
            demand.cpu_millis <= self.total.cpu_millis
            and demand.memory_mib <= self.total.memory_mib
            and demand.storage_mib <= self.total.storage_mib
        )


def normalize_task_resources(resources: Resources) -> ResourceVector:
    return ResourceVector(
        cpu_millis=resources.vcpu * 1000,
        memory_mib=resources.memory * 1024,
        storage_mib=resources.disk * 1024,
    )


def daytona_pool_key(
    *,
    organization_id: str,
    target: str,
    api_url: str,
) -> str:
    organization = _required(organization_id, "organization ID")
    region = _required(target, "target")
    endpoint = _canonical_endpoint(api_url)
    endpoint_hash = sha256(endpoint.encode()).hexdigest()[:24]
    return f"daytona:{endpoint_hash}:{organization}:{region}"


async def observe_daytona_capacity(
    *,
    organization_id: str,
    target: str,
    api_url: str,
    api_key: str,
) -> CapacitySnapshot:
    try:
        endpoint = _canonical_endpoint(api_url)
        configuration = Configuration(host=endpoint, access_token=api_key)
        async with asyncio.timeout(_CAPACITY_TIMEOUT_SECONDS), ApiClient(configuration) as api_client:
            overview = await OrganizationsApi(api_client).get_organization_usage_overview(organization_id)
    except Exception as error:
        _raise_classified(error)

    try:
        matches = [
            usage
            for usage in overview.region_usage
            if usage.region_id == target and usage.sandbox_class == SandboxClass.CONTAINER
        ]
        if len(matches) != 1:
            raise ValueError(f"expected one capacity row for target {target!r}, found {len(matches)}")
        usage = matches[0]
        return CapacitySnapshot(
            total=ResourceVector(
                cpu_millis=_scaled(usage.total_cpu_quota, 1000),
                memory_mib=_scaled(usage.total_memory_quota, 1024),
                storage_mib=_scaled(usage.total_disk_quota, 1024),
            ),
            used=ResourceVector(
                cpu_millis=_scaled(usage.current_cpu_usage, 1000),
                memory_mib=_scaled(usage.current_memory_usage, 1024),
                storage_mib=_scaled(usage.current_disk_usage, 1024),
            ),
        )
    except InvalidCapacityObservationError:
        raise
    except (AttributeError, TypeError, ValueError) as error:
        raise InvalidCapacityObservationError(f"Daytona returned invalid capacity data: {error}") from error


def _required(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _canonical_endpoint(value: str) -> str:
    raw = _required(value, "API URL")
    parsed = urlsplit(raw)
    port = parsed.port
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError("API URL must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("API URL must not contain credentials")

    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    if port is not None and not (scheme == "https" and port == 443) and not (scheme == "http" and port == 80):
        host = f"{host}:{port}"
    return urlunsplit(
        SplitResult(
            scheme=scheme,
            netloc=host,
            path=parsed.path.rstrip("/"),
            query=parsed.query,
            fragment="",
        )
    )


def _scaled(value: int | float, scale: int) -> int:
    scaled = value * scale
    if value < 0 or not isfinite(scaled):
        raise ValueError("quota values must be finite and non-negative")
    return round(scaled)


def _raise_classified(error: Exception) -> NoReturn:
    if isinstance(error, InvalidURL):
        raise InvalidCapacityObservationError("Daytona capacity configuration is invalid") from error
    if isinstance(error, (ApiException, ClientResponseError)):
        status = error.status
        if status == 429 or (isinstance(status, int) and 500 <= status <= 599):
            raise CapacityObservationUnavailableError("Daytona capacity is temporarily unavailable") from error
        raise InvalidCapacityObservationError("Daytona rejected the capacity request") from error
    if isinstance(error, (ConnectionError, TimeoutError, ClientError)):
        raise CapacityObservationUnavailableError("Daytona capacity is temporarily unavailable") from error
    raise InvalidCapacityObservationError("Daytona capacity could not be observed") from error
