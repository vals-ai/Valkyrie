"""Integration tests for pinned create-benchmark-service sandbox provider behavior."""

from typing import Any, cast

import pytest
from benchmark_service import ImageSource, Resources, SandboxCreateRequest
from benchmark_service.sandbox.daytona import DaytonaSandboxProvider
from daytona import DaytonaNotFoundError, SandboxState
from daytona.common.errors import DaytonaConflictError

from tracker.types import AWSCredentials, HarnessConfig


@pytest.fixture
def harness_config() -> HarnessConfig:
    """Provide a local harness config so this dependency integration does not need real secrets."""
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=1,
        daytona_secret_name="test-daytona-secret",
    )


class _InnerSandbox:
    id = "sandbox-id"
    name = "task-alias"
    state = SandboxState.DESTROYING

    async def wait_for_sandbox_start(self, timeout: int) -> None:
        assert timeout == 0


class _DestroyingNameConflictDaytonaClient:
    def __init__(self, sandbox: _InnerSandbox) -> None:
        self.sandbox = sandbox
        self.create_attempts = 0
        self.get_attempts = 0
        self.name_released = False

    async def get(self, instance_id: str) -> _InnerSandbox:
        self.get_attempts += 1
        assert instance_id == self.sandbox.name
        if self.get_attempts >= 3:
            self.name_released = True
            raise DaytonaNotFoundError("not found")
        return self.sandbox

    async def create(self, *_args: object, **_kwargs: object) -> _InnerSandbox:
        self.create_attempts += 1
        if self.create_attempts == 1:
            raise DaytonaConflictError("Sandbox name already exists")
        assert self.name_released
        self.sandbox.state = SandboxState.STARTED
        return self.sandbox


def _provider(daytona: _DestroyingNameConflictDaytonaClient) -> DaytonaSandboxProvider:
    provider = DaytonaSandboxProvider.__new__(DaytonaSandboxProvider)
    provider._daytona = cast(Any, daytona)  # pyright: ignore[reportPrivateUsage]
    return provider


def _request(name: str) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=ImageSource(image="python:3.12"),
        resources=Resources(vcpu=1, memory=2, disk=5),
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


async def test_daytona_provider_dependency_waits_for_destroying_name_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Tracker depends on create-benchmark-service to wait when Daytona keeps a destroying sandbox name reserved.

    Test cases:
    - The pinned provider catches DaytonaConflictError for a destroying sandbox name.
    - The provider waits for Daytona to release the name before retrying create.
    """
    inner = _InnerSandbox()
    daytona = _DestroyingNameConflictDaytonaClient(inner)
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("benchmark_service.sandbox.daytona.asyncio.sleep", fake_sleep)

    sandbox = await _provider(daytona).create_sandbox(_request(inner.name))

    assert sandbox.id == inner.id
    assert daytona.name_released is True
    assert daytona.create_attempts == 2
    assert daytona.get_attempts == 3
    assert sleep_calls == [2]
