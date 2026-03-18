from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service.schemas import Resources
from daytona import CreateSandboxFromImageParams, CreateSandboxFromSnapshotParams, DaytonaNotFoundError

from tracker.exceptions import SandboxError
from tracker.sandbox import _create_sandbox


async def test_create_sandbox_uses_image_params_for_plain_image() -> None:
    create_once = _create_sandbox.__wrapped__
    daytona = AsyncMock()
    daytona.get.side_effect = DaytonaNotFoundError("missing")
    daytona.create.return_value = AsyncMock()

    await create_once(
        daytona=daytona,
        sandbox_name="test-sandbox",
        image="ghcr.io/vals-ai/vcb-gen-base:tag",
        resources=Resources(vcpu=2, memory=4, disk=8),
    )

    params = daytona.create.await_args.args[0]
    assert isinstance(params, CreateSandboxFromImageParams)
    assert params.image == "ghcr.io/vals-ai/vcb-gen-base:tag"


async def test_create_sandbox_uses_snapshot_params_for_snapshot_prefix() -> None:
    create_once = _create_sandbox.__wrapped__
    daytona = AsyncMock()
    daytona.get.side_effect = DaytonaNotFoundError("missing")
    daytona.create.return_value = AsyncMock()

    await create_once(
        daytona=daytona,
        sandbox_name="test-sandbox",
        image="snapshot:vcb-test-snapshot",
        resources=Resources(vcpu=2, memory=4, disk=8),
    )

    params = daytona.create.await_args.args[0]
    assert isinstance(params, CreateSandboxFromSnapshotParams)
    assert params.snapshot == "vcb-test-snapshot"


async def test_create_sandbox_rejects_empty_snapshot_prefix() -> None:
    create_once = _create_sandbox.__wrapped__
    daytona = AsyncMock()
    daytona.get.side_effect = DaytonaNotFoundError("missing")

    with pytest.raises(SandboxError, match="without a snapshot name"):
        await create_once(
            daytona=daytona,
            sandbox_name="test-sandbox",
            image="snapshot:   ",
            resources=Resources(vcpu=2, memory=4, disk=8),
        )
