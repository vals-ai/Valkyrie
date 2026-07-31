from contextlib import AsyncExitStack
from unittest.mock import MagicMock

import pytest
from benchmark_service import Resources, TargetedSnapshotSource

from tracker.exceptions import SandboxError
from tracker.scheduler.admission import SandboxQueueContext, enter_queued_sandbox


async def test_queued_admission_rejects_targeted_snapshots_before_provider_access() -> None:
    provider = MagicMock()
    context = SandboxQueueContext(
        provider=provider,
        pool_id="pool_test",
        engine=MagicMock(),
    )

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxError, match="does not support targeted snapshots"):
            await enter_queued_sandbox(
                stack=stack,
                context=context,
                task_row_id=MagicMock(),
                expected_started_at=MagicMock(),
                source=TargetedSnapshotSource(snapshot="snapshot", target="different-pool"),
                resources=Resources(vcpu=1, memory=1, disk=1),
                create=MagicMock(),
            )

    provider.check_admission.assert_not_called()
