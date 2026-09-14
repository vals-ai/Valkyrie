import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from benchmark_service import ImageSource, Resources, TargetedSnapshotSource

import tracker.scheduler.admission as admission_module
from tracker.database.models import BenchmarkStatus, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked, SandboxError
from tracker.scheduler.admission import SandboxQueueContext, enter_queued_sandbox


_SOURCE = ImageSource(image="python:3.12")
_RESOURCES = Resources(vcpu=1, memory=1, disk=1)


def _admission_harness(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    distribution = MagicMock()
    incr = MagicMock()
    monotonic = MagicMock(side_effect=[10.0, 12.5])
    monkeypatch.setattr(admission_module, "distribution", distribution)
    monkeypatch.setattr(admission_module, "incr", incr)
    monkeypatch.setattr(admission_module, "time", SimpleNamespace(monotonic=monotonic))

    provider = MagicMock()
    provider.check_admission = AsyncMock(return_value=True)
    context = SandboxQueueContext(
        provider=provider,
        pool_id="pool_test",
        engine=MagicMock(),
        poll_interval_seconds=0,
    )

    lock = MagicMock()
    lock.connection = MagicMock()
    lock.__aenter__ = AsyncMock(return_value=True)
    lock.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(admission_module, "queue_pool_lock", MagicMock(return_value=lock))
    monkeypatch.setattr(admission_module, "_reset_abandoned_pool_builds", MagicMock())

    session = MagicMock()
    session_context = MagicMock()
    session_context.__enter__.return_value = session
    session_context.__exit__.return_value = None
    monkeypatch.setattr(admission_module, "Session", MagicMock(return_value=session_context))

    lock_authority = MagicMock()
    eligible = MagicMock(return_value=True)
    queued_state = MagicMock(return_value=(TaskStatus.PENDING, BenchmarkStatus.IN_PROGRESS))
    claim = MagicMock(return_value=True)
    start = MagicMock(return_value=True)
    monkeypatch.setattr(admission_module, "lock_execution_authority", lock_authority)
    monkeypatch.setattr(admission_module, "eligible_task_is", eligible)
    monkeypatch.setattr(admission_module, "_queued_task_state", queued_state)
    monkeypatch.setattr(admission_module, "claim_eligible_task", claim)
    monkeypatch.setattr(admission_module, "_start_claimed_task", start)

    sandbox = MagicMock()
    sandbox_context = MagicMock()
    sandbox_context.__aenter__ = AsyncMock(return_value=sandbox)
    sandbox_context.__aexit__ = AsyncMock(return_value=None)
    create = MagicMock(return_value=sandbox_context)

    return SimpleNamespace(
        authority=MagicMock(),
        claim=claim,
        context=context,
        create=create,
        distribution=distribution,
        eligible=eligible,
        incr=incr,
        lock_authority=lock_authority,
        monotonic=monotonic,
        provider=provider,
        queued_state=queued_state,
        sandbox=sandbox,
        sandbox_context=sandbox_context,
        start=start,
    )


async def _enter(harness: SimpleNamespace) -> object | None:
    async with AsyncExitStack() as stack:
        return await enter_queued_sandbox(
            stack=stack,
            context=harness.context,
            task_row_id=MagicMock(),
            expected_started_at=MagicMock(),
            authority=harness.authority,
            source=_SOURCE,
            resources=_RESOURCES,
            create=harness.create,
        )


def _assert_metrics(harness: SimpleNamespace, tags: dict[str, str]) -> None:
    harness.distribution.assert_called_once_with("valkyrie.scheduler.admission.wait", 2.5, tags=tags)
    harness.incr.assert_called_once_with("valkyrie.scheduler.admission.outcome", tags=tags)


async def test_queued_admission_rejects_targeted_snapshots_before_provider_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distribution = MagicMock()
    incr = MagicMock()
    monotonic = MagicMock()
    monkeypatch.setattr(admission_module, "distribution", distribution)
    monkeypatch.setattr(admission_module, "incr", incr)
    monkeypatch.setattr(admission_module, "time", SimpleNamespace(monotonic=monotonic))
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
                authority=MagicMock(),
                source=TargetedSnapshotSource(snapshot="snapshot", target="different-pool"),
                resources=_RESOURCES,
                create=MagicMock(),
            )

    provider.check_admission.assert_not_called()
    monotonic.assert_not_called()
    distribution.assert_not_called()
    incr.assert_not_called()


async def test_queued_admission_emits_admitted_metrics_after_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _admission_harness(monkeypatch)

    result = await _enter(harness)

    assert result is harness.sandbox
    harness.provider.check_admission.assert_awaited_once_with(_SOURCE, _RESOURCES)
    harness.create.assert_called_once_with()
    _assert_metrics(harness, {"outcome": "admitted"})


@pytest.mark.parametrize("reason", ["authority_revoked", "not_waiting", "start_refused"])
async def test_queued_admission_emits_normal_non_admitted_reason(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
) -> None:
    harness = _admission_harness(monkeypatch)
    if reason == "authority_revoked":
        harness.lock_authority.side_effect = ExecutionAuthorityRevoked()
    elif reason == "not_waiting":
        harness.eligible.return_value = False
        harness.queued_state.return_value = (TaskStatus.FINISHED, BenchmarkStatus.IN_PROGRESS)
    else:
        harness.start.return_value = False

    result = await _enter(harness)

    assert result is None
    _assert_metrics(harness, {"outcome": "not_admitted", "reason": reason})
    if reason == "start_refused":
        harness.sandbox_context.__aexit__.assert_awaited_once()


async def test_queued_admission_preserves_third_authority_revocation_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _admission_harness(monkeypatch)
    harness.lock_authority.side_effect = [None, None, ExecutionAuthorityRevoked()]

    result = await _enter(harness)

    assert result is None
    assert harness.lock_authority.call_count == 3
    harness.start.assert_not_called()
    harness.sandbox_context.__aexit__.assert_awaited_once()
    _assert_metrics(harness, {"outcome": "not_admitted", "reason": "authority_revoked"})


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_queued_admission_provider_failure_emits_no_completion_metric(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    harness = _admission_harness(monkeypatch)
    harness.provider.check_admission.side_effect = error_type("provider failed")

    with pytest.raises(error_type, match="provider failed"):
        await _enter(harness)

    harness.distribution.assert_not_called()
    harness.incr.assert_not_called()


async def test_queued_admission_sandbox_failure_emits_no_completion_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _admission_harness(monkeypatch)
    harness.sandbox_context.__aenter__.side_effect = RuntimeError("sandbox failed")

    with pytest.raises(RuntimeError, match="sandbox failed"):
        await _enter(harness)

    harness.distribution.assert_not_called()
    harness.incr.assert_not_called()
