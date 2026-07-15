"""Live verification for the target-wide Daytona cleanup sweep."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest
from benchmark_service import ImageSource, Resources, SandboxProvider
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import AsyncDaytona, AsyncSandbox, DaytonaConfig, DaytonaNotFoundError, ListSandboxesQuery, SandboxState

from tracker.daytona_cleanup import cleanup_old_sandboxes
from tracker.sandbox import create_sandbox
from tracker.types import AWSCredentials
from tracker.utils import fetch_sandbox_provider_config


class ScopedDaytonaListClient:
    """Restrict the destructive live test to its UUID label while preserving production query fields."""

    def __init__(self, daytona: AsyncDaytona, labels: Mapping[str, str]) -> None:
        self.daytona = daytona
        self.labels = dict(labels)

    async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
        assert query is not None
        sandboxes = self.daytona.list(
            ListSandboxesQuery(
                labels=self.labels,
                targets=query.targets,
                created_at_before=query.created_at_before,
                limit=query.limit,
            )
        )
        async for sandbox in sandboxes:
            if all(sandbox.labels.get(key) == value for key, value in self.labels.items()):
                yield sandbox

    async def get(self, sandbox_id_or_name: str) -> AsyncSandbox:
        sandbox = await self.daytona.get(sandbox_id_or_name)
        if not all(sandbox.labels.get(key) == value for key, value in self.labels.items()):
            raise RuntimeError("Live cleanup test refused metadata outside its UUID scope")
        return sandbox


class ScopedSandboxDeleteProvider:
    """Refuse any live-test deletion outside the exact sandbox allowlist."""

    def __init__(self, provider: SandboxProvider, allowed_ids: set[str]) -> None:
        self.provider = provider
        self.allowed_ids = allowed_ids

    async def delete_sandbox(self, instance_id: str) -> None:
        if instance_id not in self.allowed_ids:
            raise RuntimeError("Live cleanup test refused deletion outside its allowlist")
        await self.provider.delete_sandbox(instance_id)


async def test_live_cleanup_guards_reject_out_of_scope_results() -> None:
    scope_labels = {"CleanupTest": "scope-id"}
    scoped = cast(AsyncSandbox, SimpleNamespace(id="scoped", labels=scope_labels))
    unrelated = cast(AsyncSandbox, SimpleNamespace(id="unrelated", labels={}))

    class FakeDaytona:
        async def list(self, _query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
            yield scoped
            yield unrelated

        async def get(self, sandbox_id_or_name: str) -> AsyncSandbox:
            return {"scoped": scoped, "unrelated": unrelated}[sandbox_id_or_name]

    class RecordingProvider:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        async def delete_sandbox(self, instance_id: str) -> None:
            self.deleted.append(instance_id)

    client = ScopedDaytonaListClient(cast(AsyncDaytona, FakeDaytona()), scope_labels)
    assert [sandbox.id async for sandbox in client.list(ListSandboxesQuery())] == ["scoped"]
    assert (await client.get("scoped")).id == "scoped"
    with pytest.raises(RuntimeError, match="outside its UUID scope"):
        await client.get("unrelated")

    provider = RecordingProvider()
    guarded_provider = ScopedSandboxDeleteProvider(cast(SandboxProvider, provider), {"scoped"})
    await guarded_provider.delete_sandbox("scoped")
    with pytest.raises(RuntimeError, match="outside its allowlist"):
        await guarded_provider.delete_sandbox("unrelated")
    assert provider.deleted == ["scoped"]


async def _wait_until_daytona_sandbox_is_absent(daytona: AsyncDaytona, sandbox_id: str) -> None:
    for _ in range(30):
        try:
            await daytona.get(sandbox_id)
        except DaytonaNotFoundError:
            return
        await asyncio.sleep(2)
    pytest.fail(f"Sandbox {sandbox_id} was still present after cleanup completed deletion")


async def _wait_until_daytona_sandboxes_are_listed(
    daytona: AsyncDaytona,
    *,
    labels: Mapping[str, str],
    target: str,
    expected_labels: Mapping[str, Mapping[str, str]],
    created_at_before: datetime,
) -> None:
    query = ListSandboxesQuery(
        labels=dict(labels),
        targets=[target],
        created_at_before=created_at_before,
        limit=200,
    )
    for _ in range(30):
        listed = {sandbox.id: sandbox async for sandbox in daytona.list(query)}
        if all(
            sandbox_id in listed
            and all(listed[sandbox_id].labels.get(key) == value for key, value in sandbox_labels.items())
            for sandbox_id, sandbox_labels in expected_labels.items()
        ):
            return
        await asyncio.sleep(2)
    pytest.fail("Cleanup test sandboxes were not both visible to the Daytona list API")


@pytest.mark.slow
async def test_cleanup_deletes_eligible_sandbox_and_preserves_exemption(
    sandbox_provider: SandboxProvider,
    daytona_secret_name: str,
    aws_credentials: AWSCredentials,
    test_image: str,
    test_resources: Resources,
    creation_semaphore: asyncio.Semaphore,
) -> None:
    """Prove target-wide paused-sandbox cleanup and the issue #120 opt-out on isolated sandboxes."""
    provider_config = cast(
        DaytonaProviderConfig,
        fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona"),
    )

    scope_labels = {"CleanupTest": uuid.uuid4().hex}
    exempt_labels = {**scope_labels, "clean-up": "false"}
    daytona_config = DaytonaConfig(
        api_key=provider_config.DAYTONA_API_KEY,
        api_url=provider_config.DAYTONA_API_URL,
        target=provider_config.DAYTONA_TARGET,
    )

    async with (
        create_sandbox(
            sandbox_provider,
            "cleanup-eligible",
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            labels=scope_labels,
        ) as eligible,
        create_sandbox(
            sandbox_provider,
            "cleanup-exempt",
            ImageSource(image=test_image),
            test_resources,
            creation_semaphore,
            labels=exempt_labels,
        ) as exempt,
        AsyncDaytona(config=daytona_config) as daytona,
    ):
        await (await daytona.get(eligible.id)).pause()
        assert (await daytona.get(eligible.id)).state == SandboxState.PAUSED
        cleanup_now = datetime.now(UTC) + timedelta(hours=49)
        await _wait_until_daytona_sandboxes_are_listed(
            daytona,
            labels=scope_labels,
            target=provider_config.DAYTONA_TARGET,
            expected_labels={eligible.id: scope_labels, exempt.id: exempt_labels},
            created_at_before=cleanup_now - timedelta(hours=48),
        )
        report = await cleanup_old_sandboxes(
            ScopedDaytonaListClient(daytona, scope_labels),
            ScopedSandboxDeleteProvider(sandbox_provider, {eligible.id}),
            now=cleanup_now,
            target=provider_config.DAYTONA_TARGET,
            dry_run=False,
        )

        assert report.succeeded
        assert report.scanned == 2
        assert report.eligible == 1
        assert report.deletion_completed == 1
        assert report.exempted == 1
        await _wait_until_daytona_sandbox_is_absent(daytona, eligible.id)
        assert (await daytona.get(exempt.id)).id == exempt.id
