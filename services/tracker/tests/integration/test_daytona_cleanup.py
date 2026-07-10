"""Live verification for the scoped Daytona cleanup sweep."""

import asyncio
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from benchmark_service import ImageSource, Resources, SandboxProvider
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import AsyncDaytona, DaytonaConfig, DaytonaNotFoundError, ListSandboxesQuery

from tracker.daytona_cleanup import cleanup_old_sandboxes
from tracker.sandbox import create_sandbox
from tracker.sandbox_labels import ENVIRONMENT_LABEL, MANAGED_BY_LABEL, MANAGED_BY_VALKYRIE, valkyrie_sandbox_labels
from tracker.types import AWSCredentials
from tracker.utils import fetch_sandbox_provider_config


async def _wait_until_daytona_sandbox_is_absent(daytona: AsyncDaytona, sandbox_id: str) -> None:
    for _ in range(30):
        try:
            await daytona.get(sandbox_id)
        except DaytonaNotFoundError:
            return
        await asyncio.sleep(2)
    pytest.fail(f"Sandbox {sandbox_id} was still present after cleanup requested deletion")


async def _wait_until_daytona_sandboxes_are_listed(
    daytona: AsyncDaytona,
    *,
    labels: Mapping[str, str],
    target: str,
    sandbox_ids: set[str],
) -> None:
    query = ListSandboxesQuery(labels=dict(labels), targets=[target], limit=200)
    for _ in range(30):
        listed_ids = {sandbox.id async for sandbox in daytona.list(query)}
        if sandbox_ids <= listed_ids:
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
    """Create an isolated label scope and verify deletion plus the issue #120 escape hatch."""
    provider_config = cast(
        DaytonaProviderConfig,
        fetch_sandbox_provider_config(daytona_secret_name, aws_credentials, "daytona"),
    )

    test_environment = f"cleanup-test-{uuid.uuid4().hex}"
    eligible_labels = valkyrie_sandbox_labels(test_environment)
    exempt_labels = {**eligible_labels, "clean-up": "false"}
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
            labels=eligible_labels,
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
        await _wait_until_daytona_sandboxes_are_listed(
            daytona,
            labels={MANAGED_BY_LABEL: MANAGED_BY_VALKYRIE, ENVIRONMENT_LABEL: test_environment},
            target=provider_config.DAYTONA_TARGET,
            sandbox_ids={eligible.id, exempt.id},
        )
        report = await cleanup_old_sandboxes(
            daytona,
            sandbox_provider,
            now=datetime.now(UTC) + timedelta(hours=49),
            environment=test_environment,
            target=provider_config.DAYTONA_TARGET,
            dry_run=False,
        )

        assert report.succeeded
        assert report.eligible == 1
        assert report.deletion_requested == 1
        assert report.exempted == 1
        await _wait_until_daytona_sandbox_is_absent(daytona, eligible.id)
        assert (await daytona.get(exempt.id)).id == exempt.id
