"""Unit tests for the Daytona orphan-sandbox cleanup engine."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest
from benchmark_service import SandboxError
from daytona import (
    AsyncSandbox,
    DaytonaConnectionError,
    ListSandboxesQuery,
)

import tracker.daytona_cleanup as cleanup_module
from tracker.daytona_cleanup import cleanup_old_sandboxes
from tracker.sandbox_labels import valkyrie_sandbox_labels

NOW = datetime(2026, 7, 9, 12, tzinfo=UTC)
TARGET = "us-test"
ENVIRONMENT = "production"


def _sandbox(
    sandbox_id: str,
    *,
    created_at: datetime | str | None,
    labels: dict[str, str] | None = None,
    target: str = TARGET,
) -> AsyncSandbox:
    timestamp = created_at.isoformat().replace("+00:00", "Z") if isinstance(created_at, datetime) else created_at
    return cast(
        AsyncSandbox,
        SimpleNamespace(
            id=sandbox_id,
            name=f"sandbox-{sandbox_id}",
            labels=labels if labels is not None else valkyrie_sandbox_labels(ENVIRONMENT),
            created_at=timestamp,
            target=target,
        ),
    )


class FakeDaytona:
    def __init__(
        self,
        sandboxes: list[AsyncSandbox],
        *,
        delete_effects: dict[str, BaseException | bool] | None = None,
    ) -> None:
        self.sandboxes = sandboxes
        self.delete_effects = delete_effects or {}
        self.delete_calls: list[str] = []
        self.query: ListSandboxesQuery | None = None

    async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
        self.query = query
        for sandbox in self.sandboxes:
            yield sandbox

    async def force_delete_sandbox(self, instance_id: str) -> bool:
        self.delete_calls.append(instance_id)
        effect = self.delete_effects.get(instance_id, True)
        if isinstance(effect, BaseException):
            raise effect
        return effect


async def test_cleanup_deletes_only_explicitly_owned_old_sandboxes() -> None:
    cutoff = NOW - timedelta(hours=48)
    wrong_environment = valkyrie_sandbox_labels("dev")
    missing_cleanup = valkyrie_sandbox_labels(ENVIRONMENT)
    del missing_cleanup["clean-up"]
    exempt = {**valkyrie_sandbox_labels(ENVIRONMENT), "clean-up": " FALSE "}
    unknown_cleanup = {**valkyrie_sandbox_labels(ENVIRONMENT), "clean-up": "sometimes"}

    client = FakeDaytona(
        [
            _sandbox("eligible", created_at=cutoff - timedelta(seconds=1)),
            _sandbox("at-cutoff", created_at=cutoff),
            _sandbox("newer", created_at=cutoff + timedelta(seconds=1)),
            _sandbox("wrong-environment", created_at=cutoff - timedelta(days=1), labels=wrong_environment),
            _sandbox("wrong-target", created_at=cutoff - timedelta(days=1), target="other-target"),
            _sandbox("missing-cleanup", created_at=cutoff - timedelta(days=1), labels=missing_cleanup),
            _sandbox("unknown-cleanup", created_at=cutoff - timedelta(days=1), labels=unknown_cleanup),
            _sandbox("exempt", created_at=cutoff - timedelta(days=1), labels=exempt),
            _sandbox("unmanaged", created_at=cutoff - timedelta(days=1), labels={"Benchmark": "legacy"}),
            _sandbox("malformed", created_at="not-a-timestamp"),
            _sandbox("naive", created_at="2026-07-01T12:00:00"),
        ]
    )

    report = await cleanup_old_sandboxes(
        client,
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
    )

    assert client.delete_calls == ["eligible"]
    assert client.query is not None
    assert client.query.created_at_before == cutoff
    assert client.query.targets == [TARGET]
    assert client.query.labels == {"ManagedBy": "Valkyrie", "Environment": ENVIRONMENT}
    assert client.query.limit == 200
    assert report.scanned == 11
    assert report.eligible == 1
    assert report.deletion_requested == 1
    assert report.already_absent == 0
    assert report.exempted == 1
    assert report.unmanaged == 2
    assert report.target_mismatch == 1
    assert report.not_old == 2
    assert report.invalid_metadata == 4
    assert not report.succeeded


async def test_cleanup_dry_run_reports_eligibility_without_deleting() -> None:
    client = FakeDaytona([_sandbox("eligible", created_at=NOW - timedelta(hours=49))])

    report = await cleanup_old_sandboxes(
        client,
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=True,
    )

    assert client.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_requested == 0
    assert report.succeeded


async def test_cleanup_uses_provider_delete_outcomes_and_continues_after_failures() -> None:
    client = FakeDaytona(
        [
            _sandbox("deleted", created_at=NOW - timedelta(hours=49)),
            _sandbox("gone", created_at=NOW - timedelta(hours=49)),
            _sandbox("failed", created_at=NOW - timedelta(hours=49)),
            _sandbox("after-failure", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={
            "gone": False,
            "failed": SandboxError("invalid state"),
        },
    )

    report = await cleanup_old_sandboxes(
        client,
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
    )

    assert client.delete_calls == ["deleted", "gone", "failed", "after-failure"]
    assert report.eligible == 4
    assert report.deletion_requested == 2
    assert report.already_absent == 1
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [("failed", "SandboxError")]
    assert not report.succeeded


async def test_cleanup_materializes_listing_before_any_deletion() -> None:
    class FailingListDaytona(FakeDaytona):
        async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
            self.query = query
            yield self.sandboxes[0]
            raise DaytonaConnectionError("pagination failed")

    client = FailingListDaytona([_sandbox("first", created_at=NOW - timedelta(hours=49))])

    with pytest.raises(DaytonaConnectionError, match="pagination failed"):
        await cleanup_old_sandboxes(
            client,
            client,
            now=NOW,
            environment=ENVIRONMENT,
            target=TARGET,
        )

    assert client.delete_calls == []


async def test_cleanup_normalizes_offset_aware_creation_timestamps() -> None:
    client = FakeDaytona([_sandbox("offset", created_at="2026-07-07T12:59:59+01:00")])

    report = await cleanup_old_sandboxes(
        client,
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
    )

    assert client.delete_calls == ["offset"]
    assert report.deletion_requested == 1
    assert report.succeeded


async def test_cleanup_requires_timezone_aware_now() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await cleanup_old_sandboxes(
            FakeDaytona([]),
            FakeDaytona([]),
            now=NOW.replace(tzinfo=None),
            environment=ENVIRONMENT,
            target=TARGET,
        )


@pytest.mark.parametrize(("dry_run_value", "expected_dry_run"), [("true", True), ("false", False)])
async def test_run_cleanup_uses_fixed_production_contract(
    monkeypatch: pytest.MonkeyPatch,
    dry_run_value: str,
    expected_dry_run: bool,
) -> None:
    client = FakeDaytona([_sandbox("eligible", created_at=NOW - timedelta(hours=49))])
    received_daytona_configs: list[object] = []
    received_provider_configs: list[cleanup_module.DaytonaProviderConfig] = []
    closed_contexts: list[str] = []

    class ClientContext:
        async def __aenter__(self) -> FakeDaytona:
            return client

        async def __aexit__(self, *_exc: object) -> None:
            closed_contexts.append("daytona")

    class ProviderContext:
        async def __aenter__(self) -> FakeDaytona:
            return client

        async def __aexit__(self, *_exc: object) -> None:
            closed_contexts.append("provider")

    def fake_daytona(*, config: object) -> ClientContext:
        received_daytona_configs.append(config)
        return ClientContext()

    def fake_create_provider(config: cleanup_module.DaytonaProviderConfig) -> ProviderContext:
        received_provider_configs.append(config)
        return ProviderContext()

    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example.test/api")
    monkeypatch.setenv("DAYTONA_TARGET", TARGET)
    monkeypatch.setenv("DAYTONA_CLEANUP_DRY_RUN", dry_run_value)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setattr(cleanup_module, "AsyncDaytona", fake_daytona)
    monkeypatch.setattr(cleanup_module.DaytonaProviderConfig, "create_provider", fake_create_provider)

    report = await cleanup_module.run_cleanup(now=NOW)

    assert len(received_daytona_configs) == 1
    daytona_config = received_daytona_configs[0]
    assert getattr(daytona_config, "api_key") == "test-key"
    assert getattr(daytona_config, "api_url") == "https://daytona.example.test/api"
    assert getattr(daytona_config, "target") == TARGET
    assert len(received_provider_configs) == 1
    provider_config = received_provider_configs[0]
    assert provider_config.DAYTONA_API_KEY == "test-key"
    assert provider_config.DAYTONA_API_URL == "https://daytona.example.test/api"
    assert provider_config.DAYTONA_TARGET == TARGET
    assert closed_contexts == ["provider", "daytona"]
    assert client.query is not None
    assert client.query.labels == {"ManagedBy": "Valkyrie", "Environment": "production"}
    assert client.query.created_at_before == NOW - timedelta(hours=48)
    assert report.dry_run is expected_dry_run
    assert client.delete_calls == ([] if expected_dry_run else ["eligible"])
