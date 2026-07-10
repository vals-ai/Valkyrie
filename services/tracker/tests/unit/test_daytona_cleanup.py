"""Unit tests for the Daytona orphan-sandbox cleanup engine."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest
from daytona import (
    AsyncSandbox,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaValidationError,
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
        delete_effects: dict[str, list[BaseException | None]] | None = None,
    ) -> None:
        self.sandboxes = sandboxes
        self.delete_effects = delete_effects or {}
        self.delete_calls: list[str] = []
        self.query: ListSandboxesQuery | None = None

    async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
        self.query = query
        for sandbox in self.sandboxes:
            yield sandbox

    async def delete(self, sandbox: AsyncSandbox, timeout: float = 60) -> None:
        del timeout
        self.delete_calls.append(sandbox.id)
        effects = self.delete_effects.get(sandbox.id, [])
        if effects:
            effect = effects.pop(0)
            if effect is not None:
                raise effect


async def _no_sleep(_delay: float) -> None:
    return None


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
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
        sleep=_no_sleep,
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
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=True,
        sleep=_no_sleep,
    )

    assert client.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_requested == 0
    assert report.succeeded


async def test_cleanup_retries_transient_deletes_and_continues_after_failures() -> None:
    client = FakeDaytona(
        [
            _sandbox("retried", created_at=NOW - timedelta(hours=49)),
            _sandbox("gone", created_at=NOW - timedelta(hours=49)),
            _sandbox("generic-gone", created_at=NOW - timedelta(hours=49)),
            _sandbox("generic-code-gone", created_at=NOW - timedelta(hours=49)),
            _sandbox("failed", created_at=NOW - timedelta(hours=49)),
            _sandbox("persistent", created_at=NOW - timedelta(hours=49)),
            _sandbox("after-failure", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={
            "retried": [DaytonaConnectionError("temporary"), None],
            "gone": [DaytonaNotFoundError("gone")],
            "generic-gone": [DaytonaError("gone", status_code=404)],
            "generic-code-gone": [DaytonaError("gone", error_code="NOT_FOUND")],
            "failed": [DaytonaValidationError("invalid state")],
            "persistent": [
                DaytonaConnectionError("temporary"),
                DaytonaConnectionError("temporary"),
                DaytonaConnectionError("temporary"),
            ],
        },
    )
    sleep_calls: list[float] = []

    async def capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    report = await cleanup_old_sandboxes(
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
        sleep=capture_sleep,
    )

    assert client.delete_calls == [
        "retried",
        "retried",
        "gone",
        "generic-gone",
        "generic-code-gone",
        "failed",
        "persistent",
        "persistent",
        "persistent",
        "after-failure",
    ]
    assert sleep_calls == [1.0, 1.0, 4.0]
    assert report.eligible == 7
    assert report.deletion_requested == 2
    assert report.already_absent == 3
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [
        ("failed", "DaytonaValidationError"),
        ("persistent", "DaytonaConnectionError"),
    ]
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
            now=NOW,
            environment=ENVIRONMENT,
            target=TARGET,
            sleep=_no_sleep,
        )

    assert client.delete_calls == []


async def test_cleanup_normalizes_offset_aware_creation_timestamps() -> None:
    client = FakeDaytona([_sandbox("offset", created_at="2026-07-07T12:59:59+01:00")])

    report = await cleanup_old_sandboxes(
        client,
        now=NOW,
        environment=ENVIRONMENT,
        target=TARGET,
        dry_run=False,
        sleep=_no_sleep,
    )

    assert client.delete_calls == ["offset"]
    assert report.deletion_requested == 1
    assert report.succeeded


@pytest.mark.parametrize(
    ("now", "max_age", "environment", "target", "message"),
    [
        (NOW.replace(tzinfo=None), timedelta(hours=48), ENVIRONMENT, TARGET, "timezone-aware"),
        (NOW, timedelta(0), ENVIRONMENT, TARGET, "max_age must be positive"),
        (NOW, timedelta(hours=-1), ENVIRONMENT, TARGET, "max_age must be positive"),
        (NOW, timedelta(hours=48), "", TARGET, "environment must not be empty"),
        (NOW, timedelta(hours=48), ENVIRONMENT, "", "target must not be empty"),
    ],
)
async def test_cleanup_rejects_unsafe_boundaries(
    now: datetime,
    max_age: timedelta,
    environment: str,
    target: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await cleanup_old_sandboxes(
            FakeDaytona([]),
            now=now,
            environment=environment,
            target=target,
            max_age=max_age,
            sleep=_no_sleep,
        )


async def test_run_cleanup_requires_explicit_config_and_defaults_to_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeDaytona([_sandbox("eligible", created_at=NOW - timedelta(hours=49))])
    received_configs: list[object] = []

    class ClientContext:
        async def __aenter__(self) -> FakeDaytona:
            return client

        async def __aexit__(self, *_exc: object) -> None:
            return None

    def fake_daytona(*, config: object) -> ClientContext:
        received_configs.append(config)
        return ClientContext()

    monkeypatch.setenv("ENVIRONMENT", ENVIRONMENT)
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example.test/api")
    monkeypatch.setenv("DAYTONA_TARGET", TARGET)
    monkeypatch.delenv("DAYTONA_CLEANUP_DRY_RUN", raising=False)
    monkeypatch.delenv("DAYTONA_CLEANUP_MAX_AGE_HOURS", raising=False)
    monkeypatch.setattr(cleanup_module, "AsyncDaytona", fake_daytona)

    report = await cleanup_module.run_cleanup(now=NOW)

    assert len(received_configs) == 1
    config = received_configs[0]
    assert getattr(config, "api_key") == "test-key"
    assert getattr(config, "api_url") == "https://daytona.example.test/api"
    assert getattr(config, "target") == TARGET
    assert report.dry_run
    assert client.delete_calls == []


async def test_run_cleanup_rejects_non_48_hour_production_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", ENVIRONMENT)
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example.test/api")
    monkeypatch.setenv("DAYTONA_TARGET", TARGET)
    monkeypatch.setenv("DAYTONA_CLEANUP_MAX_AGE_HOURS", "47")

    with pytest.raises(ValueError, match="exactly 48 hours"):
        await cleanup_module.run_cleanup(now=NOW)


@pytest.mark.parametrize("name", ["DAYTONA_API_KEY", "DAYTONA_API_URL", "DAYTONA_TARGET"])
async def test_run_cleanup_rejects_missing_daytona_config(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key")
    monkeypatch.setenv("DAYTONA_API_URL", "https://daytona.example.test/api")
    monkeypatch.setenv("DAYTONA_TARGET", TARGET)
    monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match=f"{name} must be set"):
        await cleanup_module.run_cleanup(now=NOW)
