"""Unit tests for the Daytona orphan-sandbox cleanup engine."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast

import pytest
from benchmark_service import SandboxError
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import (
    AsyncSandbox,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
    DaytonaValidationError,
    ListSandboxesQuery,
)
from tenacity import RetryCallState, wait_none

import tracker.daytona_cleanup as cleanup_module
from tracker.daytona_cleanup import CleanupFailure, CleanupReport, DaytonaCleanupError, cleanup_old_sandboxes

NOW = datetime(2026, 7, 9, 12, tzinfo=UTC)
TARGET = "us-test"


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
            labels=labels or {},
            created_at=timestamp,
            target=target,
        ),
    )


class FakeDaytona:
    def __init__(
        self,
        sandboxes: list[AsyncSandbox],
        *,
        delete_effects: dict[str, BaseException | str] | None = None,
        current_sandboxes: dict[str, AsyncSandbox | BaseException] | None = None,
    ) -> None:
        self.sandboxes = sandboxes
        self.delete_effects = delete_effects or {}
        self.current_sandboxes = current_sandboxes or {sandbox.id: sandbox for sandbox in sandboxes}
        self.delete_calls: list[str] = []
        self.get_calls: list[str] = []
        self.query: ListSandboxesQuery | None = None

    async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
        self.query = query
        for sandbox in self.sandboxes:
            yield sandbox

    async def get(self, sandbox_id_or_name: str) -> AsyncSandbox:
        self.get_calls.append(sandbox_id_or_name)
        sandbox = self.current_sandboxes[sandbox_id_or_name]
        if isinstance(sandbox, BaseException):
            raise sandbox
        return sandbox

    async def delete_sandbox(self, instance_id: str) -> None:
        self.delete_calls.append(instance_id)
        effect = self.delete_effects.get(instance_id)
        if isinstance(effect, BaseException):
            raise effect
        if effect == "block":
            await asyncio.Event().wait()


def _report(*, succeeded: bool = True, dry_run: bool = True) -> CleanupReport:
    failures = () if succeeded else (CleanupFailure("id", "name", "SandboxError"),)
    return CleanupReport(
        cutoff=NOW - timedelta(hours=48),
        dry_run=dry_run,
        scanned=1,
        eligible=1,
        deletion_completed=0 if dry_run else 1,
        exempted=0,
        target_mismatch=0,
        not_old=0,
        invalid_metadata=0,
        failures=failures,
    )


async def test_cleanup_targets_all_old_sandboxes_unless_explicitly_exempt() -> None:
    cutoff = NOW - timedelta(hours=48)
    client = FakeDaytona(
        [
            _sandbox("unlabeled", created_at=cutoff - timedelta(seconds=1)),
            _sandbox("enabled", created_at=cutoff - timedelta(days=1), labels={"clean-up": "true"}),
            _sandbox("unknown-label", created_at=cutoff - timedelta(days=1), labels={"clean-up": "sometimes"}),
            _sandbox("at-cutoff", created_at=cutoff),
            _sandbox("newer", created_at=cutoff + timedelta(seconds=1)),
            _sandbox("wrong-target", created_at=cutoff - timedelta(days=1), target="other-target"),
            _sandbox("exempt", created_at=cutoff - timedelta(days=1), labels={"clean-up": " FALSE "}),
            _sandbox("malformed", created_at="not-a-timestamp"),
            _sandbox("naive", created_at="2026-07-01T12:00:00"),
            _sandbox("missing", created_at=None),
        ]
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.delete_calls == ["unlabeled", "enabled", "unknown-label"]
    assert client.query is not None
    assert client.query.created_at_before == cutoff
    assert client.query.targets == [TARGET]
    assert client.query.labels is None
    assert client.query.limit == 200
    assert report.scanned == 10
    assert report.eligible == 3
    assert report.deletion_completed == 3
    assert report.exempted == 1
    assert report.target_mismatch == 1
    assert report.not_old == 2
    assert report.invalid_metadata == 3
    assert not report.succeeded


async def test_cleanup_dry_run_reports_eligibility_without_deleting() -> None:
    client = FakeDaytona([_sandbox("eligible", created_at=NOW - timedelta(hours=49))])

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=True)

    assert client.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_completed == 0
    assert report.succeeded


async def test_cleanup_rechecks_mutable_opt_out_immediately_before_deletion() -> None:
    listed = _sandbox("newly-exempt", created_at=NOW - timedelta(hours=49))
    current = _sandbox(
        "newly-exempt",
        created_at=NOW - timedelta(hours=49),
        labels={"clean-up": "false"},
    )
    client = FakeDaytona([listed], current_sandboxes={listed.id: current})

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.get_calls == [listed.id]
    assert client.delete_calls == []
    assert report.eligible == 0
    assert report.exempted == 1
    assert report.deletion_completed == 0
    assert report.succeeded


async def test_cleanup_treats_candidate_disappearing_before_deletion_as_complete() -> None:
    listed = _sandbox("already-absent", created_at=NOW - timedelta(hours=49))
    client = FakeDaytona(
        [listed],
        current_sandboxes={listed.id: DaytonaNotFoundError("sandbox no longer exists")},
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.get_calls == [listed.id]
    assert client.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_completed == 1
    assert report.succeeded


async def test_cleanup_records_refresh_failure_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    failed = _sandbox("refresh-failed", created_at=NOW - timedelta(hours=49))
    deleted = _sandbox("deleted", created_at=NOW - timedelta(hours=49))
    client = FakeDaytona(
        [failed, deleted],
        current_sandboxes={
            failed.id: DaytonaConnectionError("refresh failed"),
            deleted.id: deleted,
        },
    )

    monkeypatch.setattr(
        cleanup_module,
        "_get_sandbox",
        cleanup_module._get_sandbox.retry_with(wait=wait_none()),  # pyright: ignore[reportPrivateUsage,reportFunctionMemberAccess]
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.get_calls == [failed.id] * 3 + [deleted.id]
    assert client.delete_calls == [deleted.id]
    assert report.deletion_completed == 1
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [
        (failed.id, "DaytonaConnectionError")
    ]
    assert not report.succeeded


@pytest.mark.parametrize(
    "transient_error",
    [
        DaytonaConnectionError("connection failed transiently"),
        DaytonaRateLimitError("rate limited transiently", headers={"retry-after-sandbox-lifecycle": "0"}),
        DaytonaTimeoutError("refresh timed out transiently"),
        DaytonaError("gateway failed transiently", status_code=502),
    ],
)
async def test_cleanup_retries_transient_refresh_before_deletion(
    monkeypatch: pytest.MonkeyPatch,
    transient_error: BaseException,
) -> None:
    class TransientGetDaytona(FakeDaytona):
        get_attempts = 0

        async def get(self, sandbox_id_or_name: str) -> AsyncSandbox:
            self.get_calls.append(sandbox_id_or_name)
            self.get_attempts += 1
            if self.get_attempts == 1:
                raise transient_error
            sandbox = self.current_sandboxes[sandbox_id_or_name]
            if isinstance(sandbox, BaseException):
                raise sandbox
            return sandbox

    sandbox = _sandbox("transient-refresh", created_at=NOW - timedelta(hours=49))
    client = TransientGetDaytona([sandbox])
    monkeypatch.setattr(
        cleanup_module,
        "_get_sandbox",
        cleanup_module._get_sandbox.retry_with(wait=wait_none()),  # pyright: ignore[reportPrivateUsage,reportFunctionMemberAccess]
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.get_calls == [sandbox.id, sandbox.id]
    assert client.delete_calls == [sandbox.id]
    assert report.deletion_completed == 1
    assert report.succeeded


async def test_cleanup_does_not_retry_non_transient_refresh_failure() -> None:
    sandbox = _sandbox("invalid-refresh", created_at=NOW - timedelta(hours=49))
    client = FakeDaytona(
        [sandbox],
        current_sandboxes={sandbox.id: DaytonaValidationError("invalid request")},
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.get_calls == [sandbox.id]
    assert client.delete_calls == []
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [
        (sandbox.id, "DaytonaValidationError")
    ]


async def test_cleanup_uses_provider_delete_and_continues_after_failures() -> None:
    client = FakeDaytona(
        [
            _sandbox("deleted", created_at=NOW - timedelta(hours=49)),
            _sandbox("failed", created_at=NOW - timedelta(hours=49)),
            _sandbox("after-failure", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={"failed": SandboxError("invalid state")},
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.delete_calls == ["deleted", "failed", "after-failure"]
    assert report.eligible == 3
    assert report.deletion_completed == 2
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [("failed", "SandboxError")]
    assert not report.succeeded


async def test_cleanup_bounds_each_delete_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cleanup_module, "_DELETE_TIMEOUT_SECONDS", 0.01)
    client = FakeDaytona(
        [
            _sandbox("blocked", created_at=NOW - timedelta(hours=49)),
            _sandbox("after-timeout", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={"blocked": "block"},
    )

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.delete_calls == ["blocked", "after-timeout"]
    assert report.deletion_completed == 1
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [("blocked", "TimeoutError")]


async def test_cleanup_retries_complete_listing_before_any_deletion(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingListDaytona(FakeDaytona):
        list_calls = 0

        async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
            self.list_calls += 1
            self.query = query
            yield self.sandboxes[0]
            raise DaytonaError("pagination failed", status_code=502)

    client = FailingListDaytona([_sandbox("first", created_at=NOW - timedelta(hours=49))])
    monkeypatch.setattr(
        cleanup_module,
        "_list_sandboxes",
        cleanup_module._list_sandboxes.retry_with(wait=wait_none()),  # pyright: ignore[reportPrivateUsage,reportFunctionMemberAccess]
    )

    with pytest.raises(DaytonaError, match="pagination failed"):
        await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET)

    assert client.list_calls == 3
    assert client.delete_calls == []


async def test_cleanup_honors_retry_after_and_restarts_listing_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RateLimitedListDaytona(FakeDaytona):
        list_calls = 0

        async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
            self.list_calls += 1
            self.query = query
            if self.list_calls == 1:
                yield self.sandboxes[0]
                raise DaytonaRateLimitError(
                    "rate limited",
                    headers={"Retry-After-Sandbox-Lifecycle": "0"},
                )
            for sandbox in self.sandboxes:
                yield sandbox

    client = RateLimitedListDaytona(
        [
            _sandbox("first", created_at=NOW - timedelta(hours=49)),
            _sandbox("second", created_at=NOW - timedelta(hours=49)),
        ]
    )
    retry_after_errors: list[DaytonaRateLimitError] = []

    def fake_retry_after(exc: DaytonaRateLimitError) -> float:
        retry_after_errors.append(exc)
        return 0

    monkeypatch.setattr(cleanup_module, "daytona_retry_after_seconds", fake_retry_after)

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.list_calls == 2
    assert len(retry_after_errors) == 1
    assert client.delete_calls == ["first", "second"]
    assert report.deletion_completed == 2
    assert report.succeeded


@pytest.mark.parametrize(
    ("error", "expected_wait"),
    [
        (
            DaytonaRateLimitError(
                "rate limited",
                headers={"Retry-After-Sandbox-Lifecycle": "7.25"},
            ),
            7.25,
        ),
        (
            DaytonaRateLimitError(
                "rate limited without a usable header",
                headers={"Retry-After-Sandbox-Lifecycle": "invalid"},
            ),
            2,
        ),
        (DaytonaConnectionError("connection failed"), 2),
    ],
)
def test_daytona_read_retry_wait_honors_retry_after_and_falls_back(
    error: BaseException,
    expected_wait: float,
) -> None:
    state = cast(
        RetryCallState,
        SimpleNamespace(
            outcome=SimpleNamespace(exception=lambda: error),
            attempt_number=2,
        ),
    )

    assert (
        cleanup_module._daytona_read_retry_wait(state)  # pyright: ignore[reportPrivateUsage]
        == expected_wait
    )


async def test_cleanup_normalizes_offset_aware_creation_timestamps() -> None:
    client = FakeDaytona([_sandbox("offset", created_at="2026-07-07T12:59:59+01:00")])

    report = await cleanup_old_sandboxes(client, client, now=NOW, target=TARGET, dry_run=False)

    assert client.delete_calls == ["offset"]
    assert report.deletion_completed == 1
    assert report.succeeded


async def test_cleanup_requires_timezone_aware_now() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await cleanup_old_sandboxes(
            FakeDaytona([]),
            FakeDaytona([]),
            now=NOW.replace(tzinfo=None),
            target=TARGET,
        )


@pytest.mark.parametrize("dry_run", [True, False])
async def test_run_cleanup_uses_provider_config_and_closes_both_clients(
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
) -> None:
    client = FakeDaytona([_sandbox("eligible", created_at=NOW - timedelta(hours=49))])
    received_daytona_configs: list[object] = []
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

    def fake_create_provider(_config: DaytonaProviderConfig) -> ProviderContext:
        return ProviderContext()

    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=TARGET,
    )
    monkeypatch.setattr(cleanup_module, "AsyncDaytona", fake_daytona)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", fake_create_provider)

    report = await cleanup_module.run_cleanup(provider_config, now=NOW, dry_run=dry_run)

    assert len(received_daytona_configs) == 1
    daytona_config = received_daytona_configs[0]
    assert getattr(daytona_config, "api_key") == "test-key"
    assert getattr(daytona_config, "api_url") == "https://daytona.example.test/api"
    assert getattr(daytona_config, "target") == TARGET
    assert closed_contexts == ["provider", "daytona"]
    assert client.query is not None
    assert client.query.created_at_before == NOW - timedelta(hours=48)
    assert report.dry_run is dry_run
    assert client.delete_calls == ([] if dry_run else ["eligible"])


class FakeSecretsManager:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.secret_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, object]:
        self.secret_ids.append(SecretId)
        return self.response


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ({}, "JSON SecretString"),
        ({"SecretString": "not-json"}, "valid JSON"),
        ({"SecretString": "[]"}, "JSON object"),
    ],
)
def test_load_provider_config_rejects_malformed_secret_payloads(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, object],
    message: str,
) -> None:
    fake = FakeSecretsManager(response)

    def fake_client(_service: str) -> FakeSecretsManager:
        return fake

    monkeypatch.setattr(cleanup_module.boto3, "client", fake_client)

    with pytest.raises(RuntimeError, match=message):
        cleanup_module._load_provider_config("cleanup-secret")  # pyright: ignore[reportPrivateUsage]

    assert fake.secret_ids == ["cleanup-secret"]


def test_load_provider_config_validates_secret_without_exposing_values(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = {
        "DAYTONA_API_KEY": "super-secret-key",
        "DAYTONA_API_URL": "https://daytona.example.test/api",
    }
    fake = FakeSecretsManager({"SecretString": json.dumps(secret)})

    def fake_client(_service: str) -> FakeSecretsManager:
        return fake

    monkeypatch.setattr(cleanup_module.boto3, "client", fake_client)

    with pytest.raises(RuntimeError, match="DAYTONA_TARGET") as exc_info:
        cleanup_module._load_provider_config("cleanup-secret")  # pyright: ignore[reportPrivateUsage]

    assert "super-secret-key" not in str(exc_info.value)
    assert fake.secret_ids == ["cleanup-secret"]


def test_load_provider_config_returns_required_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = {
        "DAYTONA_API_KEY": "test-key",
        "DAYTONA_API_URL": "https://daytona.example.test/api",
        "DAYTONA_TARGET": TARGET,
    }
    fake = FakeSecretsManager({"SecretString": json.dumps(secret)})

    def fake_client(_service: str) -> FakeSecretsManager:
        return fake

    monkeypatch.setattr(cleanup_module.boto3, "client", fake_client)

    config = cleanup_module._load_provider_config("cleanup-secret")  # pyright: ignore[reportPrivateUsage]

    assert config.DAYTONA_API_KEY == "test-key"
    assert config.DAYTONA_API_URL == "https://daytona.example.test/api"
    assert config.DAYTONA_TARGET == TARGET


class FakeLambdaContext:
    def __init__(self, remaining_milliseconds: int) -> None:
        self.remaining_milliseconds = remaining_milliseconds

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_milliseconds


def test_lambda_handler_is_fail_closed_and_returns_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=TARGET,
    )
    received: list[tuple[DaytonaProviderConfig, bool]] = []

    async def fake_run_cleanup(provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        received.append((provider_config, dry_run))
        return _report(dry_run=dry_run)

    monkeypatch.setenv("DAYTONA_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("DAYTONA_CLEANUP_DRY_RUN", "unexpected")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def fake_load_provider_config(_name: str) -> DaytonaProviderConfig:
        return config

    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", fake_run_cleanup)

    result = cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))

    assert received == [(config, True)]
    assert result["dry_run"] is True
    assert result["eligible"] == 1


def test_lambda_handler_raises_for_unsuccessful_report(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=TARGET,
    )

    async def fake_run_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        return _report(succeeded=False, dry_run=dry_run)

    monkeypatch.setenv("DAYTONA_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("DAYTONA_CLEANUP_DRY_RUN", "false")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def fake_load_provider_config(_name: str) -> DaytonaProviderConfig:
        return config

    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", fake_run_cleanup)

    with pytest.raises(DaytonaCleanupError):
        cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))


def test_lambda_handler_rejects_insufficient_time_before_loading_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def unexpected_load(_name: str) -> DaytonaProviderConfig:
        pytest.fail("secret must not be loaded")

    monkeypatch.setattr(cleanup_module, "_load_provider_config", unexpected_load)

    with pytest.raises(RuntimeError, match="Insufficient Lambda time"):
        cleanup_module.lambda_handler({}, FakeLambdaContext(60_000))


def test_lambda_handler_rechecks_time_after_loading_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=TARGET,
    )
    context = FakeLambdaContext(840_000)
    cleanup_called = False

    def fake_load_provider_config(_name: str) -> DaytonaProviderConfig:
        context.remaining_milliseconds = 60_000
        return config

    async def unexpected_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        nonlocal cleanup_called
        del dry_run
        cleanup_called = True
        return _report()

    monkeypatch.setenv("DAYTONA_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)
    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", unexpected_cleanup)

    with pytest.raises(RuntimeError, match="Insufficient Lambda time"):
        cleanup_module.lambda_handler({}, context)

    assert cleanup_called is False


def test_lambda_handler_bounds_entire_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=TARGET,
    )

    async def blocked_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        del dry_run
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setenv("DAYTONA_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def fake_load_provider_config(_name: str) -> DaytonaProviderConfig:
        return config

    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", blocked_cleanup)

    with pytest.raises(TimeoutError):
        cleanup_module.lambda_handler({}, FakeLambdaContext(60_010))
