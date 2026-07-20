"""Unit tests for the provider-neutral sandbox cleanup engine."""

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
from benchmark_service.sandbox.modal import ModalProviderConfig
from daytona import (
    AsyncSandbox,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
    DaytonaValidationError,
    ListSandboxesQuery,
    SandboxState,
)
from tenacity import RetryCallState, wait_none

import tracker.daytona_cleanup as daytona_cleanup_module
import tracker.sandbox_cleanup as cleanup_module
from tracker.daytona_cleanup import DaytonaCleanupBackend
from tracker.sandbox_cleanup import (
    CleanupCandidate,
    CleanupFailure,
    CleanupReport,
    SandboxCleanupError,
    cleanup_old_sandboxes,
)

NOW = datetime(2026, 7, 9, 12, tzinfo=UTC)
SCOPE = "us-test"


def _candidate(
    sandbox_id: str,
    *,
    created_at: datetime | None,
    labels: dict[str, str] | None = None,
    scope: str | None = SCOPE,
    provider_data: object | None = None,
) -> CleanupCandidate:
    return CleanupCandidate(
        id=sandbox_id,
        name=f"sandbox-{sandbox_id}",
        created_at=created_at,
        labels=labels or {},
        scope=scope,
        provider_data=provider_data,
    )


class FakeCleanupBackend:
    """Non-Daytona backend proving that the cleanup policy is provider-neutral."""

    provider_name = "acme-sandbox"

    def __init__(
        self,
        candidates: list[CleanupCandidate],
        *,
        scope: str = SCOPE,
        current_candidates: dict[str, CleanupCandidate | BaseException | None] | None = None,
        delete_effects: dict[str, BaseException | str] | None = None,
        list_error: BaseException | None = None,
    ) -> None:
        self.scope = scope
        self.candidates = candidates
        self.current_candidates = current_candidates or {candidate.id: candidate for candidate in candidates}
        self.delete_effects = delete_effects or {}
        self.list_error = list_error
        self.list_cutoffs: list[datetime] = []
        self.refresh_calls: list[str] = []
        self.delete_calls: list[str] = []

    async def list_candidates(self, created_before: datetime) -> list[CleanupCandidate]:
        self.list_cutoffs.append(created_before)
        if self.list_error is not None:
            raise self.list_error
        return list(self.candidates)

    async def refresh_candidate(self, sandbox_id: str) -> CleanupCandidate | None:
        self.refresh_calls.append(sandbox_id)
        candidate = self.current_candidates[sandbox_id]
        if isinstance(candidate, BaseException):
            raise candidate
        return candidate

    async def delete_candidate(self, candidate: CleanupCandidate) -> None:
        self.delete_calls.append(candidate.id)
        effect = self.delete_effects.get(candidate.id)
        if isinstance(effect, BaseException):
            raise effect
        if effect == "block":
            await asyncio.Event().wait()


def _report(*, succeeded: bool = True, dry_run: bool = True) -> CleanupReport:
    failures = () if succeeded else (CleanupFailure("id", "name", "SandboxError"),)
    return CleanupReport(
        provider="acme-sandbox",
        cutoff=NOW - timedelta(hours=48),
        dry_run=dry_run,
        scanned=1,
        eligible=1,
        deletion_completed=0 if dry_run else 1,
        exempted=0,
        scope_mismatch=0,
        not_old=0,
        invalid_metadata=0,
        failures=failures,
    )


async def test_generic_cleanup_targets_all_old_candidates_unless_explicitly_exempt() -> None:
    cutoff = NOW - timedelta(hours=48)
    backend = FakeCleanupBackend(
        [
            _candidate("unlabeled", created_at=cutoff - timedelta(seconds=1)),
            _candidate("enabled", created_at=cutoff - timedelta(days=1), labels={"clean-up": "true"}),
            _candidate("unknown-label", created_at=cutoff - timedelta(days=1), labels={"clean-up": "sometimes"}),
            _candidate("at-cutoff", created_at=cutoff),
            _candidate("newer", created_at=cutoff + timedelta(seconds=1)),
            _candidate("wrong-scope", created_at=cutoff - timedelta(days=1), scope="another-scope"),
            _candidate("exempt", created_at=cutoff - timedelta(days=1), labels={"clean-up": " FALSE "}),
            _candidate("missing", created_at=None),
            _candidate("naive", created_at=datetime(2026, 7, 1, 12)),
        ]
    )

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.list_cutoffs == [cutoff]
    assert backend.refresh_calls == ["unlabeled", "enabled", "unknown-label"]
    assert backend.delete_calls == ["unlabeled", "enabled", "unknown-label"]
    assert report.scanned == 9
    assert report.eligible == 3
    assert report.deletion_completed == 3
    assert report.exempted == 1
    assert report.scope_mismatch == 1
    assert report.not_old == 2
    assert report.invalid_metadata == 2
    assert not report.succeeded


async def test_generic_cleanup_dry_run_reports_eligibility_without_refreshing_or_deleting() -> None:
    backend = FakeCleanupBackend([_candidate("eligible", created_at=NOW - timedelta(hours=49))])

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=True)

    assert backend.refresh_calls == []
    assert backend.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_completed == 0
    assert report.succeeded


@pytest.mark.parametrize(
    ("labels", "expected_exempted"),
    [
        ({"clean-up": "false"}, 1),
        ({"clean-up": " FaLsE\t"}, 1),
        ({"clean-up": "falsey"}, 0),
        ({"cleanup": "false"}, 0),
        ({"clean-up": "true"}, 0),
    ],
)
async def test_generic_cleanup_opt_out_requires_exact_normalized_label(
    labels: dict[str, str],
    expected_exempted: int,
) -> None:
    backend = FakeCleanupBackend([_candidate("candidate", created_at=NOW - timedelta(hours=49), labels=labels)])

    report = await cleanup_old_sandboxes(backend, now=NOW)

    assert report.exempted == expected_exempted
    assert report.eligible == 1 - expected_exempted


async def test_generic_cleanup_rechecks_mutable_opt_out_immediately_before_deletion() -> None:
    listed = _candidate("newly-exempt", created_at=NOW - timedelta(hours=49))
    current = _candidate(
        "newly-exempt",
        created_at=NOW - timedelta(hours=49),
        labels={"clean-up": "false"},
    )
    backend = FakeCleanupBackend([listed], current_candidates={listed.id: current})

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.refresh_calls == [listed.id]
    assert backend.delete_calls == []
    assert report.eligible == 0
    assert report.exempted == 1
    assert report.deletion_completed == 0
    assert report.succeeded


@pytest.mark.parametrize(
    ("current", "exclusion"),
    [
        (_candidate("changed", created_at=NOW - timedelta(hours=47)), "not_old"),
        (_candidate("changed", created_at=NOW - timedelta(hours=49), scope="other"), "scope_mismatch"),
        (_candidate("changed", created_at=None), "invalid_metadata"),
    ],
)
async def test_generic_cleanup_revalidates_all_mutable_metadata_before_deletion(
    current: CleanupCandidate,
    exclusion: str,
) -> None:
    listed = _candidate("changed", created_at=NOW - timedelta(hours=49))
    backend = FakeCleanupBackend([listed], current_candidates={listed.id: current})

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.delete_calls == []
    assert report.eligible == 0
    assert getattr(report, exclusion) == 1


async def test_generic_cleanup_treats_candidate_disappearing_before_deletion_as_complete() -> None:
    listed = _candidate("already-absent", created_at=NOW - timedelta(hours=49))
    backend = FakeCleanupBackend([listed], current_candidates={listed.id: None})

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.refresh_calls == [listed.id]
    assert backend.delete_calls == []
    assert report.eligible == 1
    assert report.deletion_completed == 1
    assert report.succeeded


async def test_generic_cleanup_rejects_mismatched_refresh_identity() -> None:
    listed = _candidate("listed", created_at=NOW - timedelta(hours=49))
    different = _candidate("different", created_at=NOW - timedelta(hours=49))
    backend = FakeCleanupBackend([listed], current_candidates={listed.id: different})

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.delete_calls == []
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [
        (listed.id, "CandidateIdentityMismatch")
    ]
    assert not report.succeeded


async def test_generic_cleanup_records_refresh_failure_and_continues() -> None:
    failed = _candidate("refresh-failed", created_at=NOW - timedelta(hours=49))
    deleted = _candidate("deleted", created_at=NOW - timedelta(hours=49))
    backend = FakeCleanupBackend(
        [failed, deleted],
        current_candidates={
            failed.id: RuntimeError("refresh failed"),
            deleted.id: deleted,
        },
    )

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.refresh_calls == [failed.id, deleted.id]
    assert backend.delete_calls == [deleted.id]
    assert report.deletion_completed == 1
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [(failed.id, "RuntimeError")]
    assert not report.succeeded


async def test_generic_cleanup_uses_backend_delete_and_continues_after_failures() -> None:
    backend = FakeCleanupBackend(
        [
            _candidate("deleted", created_at=NOW - timedelta(hours=49)),
            _candidate("failed", created_at=NOW - timedelta(hours=49)),
            _candidate("after-failure", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={"failed": SandboxError("invalid state")},
    )

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.delete_calls == ["deleted", "failed", "after-failure"]
    assert report.eligible == 3
    assert report.deletion_completed == 2
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [("failed", "SandboxError")]
    assert not report.succeeded


async def test_generic_cleanup_bounds_each_delete_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup_module, "_DELETE_TIMEOUT_SECONDS", 0.01)
    backend = FakeCleanupBackend(
        [
            _candidate("blocked", created_at=NOW - timedelta(hours=49)),
            _candidate("after-timeout", created_at=NOW - timedelta(hours=49)),
        ],
        delete_effects={"blocked": "block"},
    )

    report = await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.delete_calls == ["blocked", "after-timeout"]
    assert report.deletion_completed == 1
    assert [(failure.sandbox_id, failure.error_type) for failure in report.failures] == [("blocked", "TimeoutError")]


async def test_generic_cleanup_listing_failure_occurs_before_any_mutation() -> None:
    backend = FakeCleanupBackend(
        [_candidate("candidate", created_at=NOW - timedelta(hours=49))],
        list_error=RuntimeError("pagination failed"),
    )

    with pytest.raises(RuntimeError, match="pagination failed"):
        await cleanup_old_sandboxes(backend, now=NOW, dry_run=False)

    assert backend.refresh_calls == []
    assert backend.delete_calls == []


async def test_generic_cleanup_requires_timezone_aware_now() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await cleanup_old_sandboxes(FakeCleanupBackend([]), now=NOW.replace(tzinfo=None))


async def test_generic_cleanup_nonfatal_exclusions_do_not_fail_report() -> None:
    backend = FakeCleanupBackend(
        [
            _candidate("exempt", created_at=NOW - timedelta(hours=49), labels={"clean-up": "false"}),
            _candidate("new", created_at=NOW - timedelta(hours=47)),
        ]
    )

    report = await cleanup_old_sandboxes(backend, now=NOW)

    assert report.exempted == 1
    assert report.not_old == 1
    assert report.succeeded


def _sandbox(
    sandbox_id: str,
    *,
    created_at: datetime | str | None,
    labels: dict[str, str] | None = None,
    state: SandboxState = SandboxState.STARTED,
    target: str = SCOPE,
) -> AsyncSandbox:
    timestamp = created_at.isoformat().replace("+00:00", "Z") if isinstance(created_at, datetime) else created_at
    return cast(
        AsyncSandbox,
        SimpleNamespace(
            id=sandbox_id,
            name=f"sandbox-{sandbox_id}",
            labels=labels or {},
            created_at=timestamp,
            state=state,
            target=target,
        ),
    )


class FakeDaytonaClient:
    def __init__(
        self,
        sandboxes: list[AsyncSandbox],
        *,
        current_sandboxes: dict[str, AsyncSandbox | BaseException] | None = None,
    ) -> None:
        self.sandboxes = sandboxes
        self.current_sandboxes = current_sandboxes or {sandbox.id: sandbox for sandbox in sandboxes}
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


class FakeDeleteProvider:
    def __init__(self, effects: dict[str, BaseException] | None = None) -> None:
        self.effects = effects or {}
        self.delete_calls: list[str] = []

    async def delete_sandbox(self, instance_id: str) -> None:
        self.delete_calls.append(instance_id)
        effect = self.effects.get(instance_id)
        if effect is not None:
            raise effect


async def test_daytona_backend_normalizes_target_wide_list_results() -> None:
    cutoff = NOW - timedelta(hours=48)
    sandbox = _sandbox(
        "candidate",
        created_at="2026-07-07T12:59:59+01:00",
        labels={"team": "evals"},
    )
    client = FakeDaytonaClient([sandbox])
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)

    candidates = await backend.list_candidates(cutoff)

    assert candidates == [
        _candidate(
            "candidate",
            created_at=datetime(2026, 7, 7, 11, 59, 59, tzinfo=UTC),
            labels={"team": "evals"},
            provider_data=sandbox,
        )
    ]
    assert candidates[0].provider_data is sandbox
    assert client.query is not None
    assert client.query.created_at_before == cutoff
    assert client.query.targets == [SCOPE]
    assert client.query.labels is None
    assert client.query.limit == 200


@pytest.mark.parametrize("created_at", [None, "not-a-timestamp", "2026-07-01T12:00:00"])
async def test_daytona_backend_maps_invalid_creation_timestamps_to_none(created_at: str | None) -> None:
    client = FakeDaytonaClient([_sandbox("invalid", created_at=created_at)])
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)

    candidates = await backend.list_candidates(NOW)

    assert candidates[0].created_at is None


async def test_daytona_backend_refreshes_candidate_and_maps_not_found_to_none() -> None:
    sandbox = _sandbox("present", created_at=NOW - timedelta(hours=49))
    client = FakeDaytonaClient(
        [sandbox],
        current_sandboxes={
            sandbox.id: sandbox,
            "absent": DaytonaNotFoundError("gone"),
        },
    )
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)

    refreshed = await backend.refresh_candidate(sandbox.id)

    assert refreshed is not None
    assert refreshed.id == sandbox.id
    assert refreshed.provider_data is sandbox
    assert await backend.refresh_candidate("absent") is None
    assert client.get_calls == [sandbox.id, "absent"]


@pytest.mark.parametrize(
    "transient_error",
    [
        DaytonaConnectionError("connection failed transiently"),
        DaytonaRateLimitError("rate limited transiently", headers={"retry-after-sandbox-lifecycle": "0"}),
        DaytonaTimeoutError("refresh timed out transiently"),
        DaytonaError("gateway failed transiently", status_code=502),
    ],
)
async def test_daytona_backend_retries_transient_refresh(
    monkeypatch: pytest.MonkeyPatch,
    transient_error: BaseException,
) -> None:
    class TransientGetDaytona(FakeDaytonaClient):
        get_attempts = 0

        async def get(self, sandbox_id_or_name: str) -> AsyncSandbox:
            self.get_calls.append(sandbox_id_or_name)
            self.get_attempts += 1
            if self.get_attempts == 1:
                raise transient_error
            return self.current_sandboxes[sandbox_id_or_name]  # type: ignore[return-value]

    sandbox = _sandbox("transient-refresh", created_at=NOW - timedelta(hours=49))
    client = TransientGetDaytona([sandbox])
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)
    monkeypatch.setattr(
        daytona_cleanup_module,
        "_get_sandbox",
        daytona_cleanup_module._get_sandbox.retry_with(  # pyright: ignore[reportPrivateUsage,reportFunctionMemberAccess]
            wait=wait_none()
        ),
    )

    refreshed = await backend.refresh_candidate(sandbox.id)

    assert refreshed is not None
    assert refreshed.id == sandbox.id
    assert client.get_calls == [sandbox.id, sandbox.id]


async def test_daytona_backend_does_not_retry_non_transient_refresh_failure() -> None:
    sandbox = _sandbox("invalid-refresh", created_at=NOW - timedelta(hours=49))
    client = FakeDaytonaClient(
        [sandbox],
        current_sandboxes={sandbox.id: DaytonaValidationError("invalid request")},
    )
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)

    with pytest.raises(DaytonaValidationError, match="invalid request"):
        await backend.refresh_candidate(sandbox.id)

    assert client.get_calls == [sandbox.id]


async def test_daytona_backend_retries_complete_listing_before_returning(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingListDaytona(FakeDaytonaClient):
        list_calls = 0

        async def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]:
            self.list_calls += 1
            self.query = query
            yield self.sandboxes[0]
            raise DaytonaError("pagination failed", status_code=502)

    client = FailingListDaytona([_sandbox("first", created_at=NOW - timedelta(hours=49))])
    backend = DaytonaCleanupBackend(client, FakeDeleteProvider(), SCOPE)
    monkeypatch.setattr(
        daytona_cleanup_module,
        "_list_sandboxes",
        daytona_cleanup_module._list_sandboxes.retry_with(  # pyright: ignore[reportPrivateUsage,reportFunctionMemberAccess]
            wait=wait_none()
        ),
    )

    with pytest.raises(DaytonaError, match="pagination failed"):
        await backend.list_candidates(NOW)

    assert client.list_calls == 3


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

    assert daytona_cleanup_module._daytona_read_retry_wait(state) == expected_wait  # pyright: ignore[reportPrivateUsage]


async def test_daytona_backend_deletes_paused_sandbox_directly_with_idempotent_retry() -> None:
    paused_delete_errors: list[DaytonaError] = [
        DaytonaRateLimitError("rate limited", headers={"retry-after-sandbox-lifecycle": "0"}),
        DaytonaNotFoundError("already deleted"),
    ]
    paused_delete_attempts = 0

    async def delete_paused() -> None:
        nonlocal paused_delete_attempts
        paused_delete_attempts += 1
        raise paused_delete_errors.pop(0)

    paused = _sandbox("paused", created_at=NOW - timedelta(hours=49), state=SandboxState.PAUSED)
    setattr(paused, "delete", delete_paused)
    provider = FakeDeleteProvider()
    backend = DaytonaCleanupBackend(FakeDaytonaClient([paused]), provider, SCOPE)
    candidate = (await backend.list_candidates(NOW))[0]

    await backend.delete_candidate(candidate)

    assert paused_delete_attempts == 2
    assert provider.delete_calls == []


async def test_daytona_backend_uses_cbs_provider_for_non_paused_sandbox() -> None:
    started = _sandbox("started", created_at=NOW - timedelta(hours=49))
    provider = FakeDeleteProvider()
    backend = DaytonaCleanupBackend(FakeDaytonaClient([started]), provider, SCOPE)
    candidate = (await backend.list_candidates(NOW))[0]

    await backend.delete_candidate(candidate)

    assert provider.delete_calls == [started.id]


async def test_daytona_backend_context_builds_and_closes_both_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeDaytonaClient([])
    delete_provider = FakeDeleteProvider()
    received_configs: list[object] = []
    closed_contexts: list[str] = []

    class ClientContext:
        async def __aenter__(self) -> FakeDaytonaClient:
            return client

        async def __aexit__(self, *_exc: object) -> None:
            closed_contexts.append("daytona")

    class ProviderContext:
        async def __aenter__(self) -> FakeDeleteProvider:
            return delete_provider

        async def __aexit__(self, *_exc: object) -> None:
            closed_contexts.append("provider")

    def fake_daytona(*, config: object) -> ClientContext:
        received_configs.append(config)
        return ClientContext()

    def fake_create_provider(_config: DaytonaProviderConfig) -> ProviderContext:
        return ProviderContext()

    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=SCOPE,
    )
    monkeypatch.setattr(daytona_cleanup_module, "AsyncDaytona", fake_daytona)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", fake_create_provider)

    async with daytona_cleanup_module.daytona_cleanup_backend(config) as backend:
        assert backend.provider_name == "daytona"
        assert backend.scope == SCOPE

    assert len(received_configs) == 1
    daytona_config = received_configs[0]
    assert getattr(daytona_config, "api_key") == "test-key"
    assert getattr(daytona_config, "api_url") == "https://daytona.example.test/api"
    assert getattr(daytona_config, "target") == SCOPE
    assert closed_contexts == ["provider", "daytona"]


@pytest.mark.parametrize("dry_run", [True, False])
async def test_run_cleanup_uses_daytona_backend_context(
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
) -> None:
    backend = FakeCleanupBackend([_candidate("eligible", created_at=NOW - timedelta(hours=49))])
    entered: list[DaytonaProviderConfig] = []
    exited = False

    class BackendContext:
        async def __aenter__(self) -> FakeCleanupBackend:
            entered.append(provider_config)
            return backend

        async def __aexit__(self, *_exc: object) -> None:
            nonlocal exited
            exited = True

    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=SCOPE,
    )

    def fake_daytona_cleanup_backend(_config: DaytonaProviderConfig) -> BackendContext:
        return BackendContext()

    monkeypatch.setattr(daytona_cleanup_module, "daytona_cleanup_backend", fake_daytona_cleanup_backend)

    report = await cleanup_module.run_cleanup(provider_config, now=NOW, dry_run=dry_run)

    assert entered == [provider_config]
    assert exited
    assert report.dry_run is dry_run
    assert backend.delete_calls == ([] if dry_run else ["eligible"])


async def test_run_cleanup_rejects_unsupported_provider_before_mutation() -> None:
    config = ModalProviderConfig(MODAL_TOKEN_ID="test-id", MODAL_TOKEN_SECRET="test-secret")

    with pytest.raises(RuntimeError, match="does not support provider 'modal'"):
        await cleanup_module.run_cleanup(config, now=NOW, dry_run=False)


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
        cleanup_module._load_provider_config("cleanup-secret", "daytona")  # pyright: ignore[reportPrivateUsage]

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

    with pytest.raises(RuntimeError) as exc_info:
        cleanup_module._load_provider_config("cleanup-secret", "daytona")  # pyright: ignore[reportPrivateUsage]

    assert "super-secret-key" not in str(exc_info.value)
    assert fake.secret_ids == ["cleanup-secret"]


def test_load_provider_config_parses_selected_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = {
        "DAYTONA_API_KEY": "test-key",
        "DAYTONA_API_URL": "https://daytona.example.test/api",
        "DAYTONA_TARGET": SCOPE,
    }
    fake = FakeSecretsManager({"SecretString": json.dumps(secret)})

    def fake_client(_service: str) -> FakeSecretsManager:
        return fake

    monkeypatch.setattr(cleanup_module.boto3, "client", fake_client)

    config = cleanup_module._load_provider_config("cleanup-secret", "daytona")  # pyright: ignore[reportPrivateUsage]

    assert isinstance(config, DaytonaProviderConfig)
    assert config.DAYTONA_API_KEY == "test-key"
    assert config.DAYTONA_API_URL == "https://daytona.example.test/api"
    assert config.DAYTONA_TARGET == SCOPE


class FakeLambdaContext:
    def __init__(self, remaining_milliseconds: int) -> None:
        self.remaining_milliseconds = remaining_milliseconds

    def get_remaining_time_in_millis(self) -> int:
        return self.remaining_milliseconds


def test_lambda_handler_uses_generic_provider_env_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=SCOPE,
    )
    loaded: list[tuple[str, str]] = []
    received: list[tuple[DaytonaProviderConfig, bool]] = []

    def fake_load_provider_config(secret_name: str, provider_type: str) -> DaytonaProviderConfig:
        loaded.append((secret_name, provider_type))
        return config

    async def fake_run_cleanup(provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        received.append((provider_config, dry_run))
        return _report(dry_run=dry_run)

    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("SANDBOX_CLEANUP_DRY_RUN", "unexpected")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)
    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", fake_run_cleanup)

    result = cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))

    assert loaded == [("cleanup-secret", "daytona")]
    assert received == [(config, True)]
    assert result["dry_run"] is True
    assert result["eligible"] == 1
    assert "scope_mismatch" in result
    assert "target_mismatch" not in result


def test_lambda_handler_raises_generic_error_for_unsuccessful_report(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=SCOPE,
    )

    async def fake_run_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        return _report(succeeded=False, dry_run=dry_run)

    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("SANDBOX_CLEANUP_DRY_RUN", "false")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def fake_load_provider_config(_name: str, _provider: str) -> DaytonaProviderConfig:
        return config

    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", fake_run_cleanup)

    with pytest.raises(SandboxCleanupError):
        cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))


def test_lambda_handler_rejects_insufficient_time_before_loading_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def unexpected_load(_name: str, _provider: str) -> DaytonaProviderConfig:
        pytest.fail("secret must not be loaded")

    monkeypatch.setattr(cleanup_module, "_load_provider_config", unexpected_load)

    with pytest.raises(RuntimeError, match="Insufficient Lambda time"):
        cleanup_module.lambda_handler({}, FakeLambdaContext(60_000))


def test_lambda_handler_rejects_empty_provider_before_loading_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "  ")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def unexpected_load(_name: str, _provider: str) -> DaytonaProviderConfig:
        pytest.fail("secret must not be loaded")

    monkeypatch.setattr(cleanup_module, "_load_provider_config", unexpected_load)

    with pytest.raises(RuntimeError, match="SANDBOX_CLEANUP_PROVIDER must not be empty"):
        cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))


def test_lambda_handler_rechecks_time_after_loading_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DaytonaProviderConfig(
        DAYTONA_API_KEY="test-key",
        DAYTONA_API_URL="https://daytona.example.test/api",
        DAYTONA_TARGET=SCOPE,
    )
    context = FakeLambdaContext(840_000)
    cleanup_called = False

    def fake_load_provider_config(_name: str, _provider: str) -> DaytonaProviderConfig:
        context.remaining_milliseconds = 60_000
        return config

    async def unexpected_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        nonlocal cleanup_called
        del dry_run
        cleanup_called = True
        return _report()

    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
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
        DAYTONA_TARGET=SCOPE,
    )

    async def blocked_cleanup(_provider_config: DaytonaProviderConfig, *, dry_run: bool) -> CleanupReport:
        del dry_run
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)

    def fake_load_provider_config(_name: str, _provider: str) -> DaytonaProviderConfig:
        return config

    monkeypatch.setattr(cleanup_module, "_load_provider_config", fake_load_provider_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", blocked_cleanup)

    with pytest.raises(TimeoutError):
        cleanup_module.lambda_handler({}, FakeLambdaContext(60_010))
