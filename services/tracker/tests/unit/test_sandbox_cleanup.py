"""Unit tests for Valkyrie's sandbox cleanup policy."""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncGenerator, Mapping
from datetime import UTC, datetime, timedelta
from types import MappingProxyType, SimpleNamespace
from typing import cast

import pytest
from benchmark_service import (
    Sandbox,
    SandboxCreateRequest,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxQuery,
)

import tracker.sandbox_cleanup as cleanup_module
from tracker.sandbox_cleanup import cleanup_old_sandboxes, run_cleanup

NOW = datetime(2026, 7, 9, 12, tzinfo=UTC)
CUTOFF = NOW - timedelta(hours=48)
EMPTY_LABELS: Mapping[str, str] = MappingProxyType({})


def _sandbox(
    sandbox_id: str,
    *,
    created_at: datetime | None = CUTOFF - timedelta(seconds=1),
    labels: Mapping[str, str] | None = EMPTY_LABELS,
) -> Sandbox:
    return cast(
        Sandbox,
        SimpleNamespace(
            id=sandbox_id,
            name=f"sandbox-{sandbox_id}",
            state="started",
            created_at=created_at,
            labels=labels,
        ),
    )


class FakeSandboxProvider(SandboxProvider):
    def __init__(
        self,
        sandboxes: list[Sandbox],
        *,
        current: Mapping[str, Sandbox | BaseException] | None = None,
        delete_effects: Mapping[str, BaseException] | None = None,
        list_error: BaseException | None = None,
    ) -> None:
        self.sandboxes = sandboxes
        self.current = dict(current) if current is not None else {sandbox.id: sandbox for sandbox in sandboxes}
        self.delete_effects = dict(delete_effects or {})
        self.list_error = list_error
        self.queries: list[SandboxQuery] = []
        self.get_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.inventory_complete = False
        self.closed = False

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        del request
        raise AssertionError("cleanup must not create sandboxes")

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        self.get_calls.append(instance_id)
        assert self.inventory_complete, "cleanup mutated before inventory completed"
        result = self.current[instance_id]
        if isinstance(result, BaseException):
            raise result
        return result

    async def delete_sandbox(self, instance_id: str) -> None:
        self.delete_calls.append(instance_id)
        assert self.inventory_complete, "cleanup mutated before inventory completed"
        effect = self.delete_effects.get(instance_id)
        if effect is not None:
            raise effect

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        self.queries.append(query)
        for sandbox in self.sandboxes:
            yield sandbox
        if self.list_error is not None:
            raise self.list_error
        self.inventory_complete = True

    async def close(self) -> None:
        self.closed = True


class FakeProviderConfig:
    def __init__(self, provider: SandboxProvider) -> None:
        self.provider = provider
        self.create_calls = 0

    def create_provider(self) -> SandboxProvider:
        self.create_calls += 1
        return self.provider


class FakeLambdaContext:
    def __init__(self, *remaining_milliseconds: int) -> None:
        self.remaining_milliseconds = list(remaining_milliseconds)

    def get_remaining_time_in_millis(self) -> int:
        if len(self.remaining_milliseconds) > 1:
            return self.remaining_milliseconds.pop(0)
        return self.remaining_milliseconds[0]


async def test_run_cleanup_materializes_inventory_before_mutation_and_closes_provider() -> None:
    provider = FakeSandboxProvider(
        [_sandbox("listed-before-pagination-failure")],
        list_error=RuntimeError("pagination failed"),
    )
    config = FakeProviderConfig(provider)

    with pytest.raises(RuntimeError, match="pagination failed"):
        await run_cleanup(cast(SandboxProviderConfig, config), now=NOW)

    assert provider.get_calls == []
    assert provider.delete_calls == []
    assert provider.closed
    assert config.create_calls == 1


async def test_cleanup_applies_strict_age_opt_out_and_metadata_rules() -> None:
    provider = FakeSandboxProvider(
        [
            _sandbox("old", created_at=CUTOFF - timedelta(microseconds=1)),
            _sandbox("at-cutoff", created_at=CUTOFF),
            _sandbox("opted-out", labels={"clean-up": " FaLsE\t"}),
            _sandbox("falsey", labels={"clean-up": "falsey"}),
            _sandbox("wrong-key", labels={"cleanup": "false"}),
            _sandbox("missing-labels", labels=None),
            _sandbox("missing-created-at", created_at=None),
            _sandbox("naive-created-at", created_at=CUTOFF.replace(tzinfo=None)),
        ]
    )

    outcomes = await cleanup_old_sandboxes(provider, now=NOW)

    assert provider.queries == [SandboxQuery(labels={}, page_size=200, created_at_lte=CUTOFF)]
    assert provider.get_calls == ["old", "falsey", "wrong-key"]
    assert provider.delete_calls == ["old", "falsey", "wrong-key"]
    assert outcomes == Counter({"deleted": 3, "invalid_metadata": 3, "not_old": 1, "opted_out": 1})


async def test_cleanup_reclassifies_refreshed_metadata_and_rejects_identity_change() -> None:
    listed = [
        _sandbox("newly-opted-out"),
        _sandbox("newly-at-cutoff"),
        _sandbox("metadata-lost"),
        _sandbox("identity-changed"),
    ]
    provider = FakeSandboxProvider(
        listed,
        current={
            "newly-opted-out": _sandbox("newly-opted-out", labels={"clean-up": "false"}),
            "newly-at-cutoff": _sandbox("newly-at-cutoff", created_at=CUTOFF),
            "metadata-lost": _sandbox("metadata-lost", created_at=None),
            "identity-changed": _sandbox("different-id"),
        },
    )

    outcomes = await cleanup_old_sandboxes(provider, now=NOW)

    assert provider.get_calls == [sandbox.id for sandbox in listed]
    assert provider.delete_calls == []
    assert outcomes == Counter(
        {
            "opted_out": 1,
            "not_old": 1,
            "invalid_metadata": 1,
            "identity_mismatch": 1,
        }
    )


async def test_cleanup_treats_not_found_as_complete_and_continues_after_item_failures() -> None:
    gone = _sandbox("gone")
    refresh_failed = _sandbox("refresh-failed")
    delete_failed = _sandbox("delete-failed")
    deleted_after_failures = _sandbox("deleted-after-failures")
    provider = FakeSandboxProvider(
        [gone, refresh_failed, delete_failed, deleted_after_failures],
        current={
            gone.id: SandboxNotFoundError("already absent"),
            refresh_failed.id: RuntimeError("refresh failed"),
            delete_failed.id: delete_failed,
            deleted_after_failures.id: deleted_after_failures,
        },
        delete_effects={delete_failed.id: RuntimeError("delete failed")},
    )

    outcomes = await cleanup_old_sandboxes(provider, now=NOW)

    assert provider.get_calls == [gone.id, refresh_failed.id, delete_failed.id, deleted_after_failures.id]
    assert provider.delete_calls == [delete_failed.id, deleted_after_failures.id]
    assert outcomes == Counter({"already_absent": 1, "refresh_failed": 1, "delete_failed": 1, "deleted": 1})


def test_lambda_handler_fails_for_each_unsuccessful_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    config = cast(SandboxProviderConfig, object())
    failure_outcome = "invalid_metadata"

    def fake_fetch_config(
        _secret_name: str,
        _aws: object | None,
        _provider_type: str,
    ) -> SandboxProviderConfig:
        return config

    async def fake_run_cleanup(_config: SandboxProviderConfig, *, now: datetime | None = None) -> Counter[str]:
        del now
        return Counter({failure_outcome: 1})

    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)
    monkeypatch.setattr(cleanup_module, "fetch_sandbox_provider_config", fake_fetch_config)
    monkeypatch.setattr(cleanup_module, "run_cleanup", fake_run_cleanup)

    for failure_outcome in ("invalid_metadata", "identity_mismatch", "refresh_failed", "delete_failed"):
        with pytest.raises(RuntimeError, match="Sandbox cleanup did not fully succeed"):
            cleanup_module.lambda_handler({}, FakeLambdaContext(840_000))


def test_lambda_handler_preserves_shutdown_margin_around_config_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleanup_module, "configure_logging", lambda: None)
    monkeypatch.setenv("SANDBOX_CLEANUP_SECRET_NAME", "cleanup-secret")
    monkeypatch.setenv("SANDBOX_CLEANUP_PROVIDER", "daytona")
    load_calls = 0

    def fake_fetch_config(*_args: object, **_kwargs: object) -> SandboxProviderConfig:
        nonlocal load_calls
        load_calls += 1
        return cast(SandboxProviderConfig, object())

    monkeypatch.setattr(cleanup_module, "fetch_sandbox_provider_config", fake_fetch_config)

    with pytest.raises(RuntimeError, match="Insufficient Lambda time"):
        cleanup_module.lambda_handler({}, FakeLambdaContext(60_000))
    assert load_calls == 0

    with pytest.raises(RuntimeError, match="Insufficient Lambda time"):
        cleanup_module.lambda_handler({}, FakeLambdaContext(840_000, 60_000))
    assert load_calls == 1
