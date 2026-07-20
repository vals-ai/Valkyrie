"""Daytona control-plane adapter for the sandbox cleanup engine."""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol, cast

from benchmark_service.sandbox.daytona import DaytonaProviderConfig, daytona_retry_after_seconds
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    DaytonaConfig,
    DaytonaConnectionError,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
    ListSandboxesQuery,
    SandboxState,
)
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential, wait_fixed

from tracker.observability import retry_callback
from tracker.sandbox_cleanup import CleanupCandidate, SandboxCleanupBackend

_TRANSIENT_DAYTONA_READ_ERRORS = (DaytonaConnectionError, DaytonaRateLimitError, DaytonaTimeoutError)
_DAYTONA_READ_ATTEMPTS = 3
_DAYTONA_READ_WAIT = wait_fixed(2)
_DAYTONA_RATE_LIMIT_WAIT = wait_exponential(multiplier=1, min=1, max=30)


class DaytonaListClient(Protocol):
    """Subset of ``AsyncDaytona`` needed for metadata-aware cleanup."""

    def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]: ...

    async def get(self, sandbox_id_or_name: str) -> AsyncSandbox: ...


class SandboxDeleteProvider(Protocol):
    """Existing sandbox-provider deletion operation used by the Daytona adapter."""

    async def delete_sandbox(self, instance_id: str) -> None: ...


def _daytona_read_retry_wait(retry_state: RetryCallState) -> float:
    assert retry_state.outcome is not None
    exc = retry_state.outcome.exception()
    assert exc is not None

    if isinstance(exc, DaytonaRateLimitError):
        retry_after = daytona_retry_after_seconds(exc)
        if retry_after is not None:
            return retry_after
        return _DAYTONA_RATE_LIMIT_WAIT(retry_state)

    return _DAYTONA_READ_WAIT(retry_state)


def _is_transient_daytona_read_error(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_DAYTONA_READ_ERRORS):
        return True
    return (
        isinstance(exc, DaytonaError)
        and exc.status_code is not None
        and (exc.status_code in (408, 429) or exc.status_code >= 500)
    )


_DAYTONA_READ_RETRY = retry(
    retry=retry_if_exception(_is_transient_daytona_read_error),
    stop=stop_after_attempt(_DAYTONA_READ_ATTEMPTS),
    wait=_daytona_read_retry_wait,
    before_sleep=retry_callback("valkyrie.daytona.cleanup.read"),
    reraise=True,
)
_DAYTONA_DELETE_RETRY = retry(
    retry=retry_if_exception(_is_transient_daytona_read_error),
    stop=stop_after_attempt(_DAYTONA_READ_ATTEMPTS),
    wait=_daytona_read_retry_wait,
    before_sleep=retry_callback("valkyrie.daytona.cleanup.delete"),
    reraise=True,
)


@_DAYTONA_READ_RETRY
async def _list_sandboxes(client: DaytonaListClient, query: ListSandboxesQuery) -> list[AsyncSandbox]:
    return [sandbox async for sandbox in client.list(query)]


@_DAYTONA_READ_RETRY
async def _get_sandbox(client: DaytonaListClient, sandbox_id: str) -> AsyncSandbox:
    return await client.get(sandbox_id)


@_DAYTONA_DELETE_RETRY
async def _delete_paused_sandbox(sandbox: AsyncSandbox) -> None:
    try:
        await sandbox.delete()
    except DaytonaNotFoundError:
        return


def _parse_created_at(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = f"{value[:-1]}+00:00" if value.endswith(("Z", "z")) else value
    try:
        created_at = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if created_at.tzinfo is None:
        return None
    return created_at.astimezone(UTC)


class DaytonaCleanupBackend:
    """Metadata-aware cleanup operations backed by Daytona's control-plane SDK."""

    provider_name = "daytona"

    def __init__(self, client: DaytonaListClient, delete_provider: SandboxDeleteProvider, target: str) -> None:
        self._client = client
        self._delete_provider = delete_provider
        self.scope = target

    def _candidate(self, sandbox: AsyncSandbox) -> CleanupCandidate:
        labels = cast(Mapping[str, str], sandbox.labels or {})
        return CleanupCandidate(
            id=sandbox.id,
            name=sandbox.name,
            created_at=_parse_created_at(sandbox.created_at),
            labels=labels,
            scope=sandbox.target,
            provider_data=sandbox,
        )

    async def list_candidates(self, created_before: datetime) -> list[CleanupCandidate]:
        query = ListSandboxesQuery(targets=[self.scope], created_at_before=created_before, limit=200)
        return [self._candidate(sandbox) for sandbox in await _list_sandboxes(self._client, query)]

    async def refresh_candidate(self, sandbox_id: str) -> CleanupCandidate | None:
        try:
            sandbox = await _get_sandbox(self._client, sandbox_id)
        except DaytonaNotFoundError:
            return None
        return self._candidate(sandbox)

    async def delete_candidate(self, candidate: CleanupCandidate) -> None:
        sandbox = cast(AsyncSandbox, candidate.provider_data)
        if sandbox.state == SandboxState.PAUSED:
            await _delete_paused_sandbox(sandbox)
        else:
            await self._delete_provider.delete_sandbox(candidate.id)


@asynccontextmanager
async def daytona_cleanup_backend(provider_config: DaytonaProviderConfig) -> AsyncGenerator[SandboxCleanupBackend]:
    """Build and close Daytona clients for one cleanup sweep."""
    daytona_config = DaytonaConfig(
        api_key=provider_config.DAYTONA_API_KEY,
        api_url=provider_config.DAYTONA_API_URL,
        target=provider_config.DAYTONA_TARGET,
    )
    async with (
        AsyncDaytona(config=daytona_config) as client,
        provider_config.create_provider() as delete_provider,
    ):
        yield DaytonaCleanupBackend(client, delete_provider, provider_config.DAYTONA_TARGET)
