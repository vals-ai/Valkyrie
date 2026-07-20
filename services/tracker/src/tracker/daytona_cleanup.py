"""Delete Daytona sandboxes older than 48 hours from the configured target."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast

import boto3
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

from tracker.logging import configure_logging, get_logger
from tracker.observability import retry_callback

logger = get_logger(__name__)

_CLEANUP_LABEL = "clean-up"
_CLEANUP_DISABLED = "false"
_MAX_SANDBOX_AGE = timedelta(hours=48)
_DELETE_TIMEOUT_SECONDS = 120
_LAMBDA_SHUTDOWN_MARGIN_SECONDS = 60
_REQUIRED_SECRET_FIELDS = ("DAYTONA_API_KEY", "DAYTONA_API_URL", "DAYTONA_TARGET")
_TRANSIENT_DAYTONA_READ_ERRORS = (DaytonaConnectionError, DaytonaRateLimitError, DaytonaTimeoutError)
_DAYTONA_READ_ATTEMPTS = 3
_DAYTONA_READ_WAIT = wait_fixed(2)
_DAYTONA_RATE_LIMIT_WAIT = wait_exponential(multiplier=1, min=1, max=30)
CandidateExclusion = Literal["exempted", "target_mismatch", "not_old", "invalid_metadata"]


class DaytonaListClient(Protocol):
    """Subset of ``AsyncDaytona`` needed for metadata-aware cleanup."""

    def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]: ...

    async def get(self, sandbox_id_or_name: str) -> AsyncSandbox: ...


class SandboxDeleteProvider(Protocol):
    """Existing sandbox-provider deletion operation used by the cleanup engine."""

    async def delete_sandbox(self, instance_id: str) -> None: ...


class SecretsManagerClient(Protocol):
    """Secrets Manager operation used by the Lambda handler."""

    def get_secret_value(self, *, SecretId: str) -> Mapping[str, object]: ...


class LambdaContext(Protocol):
    """Lambda context operation used to preserve a shutdown margin."""

    def get_remaining_time_in_millis(self) -> int: ...


@dataclass(frozen=True)
class CleanupFailure:
    sandbox_id: str
    sandbox_name: str
    error_type: str


@dataclass(frozen=True)
class CleanupReport:
    cutoff: datetime
    dry_run: bool
    scanned: int
    eligible: int
    deletion_completed: int
    exempted: int
    target_mismatch: int
    not_old: int
    invalid_metadata: int
    failures: tuple[CleanupFailure, ...]

    @property
    def succeeded(self) -> bool:
        return self.target_mismatch == 0 and self.invalid_metadata == 0 and not self.failures


class DaytonaCleanupError(RuntimeError):
    def __init__(self, report: CleanupReport):
        self.report = report
        super().__init__(
            "Daytona cleanup did not fully succeed: "
            f"target_mismatch={report.target_mismatch}, "
            f"invalid_metadata={report.invalid_metadata}, failures={len(report.failures)}"
        )


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


def _cleanup_is_disabled(labels: Mapping[str, str] | None) -> bool:
    value = labels.get(_CLEANUP_LABEL) if labels else None
    return value is not None and value.strip().casefold() == _CLEANUP_DISABLED


def _candidate_exclusion(sandbox: AsyncSandbox, *, target: str, cutoff: datetime) -> CandidateExclusion | None:
    if sandbox.target != target:
        return "target_mismatch"
    if _cleanup_is_disabled(sandbox.labels):
        return "exempted"

    created_at = _parse_created_at(sandbox.created_at)
    if created_at is None:
        return "invalid_metadata"
    if created_at >= cutoff:
        return "not_old"
    return None


def _log_exclusion(exclusion: CandidateExclusion, sandbox: AsyncSandbox) -> None:
    if exclusion == "target_mismatch":
        logger.error(
            "Skipping Daytona sandbox returned outside the configured target",
            extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
        )
    elif exclusion == "invalid_metadata":
        logger.error(
            "Skipping Daytona sandbox with invalid creation timestamp",
            extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
        )


async def cleanup_old_sandboxes(
    client: DaytonaListClient,
    delete_provider: SandboxDeleteProvider,
    *,
    now: datetime,
    target: str,
    dry_run: bool = True,
) -> CleanupReport:
    """Delete target sandboxes strictly older than 48 hours unless they opt out."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    cutoff = now.astimezone(UTC) - _MAX_SANDBOX_AGE
    scanned = eligible = deletion_completed = 0
    exclusions: dict[CandidateExclusion, int] = {
        "exempted": 0,
        "target_mismatch": 0,
        "not_old": 0,
        "invalid_metadata": 0,
    }
    failures: list[CleanupFailure] = []

    query = ListSandboxesQuery(targets=[target], created_at_before=cutoff, limit=200)
    sandboxes = await _list_sandboxes(client, query)
    for sandbox in sandboxes:
        scanned += 1
        exclusion = _candidate_exclusion(sandbox, target=target, cutoff=cutoff)
        if exclusion is not None:
            exclusions[exclusion] += 1
            _log_exclusion(exclusion, sandbox)
            continue

        eligible += 1
        if dry_run:
            logger.info(
                "Daytona sandbox is eligible for cleanup (dry run)",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue

        try:
            current_sandbox = await _get_sandbox(client, sandbox.id)
        except DaytonaNotFoundError:
            deletion_completed += 1
            logger.info(
                "Daytona sandbox was already absent before cleanup deletion",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue
        except Exception as exc:
            failures.append(
                CleanupFailure(
                    sandbox_id=sandbox.id,
                    sandbox_name=sandbox.name,
                    error_type=type(exc).__name__,
                )
            )
            logger.error(
                "Failed to refresh Daytona sandbox before deletion",
                extra={
                    "sandbox_id": sandbox.id,
                    "sandbox_name": sandbox.name,
                    "error_type": type(exc).__name__,
                },
            )
            continue

        exclusion = _candidate_exclusion(current_sandbox, target=target, cutoff=cutoff)
        if exclusion is not None:
            eligible -= 1
            exclusions[exclusion] += 1
            _log_exclusion(exclusion, current_sandbox)
            continue

        try:
            delete_operation = (
                _delete_paused_sandbox(current_sandbox)
                if current_sandbox.state == SandboxState.PAUSED
                else delete_provider.delete_sandbox(sandbox.id)
            )
            await asyncio.wait_for(
                delete_operation,
                timeout=_DELETE_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            failures.append(
                CleanupFailure(
                    sandbox_id=sandbox.id,
                    sandbox_name=sandbox.name,
                    error_type=type(exc).__name__,
                )
            )
            logger.error(
                "Failed to delete Daytona sandbox",
                extra={
                    "sandbox_id": sandbox.id,
                    "sandbox_name": sandbox.name,
                    "error_type": type(exc).__name__,
                },
            )
            continue

        deletion_completed += 1
        logger.info(
            "Completed deletion of old Daytona sandbox",
            extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
        )

    return CleanupReport(
        cutoff=cutoff,
        dry_run=dry_run,
        scanned=scanned,
        eligible=eligible,
        deletion_completed=deletion_completed,
        exempted=exclusions["exempted"],
        target_mismatch=exclusions["target_mismatch"],
        not_old=exclusions["not_old"],
        invalid_metadata=exclusions["invalid_metadata"],
        failures=tuple(failures),
    )


async def run_cleanup(
    provider_config: DaytonaProviderConfig,
    *,
    now: datetime | None = None,
    dry_run: bool = True,
) -> CleanupReport:
    """Build Daytona clients and perform one cleanup sweep."""
    daytona_config = DaytonaConfig(
        api_key=provider_config.DAYTONA_API_KEY,
        api_url=provider_config.DAYTONA_API_URL,
        target=provider_config.DAYTONA_TARGET,
    )
    async with (
        AsyncDaytona(config=daytona_config) as client,
        provider_config.create_provider() as delete_provider,
    ):
        return await cleanup_old_sandboxes(
            client,
            delete_provider,
            now=now or datetime.now(UTC),
            target=provider_config.DAYTONA_TARGET,
            dry_run=dry_run,
        )


def _load_provider_config(secret_name: str) -> DaytonaProviderConfig:
    secrets = cast(
        SecretsManagerClient,
        boto3.client("secretsmanager"),  # pyright: ignore[reportUnknownMemberType]
    )
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        raise RuntimeError("Daytona cleanup secret must contain a JSON SecretString")

    try:
        parsed: object = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Daytona cleanup secret must contain valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Daytona cleanup secret must contain a JSON object")
    payload = cast(dict[str, object], parsed)

    values: dict[str, str] = {}
    for field in _REQUIRED_SECRET_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Daytona cleanup secret is missing required field {field}")
        values[field] = value

    return DaytonaProviderConfig(
        DAYTONA_API_KEY=values["DAYTONA_API_KEY"],
        DAYTONA_API_URL=values["DAYTONA_API_URL"],
        DAYTONA_TARGET=values["DAYTONA_TARGET"],
    )


def _report_fields(report: CleanupReport) -> dict[str, object]:
    return {
        "dry_run": report.dry_run,
        "cutoff": report.cutoff.isoformat(),
        "scanned": report.scanned,
        "eligible": report.eligible,
        "deletion_completed": report.deletion_completed,
        "exempted": report.exempted,
        "target_mismatch": report.target_mismatch,
        "not_old": report.not_old,
        "invalid_metadata": report.invalid_metadata,
        "failures": len(report.failures),
    }


def _remaining_cleanup_seconds(context: LambdaContext) -> float:
    timeout_seconds = context.get_remaining_time_in_millis() / 1000 - _LAMBDA_SHUTDOWN_MARGIN_SECONDS
    if timeout_seconds <= 0:
        raise RuntimeError("Insufficient Lambda time remaining for Daytona cleanup")
    return timeout_seconds


def lambda_handler(_event: object, context: LambdaContext) -> dict[str, object]:
    """Run one bounded cleanup sweep from EventBridge Scheduler."""
    configure_logging()
    _remaining_cleanup_seconds(context)
    provider_config = _load_provider_config(os.environ["DAYTONA_CLEANUP_SECRET_NAME"])
    timeout_seconds = _remaining_cleanup_seconds(context)
    dry_run = os.environ.get("DAYTONA_CLEANUP_DRY_RUN") != "false"
    report = asyncio.run(
        asyncio.wait_for(
            run_cleanup(provider_config, dry_run=dry_run),
            timeout=timeout_seconds,
        )
    )
    fields = _report_fields(report)
    logger.info("Daytona cleanup sweep complete", extra=fields)
    if not report.succeeded:
        raise DaytonaCleanupError(report)
    return fields
