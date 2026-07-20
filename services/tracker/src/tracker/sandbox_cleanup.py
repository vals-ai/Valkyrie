"""Delete stale sandboxes through a provider-neutral cleanup engine."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast

import boto3
from benchmark_service.sandbox import SandboxProviderConfig, sandbox_provider_config_from_mapping

from tracker.logging import configure_logging, get_logger

logger = get_logger(__name__)

_CLEANUP_LABEL = "clean-up"
_CLEANUP_DISABLED = "false"
_MAX_SANDBOX_AGE = timedelta(hours=48)
_DELETE_TIMEOUT_SECONDS = 120
_LAMBDA_SHUTDOWN_MARGIN_SECONDS = 60
CandidateExclusion = Literal["exempted", "scope_mismatch", "not_old", "invalid_metadata"]


@dataclass(frozen=True)
class CleanupCandidate:
    """Provider-neutral metadata required to make a cleanup decision."""

    id: str
    name: str
    created_at: datetime | None
    labels: Mapping[str, str]
    scope: str | None
    provider_data: object | None = field(default=None, repr=False, compare=False)


class SandboxCleanupBackend(Protocol):
    """Control-plane operations a sandbox provider must expose for cleanup."""

    @property
    def provider_name(self) -> str: ...

    @property
    def scope(self) -> str | None: ...

    async def list_candidates(self, created_before: datetime) -> list[CleanupCandidate]: ...

    async def refresh_candidate(self, sandbox_id: str) -> CleanupCandidate | None: ...

    async def delete_candidate(self, candidate: CleanupCandidate) -> None: ...


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
    provider: str
    cutoff: datetime
    dry_run: bool
    scanned: int
    eligible: int
    deletion_completed: int
    exempted: int
    scope_mismatch: int
    not_old: int
    invalid_metadata: int
    failures: tuple[CleanupFailure, ...]

    @property
    def succeeded(self) -> bool:
        return self.scope_mismatch == 0 and self.invalid_metadata == 0 and not self.failures


class SandboxCleanupError(RuntimeError):
    def __init__(self, report: CleanupReport):
        self.report = report
        super().__init__(
            "Sandbox cleanup did not fully succeed: "
            f"scope_mismatch={report.scope_mismatch}, "
            f"invalid_metadata={report.invalid_metadata}, failures={len(report.failures)}"
        )


def _cleanup_is_disabled(labels: Mapping[str, str]) -> bool:
    value = labels.get(_CLEANUP_LABEL)
    return value is not None and value.strip().casefold() == _CLEANUP_DISABLED


def _candidate_exclusion(
    candidate: CleanupCandidate,
    *,
    scope: str | None,
    cutoff: datetime,
) -> CandidateExclusion | None:
    if candidate.scope != scope:
        return "scope_mismatch"
    if _cleanup_is_disabled(candidate.labels):
        return "exempted"
    if candidate.created_at is None or candidate.created_at.tzinfo is None:
        return "invalid_metadata"
    if candidate.created_at.astimezone(UTC) >= cutoff:
        return "not_old"
    return None


def _log_exclusion(
    exclusion: CandidateExclusion,
    candidate: CleanupCandidate,
    *,
    provider: str,
) -> None:
    if exclusion == "scope_mismatch":
        logger.error(
            "Skipping sandbox returned outside the configured cleanup scope",
            extra={"provider": provider, "sandbox_id": candidate.id, "sandbox_name": candidate.name},
        )
    elif exclusion == "invalid_metadata":
        logger.error(
            "Skipping sandbox with invalid creation metadata",
            extra={"provider": provider, "sandbox_id": candidate.id, "sandbox_name": candidate.name},
        )


async def cleanup_old_sandboxes(
    backend: SandboxCleanupBackend,
    *,
    now: datetime,
    dry_run: bool = True,
) -> CleanupReport:
    """Delete in-scope sandboxes strictly older than 48 hours unless they opt out."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    cutoff = now.astimezone(UTC) - _MAX_SANDBOX_AGE
    scanned = eligible = deletion_completed = 0
    exclusions: dict[CandidateExclusion, int] = {
        "exempted": 0,
        "scope_mismatch": 0,
        "not_old": 0,
        "invalid_metadata": 0,
    }
    failures: list[CleanupFailure] = []

    # Providers must finish inventory before the first mutation so a pagination
    # retry can never cause already-deleted sandboxes to be listed again.
    candidates = await backend.list_candidates(cutoff)
    for candidate in candidates:
        scanned += 1
        exclusion = _candidate_exclusion(candidate, scope=backend.scope, cutoff=cutoff)
        if exclusion is not None:
            exclusions[exclusion] += 1
            _log_exclusion(exclusion, candidate, provider=backend.provider_name)
            continue

        eligible += 1
        if dry_run:
            logger.info(
                "Sandbox is eligible for cleanup (dry run)",
                extra={
                    "provider": backend.provider_name,
                    "sandbox_id": candidate.id,
                    "sandbox_name": candidate.name,
                },
            )
            continue

        try:
            current_candidate = await backend.refresh_candidate(candidate.id)
        except Exception as exc:
            failures.append(
                CleanupFailure(
                    sandbox_id=candidate.id,
                    sandbox_name=candidate.name,
                    error_type=type(exc).__name__,
                )
            )
            logger.error(
                "Failed to refresh sandbox before deletion",
                extra={
                    "provider": backend.provider_name,
                    "sandbox_id": candidate.id,
                    "sandbox_name": candidate.name,
                    "error_type": type(exc).__name__,
                },
            )
            continue

        if current_candidate is None:
            deletion_completed += 1
            logger.info(
                "Sandbox was already absent before cleanup deletion",
                extra={
                    "provider": backend.provider_name,
                    "sandbox_id": candidate.id,
                    "sandbox_name": candidate.name,
                },
            )
            continue

        if current_candidate.id != candidate.id:
            failures.append(
                CleanupFailure(
                    sandbox_id=candidate.id,
                    sandbox_name=candidate.name,
                    error_type="CandidateIdentityMismatch",
                )
            )
            logger.error(
                "Cleanup backend refreshed a different sandbox",
                extra={
                    "provider": backend.provider_name,
                    "sandbox_id": candidate.id,
                    "sandbox_name": candidate.name,
                    "refreshed_sandbox_id": current_candidate.id,
                },
            )
            continue

        exclusion = _candidate_exclusion(current_candidate, scope=backend.scope, cutoff=cutoff)
        if exclusion is not None:
            eligible -= 1
            exclusions[exclusion] += 1
            _log_exclusion(exclusion, current_candidate, provider=backend.provider_name)
            continue

        try:
            await asyncio.wait_for(
                backend.delete_candidate(current_candidate),
                timeout=_DELETE_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            failures.append(
                CleanupFailure(
                    sandbox_id=current_candidate.id,
                    sandbox_name=current_candidate.name,
                    error_type=type(exc).__name__,
                )
            )
            logger.error(
                "Failed to delete sandbox",
                extra={
                    "provider": backend.provider_name,
                    "sandbox_id": current_candidate.id,
                    "sandbox_name": current_candidate.name,
                    "error_type": type(exc).__name__,
                },
            )
            continue

        deletion_completed += 1
        logger.info(
            "Completed deletion of old sandbox",
            extra={
                "provider": backend.provider_name,
                "sandbox_id": current_candidate.id,
                "sandbox_name": current_candidate.name,
            },
        )

    return CleanupReport(
        provider=backend.provider_name,
        cutoff=cutoff,
        dry_run=dry_run,
        scanned=scanned,
        eligible=eligible,
        deletion_completed=deletion_completed,
        exempted=exclusions["exempted"],
        scope_mismatch=exclusions["scope_mismatch"],
        not_old=exclusions["not_old"],
        invalid_metadata=exclusions["invalid_metadata"],
        failures=tuple(failures),
    )


@asynccontextmanager
async def _cleanup_backend(provider_config: SandboxProviderConfig) -> AsyncGenerator[SandboxCleanupBackend]:
    if provider_config.type == "daytona":
        from tracker.daytona_cleanup import daytona_cleanup_backend

        async with daytona_cleanup_backend(provider_config) as backend:
            yield backend
        return

    raise RuntimeError(f"Sandbox cleanup does not support provider {provider_config.type!r}")


async def run_cleanup(
    provider_config: SandboxProviderConfig,
    *,
    now: datetime | None = None,
    dry_run: bool = True,
) -> CleanupReport:
    """Build the configured provider backend and perform one cleanup sweep."""
    async with _cleanup_backend(provider_config) as backend:
        return await cleanup_old_sandboxes(
            backend,
            now=now or datetime.now(UTC),
            dry_run=dry_run,
        )


def _load_provider_config(secret_name: str, provider_type: str) -> SandboxProviderConfig:
    secrets = cast(
        SecretsManagerClient,
        boto3.client("secretsmanager"),  # pyright: ignore[reportUnknownMemberType]
    )
    response = secrets.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        raise RuntimeError("Sandbox cleanup secret must contain a JSON SecretString")

    try:
        parsed: object = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Sandbox cleanup secret must contain valid JSON") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Sandbox cleanup secret must contain a JSON object")
    payload = cast(dict[str, object], parsed)

    try:
        return sandbox_provider_config_from_mapping({**payload, "type": provider_type})
    except (TypeError, ValueError):
        # Validation details may echo provider credentials, so expose only the
        # provider discriminator and keep the original exception unchained.
        raise RuntimeError(f"Sandbox cleanup secret is invalid for provider {provider_type!r}") from None


def _report_fields(report: CleanupReport) -> dict[str, object]:
    return {
        "provider": report.provider,
        "dry_run": report.dry_run,
        "cutoff": report.cutoff.isoformat(),
        "scanned": report.scanned,
        "eligible": report.eligible,
        "deletion_completed": report.deletion_completed,
        "exempted": report.exempted,
        "scope_mismatch": report.scope_mismatch,
        "not_old": report.not_old,
        "invalid_metadata": report.invalid_metadata,
        "failures": len(report.failures),
    }


def _remaining_cleanup_seconds(context: LambdaContext) -> float:
    timeout_seconds = context.get_remaining_time_in_millis() / 1000 - _LAMBDA_SHUTDOWN_MARGIN_SECONDS
    if timeout_seconds <= 0:
        raise RuntimeError("Insufficient Lambda time remaining for sandbox cleanup")
    return timeout_seconds


def lambda_handler(_event: object, context: LambdaContext) -> dict[str, object]:
    """Run one bounded cleanup sweep from EventBridge Scheduler."""
    configure_logging()
    _remaining_cleanup_seconds(context)

    provider_type = os.environ.get("SANDBOX_CLEANUP_PROVIDER", "daytona").strip().casefold()
    if not provider_type:
        raise RuntimeError("SANDBOX_CLEANUP_PROVIDER must not be empty")
    provider_config = _load_provider_config(os.environ["SANDBOX_CLEANUP_SECRET_NAME"], provider_type)

    timeout_seconds = _remaining_cleanup_seconds(context)
    dry_run = os.environ.get("SANDBOX_CLEANUP_DRY_RUN", "true").strip().casefold() != "false"
    report = asyncio.run(
        asyncio.wait_for(
            run_cleanup(provider_config, dry_run=dry_run),
            timeout=timeout_seconds,
        )
    )
    fields = _report_fields(report)
    logger.info("Sandbox cleanup sweep complete", extra=fields)
    if not report.succeeded:
        raise SandboxCleanupError(report)
    return fields
