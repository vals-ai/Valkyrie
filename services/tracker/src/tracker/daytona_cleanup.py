"""Delete Valkyrie-managed Daytona sandboxes older than the configured cutoff."""

from __future__ import annotations

import asyncio
import math
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    DaytonaConflictError,
    DaytonaConnectionError,
    DaytonaConfig,
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaRateLimitError,
    DaytonaTimeoutError,
    ListSandboxesQuery,
)

from tracker.logging import configure_logging, get_logger
from tracker.sandbox_labels import (
    ENVIRONMENT_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALKYRIE,
    cleanup_is_disabled,
    cleanup_is_enabled,
    is_valkyrie_managed,
)

logger = get_logger(__name__)

DEFAULT_MAX_AGE = timedelta(hours=48)
_DELETE_RETRY_DELAYS_SECONDS = (1.0, 4.0)


class DaytonaCleanupClient(Protocol):
    """Subset of ``AsyncDaytona`` used by the cleanup engine."""

    def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]: ...

    async def delete(self, sandbox: AsyncSandbox, timeout: float = 60) -> None: ...


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
    deletion_requested: int
    already_absent: int
    exempted: int
    unmanaged: int
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


def _is_retryable_delete_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (DaytonaConflictError, DaytonaConnectionError, DaytonaRateLimitError, DaytonaTimeoutError),
    ):
        return True
    return isinstance(exc, DaytonaError) and exc.status_code is not None and exc.status_code >= 500


def _is_not_found_error(exc: DaytonaError) -> bool:
    return (
        isinstance(exc, DaytonaNotFoundError)
        or exc.status_code == 404
        or (exc.error_code or "").strip().casefold() == "not_found"
    )


async def _delete_with_retry(
    client: DaytonaCleanupClient,
    sandbox: AsyncSandbox,
    *,
    sleep: Callable[[float], Awaitable[None]],
) -> bool:
    """Delete a sandbox, returning false when another actor already removed it."""
    for attempt in range(len(_DELETE_RETRY_DELAYS_SECONDS) + 1):
        try:
            await client.delete(sandbox)
            return True
        except DaytonaError as exc:
            if _is_not_found_error(exc):
                return False
            if not _is_retryable_delete_error(exc) or attempt == len(_DELETE_RETRY_DELAYS_SECONDS):
                raise
            delay = _DELETE_RETRY_DELAYS_SECONDS[attempt]
            logger.warning(
                "Retrying Daytona sandbox deletion",
                extra={
                    "sandbox_id": sandbox.id,
                    "sandbox_name": sandbox.name,
                    "attempt": attempt + 1,
                    "delay_seconds": delay,
                    "error_type": type(exc).__name__,
                },
            )
            await sleep(delay)

    raise AssertionError("delete retry loop exhausted without returning or raising")


async def cleanup_old_sandboxes(
    client: DaytonaCleanupClient,
    *,
    now: datetime,
    environment: str,
    target: str,
    max_age: timedelta = DEFAULT_MAX_AGE,
    dry_run: bool = True,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> CleanupReport:
    """Delete owned sandboxes strictly older than ``max_age`` and return an audit summary."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if not environment:
        raise ValueError("environment must not be empty")
    if not target:
        raise ValueError("target must not be empty")
    if max_age <= timedelta(0):
        raise ValueError("max_age must be positive")

    cutoff = now.astimezone(UTC) - max_age
    scanned = eligible = deletion_requested = already_absent = 0
    exempted = unmanaged = target_mismatch = not_old = invalid_metadata = 0
    failures: list[CleanupFailure] = []

    query = ListSandboxesQuery(
        labels={MANAGED_BY_LABEL: MANAGED_BY_VALKYRIE, ENVIRONMENT_LABEL: environment},
        targets=[target],
        created_at_before=cutoff,
        limit=200,
    )
    sandboxes = [sandbox async for sandbox in client.list(query)]
    for sandbox in sandboxes:
        scanned += 1
        if sandbox.target != target:
            target_mismatch += 1
            logger.error(
                "Skipping Daytona sandbox returned outside the configured target",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue
        if not is_valkyrie_managed(sandbox.labels, environment=environment):
            unmanaged += 1
            continue
        if cleanup_is_disabled(sandbox.labels):
            exempted += 1
            continue
        if not cleanup_is_enabled(sandbox.labels):
            invalid_metadata += 1
            logger.error(
                "Skipping managed Daytona sandbox without an explicit cleanup label",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue

        created_at = _parse_created_at(sandbox.created_at)
        if created_at is None:
            invalid_metadata += 1
            logger.error(
                "Skipping managed Daytona sandbox with invalid creation timestamp",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue
        if created_at >= cutoff:
            not_old += 1
            continue

        eligible += 1
        if dry_run:
            logger.info(
                "Daytona sandbox is eligible for cleanup (dry run)",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
            continue

        try:
            was_deleted = await _delete_with_retry(client, sandbox, sleep=sleep)
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

        if was_deleted:
            deletion_requested += 1
            logger.info(
                "Requested deletion of old Daytona sandbox",
                extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
            )
        else:
            already_absent += 1

    return CleanupReport(
        cutoff=cutoff,
        dry_run=dry_run,
        scanned=scanned,
        eligible=eligible,
        deletion_requested=deletion_requested,
        already_absent=already_absent,
        exempted=exempted,
        unmanaged=unmanaged,
        target_mismatch=target_mismatch,
        not_old=not_old,
        invalid_metadata=invalid_metadata,
        failures=tuple(failures),
    )


def _boolean_environment(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().casefold()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{name} must be 'true' or 'false'")


def _max_age_environment() -> timedelta:
    raw_value = os.environ.get("DAYTONA_CLEANUP_MAX_AGE_HOURS", "48")
    try:
        hours = float(raw_value)
    except ValueError as exc:
        raise ValueError("DAYTONA_CLEANUP_MAX_AGE_HOURS must be a positive number") from exc
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError("DAYTONA_CLEANUP_MAX_AGE_HOURS must be a positive finite number")
    return timedelta(hours=hours)


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} must be set")
    return value


async def run_cleanup(*, now: datetime | None = None) -> CleanupReport:
    """Build the environment-configured Daytona client and perform one cleanup sweep."""
    dry_run = _boolean_environment("DAYTONA_CLEANUP_DRY_RUN", default=True)
    environment = _required_environment("ENVIRONMENT")
    api_key = _required_environment("DAYTONA_API_KEY")
    api_url = _required_environment("DAYTONA_API_URL")
    target = _required_environment("DAYTONA_TARGET")
    max_age = _max_age_environment()
    if environment == "production" and max_age != DEFAULT_MAX_AGE:
        raise ValueError("production Daytona cleanup age must remain exactly 48 hours")

    config = DaytonaConfig(api_key=api_key, api_url=api_url, target=target)
    async with AsyncDaytona(config=config) as client:
        return await cleanup_old_sandboxes(
            client,
            now=now or datetime.now(UTC),
            environment=environment,
            target=target,
            max_age=max_age,
            dry_run=dry_run,
        )


def main() -> None:
    """Run one cleanup sweep and exit non-zero when any managed sandbox could not be evaluated or removed."""
    # This process holds a Daytona client config in local variables. Keep exception reporting in
    # CloudWatch logs so traceback-local capture cannot serialize provider credentials.
    configure_logging()

    try:
        report = asyncio.run(run_cleanup())
        logger.info(
            "Daytona cleanup sweep complete",
            extra={
                "dry_run": report.dry_run,
                "cutoff": report.cutoff.isoformat(),
                "scanned": report.scanned,
                "eligible": report.eligible,
                "deletion_requested": report.deletion_requested,
                "already_absent": report.already_absent,
                "exempted": report.exempted,
                "unmanaged": report.unmanaged,
                "target_mismatch": report.target_mismatch,
                "not_old": report.not_old,
                "invalid_metadata": report.invalid_metadata,
                "failures": len(report.failures),
            },
        )
        if not report.succeeded:
            raise DaytonaCleanupError(report)
    except Exception:
        logger.exception("Daytona cleanup sweep failed")
        raise


if __name__ == "__main__":
    main()
