"""Delete Valkyrie-managed Daytona sandboxes older than 48 hours."""

from __future__ import annotations

import asyncio
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

_MAX_SANDBOX_AGE = timedelta(hours=48)
_PRODUCTION_ENVIRONMENT = "production"
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
    dry_run: bool = True,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> CleanupReport:
    """Delete owned sandboxes strictly older than 48 hours and return an audit summary."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    cutoff = now.astimezone(UTC) - _MAX_SANDBOX_AGE
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


async def run_cleanup(*, now: datetime | None = None) -> CleanupReport:
    """Build the production Daytona client and perform one cleanup sweep."""
    target = os.environ["DAYTONA_TARGET"]
    config = DaytonaConfig(
        api_key=os.environ["DAYTONA_API_KEY"],
        api_url=os.environ["DAYTONA_API_URL"],
        target=target,
    )
    async with AsyncDaytona(config=config) as client:
        return await cleanup_old_sandboxes(
            client,
            now=now or datetime.now(UTC),
            environment=_PRODUCTION_ENVIRONMENT,
            target=target,
            dry_run=os.environ["DAYTONA_CLEANUP_DRY_RUN"] != "false",
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
