"""Delete Valkyrie-managed Daytona sandboxes older than 48 hours."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    DaytonaConfig,
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


class DaytonaListClient(Protocol):
    """Subset of ``AsyncDaytona`` needed for metadata-aware cleanup listing."""

    def list(self, query: ListSandboxesQuery | None = None) -> AsyncIterator[AsyncSandbox]: ...


class SandboxForceDeleteProvider(Protocol):
    """Core sandbox-provider deletion operation used by the cleanup engine."""

    async def force_delete_sandbox(self, instance_id: str) -> bool: ...


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


async def cleanup_old_sandboxes(
    client: DaytonaListClient,
    delete_provider: SandboxForceDeleteProvider,
    *,
    now: datetime,
    environment: str,
    target: str,
    dry_run: bool = True,
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
            was_deleted = await delete_provider.force_delete_sandbox(sandbox.id)
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
    provider_config = DaytonaProviderConfig(
        DAYTONA_API_KEY=os.environ["DAYTONA_API_KEY"],
        DAYTONA_API_URL=os.environ["DAYTONA_API_URL"],
        DAYTONA_TARGET=os.environ["DAYTONA_TARGET"],
    )
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
            environment=_PRODUCTION_ENVIRONMENT,
            target=provider_config.DAYTONA_TARGET,
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
