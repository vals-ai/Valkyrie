"""Delete stale sandboxes through Create Benchmark Service providers."""

from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Protocol

from benchmark_service import (
    Sandbox,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxQuery,
)

from tracker.logging import configure_logging, get_logger
from tracker.sandbox import audit_sandbox_delete
from tracker.utils.resources import fetch_sandbox_provider_config

logger = get_logger(__name__)

_CLEANUP_LABEL = "clean-up"
_MAX_SANDBOX_AGE = timedelta(hours=48)
_OPERATION_TIMEOUT_SECONDS = 120
_LAMBDA_SHUTDOWN_MARGIN_SECONDS = 60
_OUTCOMES = (
    "deleted",
    "already_absent",
    "opted_out",
    "not_old",
    "invalid_metadata",
    "identity_mismatch",
    "refresh_failed",
    "delete_failed",
)
_FAILURE_OUTCOMES = (
    "invalid_metadata",
    "identity_mismatch",
    "refresh_failed",
    "delete_failed",
)


class LambdaContext(Protocol):
    """Lambda context operation used to preserve a shutdown margin."""

    def get_remaining_time_in_millis(self) -> int: ...


def _classify(sandbox: Sandbox, cutoff: datetime) -> str | None:
    labels = sandbox.labels
    created_at = sandbox.created_at
    if labels is None or created_at is None or created_at.utcoffset() is None:
        return "invalid_metadata"

    cleanup_label = labels.get(_CLEANUP_LABEL)
    if cleanup_label is not None and cleanup_label.strip().casefold() == "false":
        return "opted_out"
    if created_at.astimezone(UTC) >= cutoff:
        return "not_old"
    return None


def _record_classification(outcomes: Counter[str], sandbox: Sandbox, outcome: str) -> None:
    outcomes[outcome] += 1
    if outcome == "invalid_metadata":
        logger.error(
            "Skipping sandbox with invalid cleanup metadata",
            extra={"sandbox_id": sandbox.id, "sandbox_name": sandbox.name},
        )


async def cleanup_old_sandboxes(
    provider: SandboxProvider,
    *,
    now: datetime,
) -> Counter[str]:
    """Delete sandboxes strictly older than 48 hours unless they opt out."""
    if now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")

    cutoff = now.astimezone(UTC) - _MAX_SANDBOX_AGE
    query = SandboxQuery(labels={}, page_size=200, created_at_lte=cutoff)

    # Complete inventory before the first mutation so pagination cannot be
    # disturbed by deleting sandboxes from the same result set.
    sandboxes = [sandbox async for sandbox in provider.list_sandboxes(query)]
    outcomes: Counter[str] = Counter()

    for sandbox in sandboxes:
        classification = _classify(sandbox, cutoff)
        if classification is not None:
            _record_classification(outcomes, sandbox, classification)
            continue

        try:
            current = await asyncio.wait_for(
                provider.get_sandbox(sandbox.id),
                timeout=_OPERATION_TIMEOUT_SECONDS,
            )
        except SandboxNotFoundError:
            outcomes["already_absent"] += 1
            continue
        except Exception as exc:
            outcomes["refresh_failed"] += 1
            logger.error(
                "Failed to refresh sandbox before cleanup",
                extra={"sandbox_id": sandbox.id, "error_type": type(exc).__name__},
            )
            continue

        if current.id != sandbox.id:
            outcomes["identity_mismatch"] += 1
            logger.error(
                "Cleanup refresh returned a different sandbox",
                extra={"sandbox_id": sandbox.id, "refreshed_sandbox_id": current.id},
            )
            continue

        classification = _classify(current, cutoff)
        if classification is not None:
            _record_classification(outcomes, current, classification)
            continue

        try:
            await asyncio.wait_for(
                provider.delete_sandbox(current.id),
                timeout=_OPERATION_TIMEOUT_SECONDS,
            )
        except SandboxNotFoundError:
            audit_sandbox_delete(current, "orphan_cleanup", org_id=None, outcome="already_gone")
            outcomes["already_absent"] += 1
        except Exception as exc:
            audit_sandbox_delete(current, "orphan_cleanup", org_id=None, outcome="failed", error=type(exc).__name__)
            outcomes["delete_failed"] += 1
            logger.error(
                "Failed to delete sandbox",
                extra={"sandbox_id": current.id, "error_type": type(exc).__name__},
            )
        else:
            audit_sandbox_delete(current, "orphan_cleanup", org_id=None, outcome="deleted")
            outcomes["deleted"] += 1

    return outcomes


async def run_cleanup(
    provider_config: SandboxProviderConfig,
    *,
    now: datetime | None = None,
) -> Counter[str]:
    """Create the configured CBS provider and perform one cleanup sweep."""
    async with provider_config.create_provider() as provider:
        return await cleanup_old_sandboxes(provider, now=now or datetime.now(UTC))


def _load_provider_config(secret_name: str, provider_type: str) -> SandboxProviderConfig:
    try:
        return fetch_sandbox_provider_config(secret_name, None, provider_type)
    except (TypeError, ValueError):
        # Provider validation may echo credentials, so do not expose or chain it.
        raise RuntimeError(f"Sandbox cleanup secret is invalid for provider {provider_type!r}") from None


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
    secret_name = os.environ.get("SANDBOX_CLEANUP_SECRET_NAME", "").strip()
    if not secret_name:
        raise RuntimeError("SANDBOX_CLEANUP_SECRET_NAME must not be empty")

    provider_config = _load_provider_config(secret_name, provider_type)
    timeout_seconds = _remaining_cleanup_seconds(context)
    outcomes = asyncio.run(
        asyncio.wait_for(
            run_cleanup(provider_config),
            timeout=timeout_seconds,
        )
    )
    fields: dict[str, object] = {
        "provider": provider_type,
        "scanned": sum(outcomes.values()),
        **{outcome: outcomes[outcome] for outcome in _OUTCOMES},
    }
    logger.info("Sandbox cleanup sweep complete", extra=fields)

    failures = sum(outcomes[outcome] for outcome in _FAILURE_OUTCOMES)
    if failures:
        raise RuntimeError(f"Sandbox cleanup did not fully succeed: failures={failures}")
    return fields
