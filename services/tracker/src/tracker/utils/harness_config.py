"""Helpers that turn X-Harness-* request headers into a HarnessConfig."""

from fastapi import Depends, HTTPException, Request

from tracker import config
from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.runtime import LegacyRuntime, harness_config_for_runtime, managed_runtime
from tracker.types import (
    AWSCredentials,
    HarnessConfig,
)


def _parse_log_retention_policy(value: int | str | None, *, source: str) -> int:
    if value in (None, ""):
        return 30
    try:
        parsed = int(value)
    except (TypeError, ValueError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid log_retention_policy from {source}: must be an integer",
        ) from e
    if parsed <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid log_retention_policy from {source}: must be positive",
        )
    return parsed


_REQUIRED_HARNESS_HEADER_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_default_region", "s3_bucket")


def _parse_harness_headers(request: Request) -> dict[str, str]:
    prefix = "x-harness-"
    return {
        key[len(prefix) :].replace("-", "_"): value for key, value in request.headers.items() if key.startswith(prefix)
    }


def _build_harness_config(flat: dict[str, str]) -> HarnessConfig:
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id=flat["aws_access_key_id"],
            aws_secret_access_key=flat["aws_secret_access_key"],
            aws_default_region=flat["aws_default_region"],
            aws_session_token=flat.get("aws_session_token"),
        ),
        s3_bucket=flat["s3_bucket"],
        log_group=flat.get("log_group") or "",
        log_retention_policy=_parse_log_retention_policy(
            flat.get("log_retention_policy"),
            source="request headers",
        ),
        sandbox_provider_secret_name=flat.get("sandbox_provider_secret_name") or flat.get("daytona_secret_name") or "",
    )


def try_fetch_harness_config(request: Request) -> HarnessConfig | None:
    """HarnessConfig from X-Harness-* headers, or None when no harness headers are sent.

    Used by endpoints (e.g. /start-benchmark) that accept harness_config either from headers (web FE)
    or from the request body (CLI).
    """
    flat = _parse_harness_headers(request)
    if not flat:
        return None
    for key in _REQUIRED_HARNESS_HEADER_KEYS:
        if not flat.get(key):
            header_name = key.replace("_", "-")
            raise HTTPException(status_code=400, detail=f"Missing harness config header 'x-harness-{header_name}'")

    return _build_harness_config(flat)


def resolve_harness_config(
    request: Request,
    org: Org = Depends(get_current_org),
) -> HarnessConfig | None:
    legacy_config = try_fetch_harness_config(request)
    if legacy_config:
        return harness_config_for_runtime(LegacyRuntime(kind="legacy", harness_config=legacy_config))
    if config.AUTH_REQUIRED:
        return harness_config_for_runtime(managed_runtime(org))
    return None


def fetch_harness_config(
    request: Request,
    org: Org = Depends(get_current_org),
) -> HarnessConfig:
    """HarnessConfig from X-Harness-* headers; 400 naming the first missing required header."""
    if harness_config := resolve_harness_config(request, org):
        return harness_config

    raise HTTPException(status_code=400, detail="Missing harness config header 'x-harness-aws-access-key-id'")


def try_resolve_harness_config(
    request: Request,
    org: Org = Depends(get_current_org),
) -> HarnessConfig | None:
    return resolve_harness_config(request, org)
