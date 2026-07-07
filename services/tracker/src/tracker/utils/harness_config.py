"""Helpers that turn X-Harness-* request headers into a HarnessConfig."""

from fastapi import HTTPException, Request

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
    """HarnessConfig from X-Harness-* headers, or None if any required header is missing.

    Used by endpoints (e.g. /start-benchmark) that accept harness_config either from headers (web FE)
    or from the request body (CLI).
    """
    flat = _parse_harness_headers(request)
    if any(not flat.get(key) for key in _REQUIRED_HARNESS_HEADER_KEYS):
        return None

    return _build_harness_config(flat)


def fetch_harness_config(request: Request) -> HarnessConfig:
    """HarnessConfig from X-Harness-* headers; 400 naming the first missing required header."""
    flat = _parse_harness_headers(request)
    for key in _REQUIRED_HARNESS_HEADER_KEYS:
        if not flat.get(key):
            header_name = key.replace("_", "-")
            raise HTTPException(status_code=400, detail=f"Missing harness config header 'x-harness-{header_name}'")

    return _build_harness_config(flat)
