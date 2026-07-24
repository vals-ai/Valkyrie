"""Select legacy or deployment AWS authority for tracker operations."""

from dataclasses import dataclass
from typing import Never

from fastapi import HTTPException, Request

from tracker import config
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials, HarnessConfig

_REQUIRED_HARNESS_HEADER_KEYS = (
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_default_region",
    "s3_bucket",
)


class ManagedAWSError(ValueError):
    """Base error for deployment-managed AWS resolution."""


class ManagedAWSEligibilityError(ManagedAWSError):
    """The organization is not allowed to use deployment AWS authority."""


class ManagedAWSConfigurationError(ManagedAWSError):
    """The deployment's managed AWS configuration is invalid."""


@dataclass(frozen=True)
class AWSRuntimeResolution:
    runtime: AWSRuntime
    legacy_harness_config: HarnessConfig | None

    @property
    def aws_managed(self) -> bool:
        """Return whether deployment-managed AWS authority was selected."""
        return self.legacy_harness_config is None


@dataclass(frozen=True)
class HarnessHeaderState:
    """Presence and completeness of legacy AWS request headers."""

    present: bool
    config: HarnessConfig | None
    first_missing_key: str | None


def parse_log_retention_policy(value: int | str | None, *, source: str) -> int:
    if value in (None, ""):
        return 30
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid log_retention_policy from {source}: must be an integer",
        ) from exc
    if parsed <= 0:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid log_retention_policy from {source}: must be positive",
        )
    return parsed


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
        log_retention_policy=parse_log_retention_policy(
            flat.get("log_retention_policy"),
            source="request headers",
        ),
        sandbox_provider_secret_name=flat.get("sandbox_provider_secret_name") or flat.get("daytona_secret_name") or "",
    )


def inspect_harness_headers(request: Request) -> HarnessHeaderState:
    """Inspect legacy headers without treating their absence as an error."""
    flat = _parse_harness_headers(request)
    first_missing_key = next((key for key in _REQUIRED_HARNESS_HEADER_KEYS if not flat.get(key)), None)
    return HarnessHeaderState(
        present=bool(flat),
        config=_build_harness_config(flat) if first_missing_key is None else None,
        first_missing_key=first_missing_key,
    )


def _raise_missing_header(key: str) -> Never:
    header_name = key.replace("_", "-")
    raise HTTPException(status_code=400, detail=f"Missing harness config header 'x-harness-{header_name}'")


def try_fetch_harness_config(request: Request) -> HarnessConfig | None:
    """Return complete legacy request headers, if supplied."""
    return inspect_harness_headers(request).config


def fetch_harness_config(request: Request) -> HarnessConfig:
    """Return complete legacy request headers or name the first missing header."""
    state = inspect_harness_headers(request)
    if state.config is not None:
        return state.config
    assert state.first_missing_key is not None
    _raise_missing_header(state.first_missing_key)


def resolve_start_harness_config(request: Request, body_config: HarnessConfig | None) -> HarnessConfig | None:
    """Apply legacy header-over-body precedence for a start request."""
    state = inspect_harness_headers(request)
    if state.config is not None:
        return state.config
    if body_config is not None:
        return body_config
    if state.present:
        assert state.first_missing_key is not None
        _raise_missing_header(state.first_missing_key)
    return None


def _managed_resources() -> AWSResources:
    missing = [
        name
        for name, value in (
            ("AWS_DEPLOYMENT_REGION", config.AWS_DEPLOYMENT_REGION),
            ("AWS_DEPLOYMENT_S3_BUCKET", config.AWS_DEPLOYMENT_S3_BUCKET),
            ("AWS_DEPLOYMENT_LOG_GROUP", config.AWS_DEPLOYMENT_LOG_GROUP),
            ("AWS_DEPLOYMENT_LOG_RETENTION_DAYS", config.AWS_DEPLOYMENT_LOG_RETENTION_DAYS),
        )
        if not value
    ]
    if missing:
        raise ManagedAWSConfigurationError(f"Managed AWS configuration is missing {', '.join(missing)}")

    try:
        retention_days = int(config.AWS_DEPLOYMENT_LOG_RETENTION_DAYS or "")
    except ValueError as exc:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_LOG_RETENTION_DAYS must be an integer") from exc
    if retention_days <= 0:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_LOG_RETENTION_DAYS must be positive")

    assert config.AWS_DEPLOYMENT_REGION is not None
    assert config.AWS_DEPLOYMENT_S3_BUCKET is not None
    assert config.AWS_DEPLOYMENT_LOG_GROUP is not None
    return AWSResources(
        region=config.AWS_DEPLOYMENT_REGION,
        s3_bucket=config.AWS_DEPLOYMENT_S3_BUCKET,
        log_group=config.AWS_DEPLOYMENT_LOG_GROUP,
        log_retention_days=retention_days,
    )


def organization_can_use_managed_aws(tenant_id: str) -> bool:
    return tenant_id in config.managed_tenant_ids()


def deployment_aws_runtime(tenant_id: str) -> AWSRuntime:
    if not organization_can_use_managed_aws(tenant_id):
        raise ManagedAWSEligibilityError(
            "Managed AWS access is not available for this organization. Configure AWS access keys and try again."
        )
    resources = _managed_resources()
    return AWSRuntime(
        resources=resources,
        clients=DefaultChainAWSClientProvider(resources.region),
    )


def _http_deployment_runtime(tenant_id: str) -> AWSRuntime:
    try:
        return deployment_aws_runtime(tenant_id)
    except ManagedAWSEligibilityError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def resolve_start_aws_runtime(
    request: Request,
    body_config: HarnessConfig | None,
    tenant_id: str,
) -> AWSRuntimeResolution:
    """Resolve a new run without reinterpreting partial legacy input as managed."""
    harness_config = resolve_start_harness_config(request, body_config)
    if harness_config is not None:
        return AWSRuntimeResolution(AWSRuntime.from_harness_config(harness_config), harness_config)
    if not config.AWS_MANAGED_SUBMISSIONS_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Managed AWS submissions are temporarily unavailable. Configure AWS access keys and try again.",
        )
    return AWSRuntimeResolution(_http_deployment_runtime(tenant_id), None)


def resolve_run_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    tenant_id: str,
    legacy_harness_config: HarnessConfig | None = None,
) -> AWSRuntimeResolution:
    """Resolve AWS authority from a persisted run mode."""
    if aws_managed:
        return AWSRuntimeResolution(_http_deployment_runtime(tenant_id), None)
    harness_config = legacy_harness_config or fetch_harness_config(request)
    return AWSRuntimeResolution(AWSRuntime.from_harness_config(harness_config), harness_config)


def resolve_optional_run_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    tenant_id: str,
    legacy_harness_config: HarnessConfig | None = None,
) -> AWSRuntime | None:
    """Resolve AWS authority when legacy metadata links may be omitted."""
    if aws_managed:
        return _http_deployment_runtime(tenant_id)
    harness_config = legacy_harness_config or try_fetch_harness_config(request)
    return AWSRuntime.from_harness_config(harness_config) if harness_config is not None else None


def resolve_non_run_aws_runtime(
    request: Request,
    tenant_id: str,
    legacy_harness_config: HarnessConfig | None = None,
) -> AWSRuntime:
    """Resolve agent-library operations from complete headers or managed eligibility."""
    if legacy_harness_config is not None:
        return AWSRuntime.from_harness_config(legacy_harness_config)
    header_state = inspect_harness_headers(request)
    if header_state.config is not None:
        return AWSRuntime.from_harness_config(header_state.config)
    if header_state.first_missing_key is not None and header_state.present:
        _raise_missing_header(header_state.first_missing_key)
    return _http_deployment_runtime(tenant_id)


def resolve_aws_runtime_metadata(tenant_id: str) -> AWSResources | None:
    """Return non-secret deployment resource locations for an eligible organization."""
    try:
        if not config.AWS_MANAGED_SUBMISSIONS_ENABLED or not organization_can_use_managed_aws(tenant_id):
            return None
        if not config.AWS_DEPLOYMENT_SANDBOX_PROVIDER or not config.AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME:
            raise ManagedAWSConfigurationError("Managed sandbox configuration is unavailable")
        return _managed_resources()
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
