"""Select request-provided or deployment-managed AWS authority."""

from dataclasses import dataclass
from typing import Never
from uuid import UUID

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
    """Resolved AWS runtime and any access-key configuration used to build it."""

    runtime: AWSRuntime
    access_key_harness_config: HarnessConfig | None

    @property
    def aws_managed(self) -> bool:
        """Return whether deployment-managed AWS authority was selected."""
        return self.access_key_harness_config is None


@dataclass(frozen=True)
class HarnessHeaderInspection:
    """Presence and completeness of access-key request headers."""

    present: bool
    config: HarnessConfig | None
    first_missing_key: str | None


def parse_log_retention_policy(value: int | str | None, *, source: str) -> int:
    """Parse a positive log-retention value, defaulting to 30 days."""
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
    """Normalize access-key request headers into field names."""
    prefix = "x-harness-"
    return {
        key[len(prefix) :].replace("-", "_"): value for key, value in request.headers.items() if key.startswith(prefix)
    }


def _build_harness_config(flat: dict[str, str]) -> HarnessConfig:
    """Build a harness config from complete normalized headers."""
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


def inspect_harness_headers(request: Request) -> HarnessHeaderInspection:
    """Inspect access-key headers without treating their absence as an error."""
    flat = _parse_harness_headers(request)
    first_missing_key = next((key for key in _REQUIRED_HARNESS_HEADER_KEYS if not flat.get(key)), None)
    return HarnessHeaderInspection(
        present=bool(flat),
        config=_build_harness_config(flat) if first_missing_key is None else None,
        first_missing_key=first_missing_key,
    )


def _raise_missing_header(key: str) -> Never:
    """Raise a client error naming a missing access-key header."""
    header_name = key.replace("_", "-")
    raise HTTPException(status_code=400, detail=f"Missing harness config header 'x-harness-{header_name}'")


def try_fetch_harness_config(request: Request) -> HarnessConfig | None:
    """Return complete access-key request headers, if supplied."""
    return inspect_harness_headers(request).config


def fetch_harness_config(request: Request) -> HarnessConfig:
    """Return complete access-key request headers or name the first missing header."""
    header_inspection = inspect_harness_headers(request)
    if header_inspection.config is not None:
        return header_inspection.config
    assert header_inspection.first_missing_key is not None
    _raise_missing_header(header_inspection.first_missing_key)


def _eligible_org_ids() -> frozenset[UUID]:
    """Parse organizations allowed to use deployment AWS authority."""
    try:
        return frozenset(
            UUID(value.strip()) for value in config.AWS_DEPLOYMENT_ROLE_ORG_IDS.split(",") if value.strip()
        )
    except ValueError as exc:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_ROLE_ORG_IDS contains an invalid organization ID") from exc


def _managed_resources() -> AWSResources:
    """Build non-secret AWS resources from deployment configuration."""
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


def organization_can_use_managed_aws(org_id: UUID) -> bool:
    """Return whether an organization may use deployment AWS authority."""
    return org_id in _eligible_org_ids()


def deployment_aws_runtime(org_id: UUID) -> AWSRuntime:
    """Build a default-chain runtime for an eligible organization."""
    if not organization_can_use_managed_aws(org_id):
        raise ManagedAWSEligibilityError(
            "Managed AWS access is not available for this organization. Configure AWS access keys and try again."
        )
    resources = _managed_resources()
    return AWSRuntime(
        resources=resources,
        clients=DefaultChainAWSClientProvider(resources.region),
    )


def _http_deployment_runtime(org_id: UUID) -> AWSRuntime:
    """Translate managed-runtime configuration failures into HTTP errors."""
    try:
        return deployment_aws_runtime(org_id)
    except ManagedAWSEligibilityError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def resolve_run_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
) -> AWSRuntime:
    """Resolve AWS authority from a persisted run mode."""
    return resolve_run_aws_runtime_and_access_key_config(
        request,
        aws_managed=aws_managed,
        org_id=org_id,
    ).runtime


def resolve_run_aws_runtime_and_access_key_config(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
) -> AWSRuntimeResolution:
    """Resolve AWS authority and retain any access-key harness configuration."""
    if aws_managed:
        return AWSRuntimeResolution(_http_deployment_runtime(org_id), None)
    harness_config = fetch_harness_config(request)
    return AWSRuntimeResolution(AWSRuntime.from_harness_config(harness_config), harness_config)


def resolve_run_metadata_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
) -> AWSRuntime | None:
    """Resolve AWS authority when access-key metadata links may be omitted."""
    if aws_managed:
        return _http_deployment_runtime(org_id)
    harness_config = try_fetch_harness_config(request)
    return AWSRuntime.from_harness_config(harness_config) if harness_config is not None else None


def resolve_agent_library_aws_runtime(
    request: Request,
    org_id: UUID,
) -> AWSRuntime:
    """Resolve agent-library operations from complete headers or managed eligibility."""
    header_state = inspect_harness_headers(request)
    if header_state.config is not None:
        return AWSRuntime.from_harness_config(header_state.config)
    if header_state.first_missing_key is not None and header_state.present:
        _raise_missing_header(header_state.first_missing_key)
    return _http_deployment_runtime(org_id)


def resolve_aws_runtime_metadata(org_id: UUID) -> AWSResources | None:
    """Return non-secret deployment resource locations for an eligible organization."""
    try:
        if not organization_can_use_managed_aws(org_id):
            return None
        return _managed_resources()
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
