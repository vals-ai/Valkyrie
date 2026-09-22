"""Select request-provided or deployment-managed AWS authority."""

from collections import OrderedDict
from dataclasses import dataclass
from time import monotonic
from typing import Never
from uuid import UUID

from fastapi import HTTPException, Request

from tracker import config
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.managed_storage import (
    ManagedStorageError,
    load_managed_storage_policy,
    validate_managed_storage_bucket,
)
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.types import AWSCredentials, HarnessConfig

_REQUIRED_HARNESS_HEADER_KEYS = (
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_default_region",
    "s3_bucket",
)

_MANAGED_STORAGE_VALIDATION_CACHE_LIMIT = 512
_ManagedStorageValidationKey = tuple[UUID, str, str, str, str]
_managed_storage_validations: "OrderedDict[_ManagedStorageValidationKey, float]" = OrderedDict()


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

    def with_submission_properties(self, properties: AWSResources | None) -> "AWSRuntimeResolution":
        """Apply caller resources while keeping managed submissions on deployment resources."""
        if properties is None:
            return self
        if self.aws_managed and properties != self.runtime.resources:
            raise HTTPException(
                status_code=400, detail="Managed run properties must match the deployment AWS resources"
            )

        return AWSRuntimeResolution(self.runtime.with_resources(properties), self.access_key_harness_config)


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


def fetch_harness_config(request: Request) -> HarnessConfig:
    """Return complete access-key request headers or name the first missing header."""
    header_inspection = inspect_harness_headers(request)
    if header_inspection.config is not None:
        return header_inspection.config
    assert header_inspection.first_missing_key is not None
    _raise_missing_header(header_inspection.first_missing_key)


def resolve_start_harness_config(request: Request, body_config: HarnessConfig | None) -> HarnessConfig | None:
    """Apply access-key header-over-body precedence for a start request."""
    header_inspection = inspect_harness_headers(request)
    if header_inspection.config is not None:
        return header_inspection.config
    if body_config is not None:
        return body_config
    if header_inspection.present:
        assert header_inspection.first_missing_key is not None
        _raise_missing_header(header_inspection.first_missing_key)
    return None


def _eligible_org_ids() -> frozenset[UUID]:
    """Parse organizations allowed to use deployment AWS authority."""
    try:
        return frozenset(
            UUID(value.strip()) for value in config.AWS_DEPLOYMENT_ROLE_ORG_IDS.split(",") if value.strip()
        )
    except ValueError as exc:
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_ROLE_ORG_IDS contains an invalid organization ID") from exc


def _managed_resources(properties: AWSResources | None = None) -> AWSResources:
    """Use saved resources, or resolve and validate deployment defaults."""
    if properties is not None:
        return properties

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


def _deployment_account_id() -> str:
    """Return the trusted account that owns deployment-managed buckets."""
    account_id = config.AWS_DEPLOYMENT_ACCOUNT_ID
    if account_id is None or len(account_id) != 12 or not account_id.isascii() or not account_id.isdigit():
        raise ManagedAWSConfigurationError("AWS_DEPLOYMENT_ACCOUNT_ID must be a 12-digit AWS account ID")

    return account_id


def organization_can_use_managed_aws(org_id: UUID) -> bool:
    """Return whether an organization may use deployment AWS authority."""
    return org_id in _eligible_org_ids()


def deployment_aws_runtime(org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
    """Build a default-chain runtime for an eligible organization."""
    if not organization_can_use_managed_aws(org_id):
        raise ManagedAWSEligibilityError(
            "Managed AWS access is not available for this organization. Configure AWS access keys and try again."
        )
    resources = _managed_resources(properties)
    return AWSRuntime(
        resources=resources,
        clients=DefaultChainAWSClientProvider(resources.region),
        expected_bucket_owner=_deployment_account_id(),
    )


def _http_deployment_runtime(org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
    """Translate managed-runtime configuration failures into HTTP errors."""
    try:
        return deployment_aws_runtime(org_id, properties)
    except ManagedAWSEligibilityError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def resolve_start_aws_runtime(
    request: Request,
    body_config: HarnessConfig | None,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntimeResolution:
    """Resolve a new run without reinterpreting partial access-key input as managed."""
    harness_config = resolve_start_harness_config(request, body_config)
    if harness_config is not None:
        return AWSRuntimeResolution(
            AWSRuntime.from_harness_config(harness_config), harness_config
        ).with_submission_properties(properties)
    if not config.AWS_MANAGED_SUBMISSIONS_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Managed AWS submissions are temporarily unavailable. Configure AWS access keys and try again.",
        )

    return AWSRuntimeResolution(_http_deployment_runtime(org_id), None).with_submission_properties(properties)


def resolve_run_aws_runtime_and_access_key_config(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntimeResolution:
    """Resolve AWS authority and retain any access-key harness configuration."""
    if aws_managed:
        return AWSRuntimeResolution(_http_deployment_runtime(org_id, properties), None)

    header_inspection = inspect_harness_headers(request)
    if not header_inspection.present:
        raise HTTPException(
            status_code=400,
            detail="This run was started with access-key AWS and requires its legacy AWS configuration.",
        )
    if header_inspection.config is None:
        assert header_inspection.first_missing_key is not None
        _raise_missing_header(header_inspection.first_missing_key)

    harness_config = header_inspection.config
    return AWSRuntimeResolution(
        AWSRuntime.from_harness_config(harness_config).with_resources(properties), harness_config
    )


def resolve_run_metadata_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntime | None:
    """Resolve AWS authority when access-key metadata links may be omitted."""
    if aws_managed:
        return _http_deployment_runtime(org_id, properties)

    harness_config = inspect_harness_headers(request).config
    if harness_config is None:
        return None
    return AWSRuntime.from_harness_config(harness_config).with_resources(properties)


def reset_managed_storage_validation_cache() -> None:
    """Forget every remembered owner-bucket validation in this process."""
    _managed_storage_validations.clear()


def _managed_storage_validation_key(runtime: AWSRuntime, org_id: UUID) -> _ManagedStorageValidationKey:
    """Identify one owner-bucket validation by everything the validator inspects."""
    return (
        org_id,
        runtime.resources.s3_bucket,
        runtime.resources.region,
        runtime.expected_bucket_owner or "",
        runtime.clients.credential_source,
    )


def _managed_storage_validation_is_fresh(key: _ManagedStorageValidationKey, *, now: float) -> bool:
    """Return whether a previous validation of this bucket is still within its window."""
    expires_at = _managed_storage_validations.get(key)
    if expires_at is None:
        return False

    if expires_at <= now:
        del _managed_storage_validations[key]
        return False

    _managed_storage_validations.move_to_end(key)
    return True


def _remember_managed_storage_validation(key: _ManagedStorageValidationKey, *, now: float, ttl_seconds: int) -> None:
    """Record one successful validation and evict the least recently used entries."""
    _managed_storage_validations[key] = now + ttl_seconds
    _managed_storage_validations.move_to_end(key)
    while len(_managed_storage_validations) > _MANAGED_STORAGE_VALIDATION_CACHE_LIMIT:
        _ = _managed_storage_validations.popitem(last=False)


async def validate_saved_managed_storage_runtime(runtime: AWSRuntime, *, org_id: UUID) -> None:
    """Revalidate persisted owner storage before a managed read uses it."""
    if not runtime.resources.s3_bucket.startswith(("vs-dev-", "vs-prod-")):
        return

    ttl_seconds = config.AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS
    cache_key = _managed_storage_validation_key(runtime, org_id)
    now = monotonic()
    if ttl_seconds > 0 and _managed_storage_validation_is_fresh(cache_key, now=now):
        return

    policy = load_managed_storage_policy()
    await validate_managed_storage_bucket(
        runtime,
        org_id=org_id,
        bucket_name=runtime.resources.s3_bucket,
        policy=policy,
    )

    if ttl_seconds > 0:
        _remember_managed_storage_validation(cache_key, now=now, ttl_seconds=ttl_seconds)


async def http_validate_saved_managed_storage_runtime(runtime: AWSRuntime, *, org_id: UUID) -> None:
    """Translate saved owner-storage failures into HTTP errors for API routes."""
    try:
        await validate_saved_managed_storage_runtime(runtime, org_id=org_id)
    except ManagedStorageError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


def resolve_agent_library_aws_runtime(
    request: Request,
    org_id: UUID,
) -> AWSRuntime:
    """Resolve agent-library operations from complete headers or managed eligibility."""
    header_inspection = inspect_harness_headers(request)
    if header_inspection.config is not None:
        return AWSRuntime.from_harness_config(header_inspection.config)
    if header_inspection.first_missing_key is not None and header_inspection.present:
        _raise_missing_header(header_inspection.first_missing_key)
    return _http_deployment_runtime(org_id)


def resolve_aws_runtime_metadata(org_id: UUID) -> AWSResources | None:
    """Return deployment resources when managed submissions are available to the organization."""
    try:
        if not config.AWS_MANAGED_SUBMISSIONS_ENABLED or not organization_can_use_managed_aws(org_id):
            return None
        return _managed_resources()
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
