"""Resolve deployment-managed AWS authority."""

from collections import OrderedDict
from time import monotonic
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
from tracker.types import StartBenchmarkRequest

_HARNESS_HEADER_PREFIX = "x-harness-"

_MANAGED_STORAGE_VALIDATION_CACHE_LIMIT = 512
_ManagedStorageValidationKey = tuple[UUID, str, str, str, str]
_managed_storage_validations: "OrderedDict[_ManagedStorageValidationKey, float]" = OrderedDict()


class ManagedAWSError(ValueError):
    """Base error for deployment-managed AWS resolution."""


class ManagedAWSEligibilityError(ManagedAWSError):
    """The organization is not allowed to use deployment AWS authority."""


class ManagedAWSConfigurationError(ManagedAWSError):
    """The deployment's managed AWS configuration is invalid."""


def _reject_harness_headers(request: Request) -> None:
    """Require application requests to use server-owned AWS authority."""
    if any(key.startswith(_HARNESS_HEADER_PREFIX) for key in request.headers):
        raise HTTPException(
            status_code=400,
            detail=(
                "AWS request headers are not accepted; runs resolve AWS resources from the deployment configuration."
            ),
        )


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


def _deployment_sandbox_provider() -> tuple[str, str]:
    """Return the deployment's default sandbox provider and its secret name."""
    if not config.AWS_DEPLOYMENT_SANDBOX_PROVIDER or not config.AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME:
        raise ManagedAWSConfigurationError(
            "Managed AWS configuration is missing AWS_DEPLOYMENT_SANDBOX_PROVIDER or "
            "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME"
        )
    return config.AWS_DEPLOYMENT_SANDBOX_PROVIDER, config.AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME


def resolve_managed_sandbox_provider(request: StartBenchmarkRequest) -> StartBenchmarkRequest:
    """Fill deployment sandbox-provider defaults omitted by a managed submission."""
    if request.sandbox_provider_secret_name:
        return request
    try:
        provider, secret_name = _deployment_sandbox_provider()
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if request.sandbox_provider and request.sandbox_provider != provider:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Managed execution has no configured secret for sandbox provider "
                f"'{request.sandbox_provider}' (deployment default: '{provider}'). "
                "Provide a provider secret name or use the deployment default provider."
            ),
        )
    return request.model_copy(update={"sandbox_provider": provider, "sandbox_provider_secret_name": secret_name})


def deployment_aws_runtime(org_id: UUID, properties: AWSResources | None = None) -> AWSRuntime:
    """Build a default-chain runtime for an eligible organization."""
    if not organization_can_use_managed_aws(org_id):
        raise ManagedAWSEligibilityError(
            "Managed AWS access is not available for this organization. "
            "Contact Vals support to enable managed AWS for this organization."
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


def _deployment_runtime_with_submission_properties(org_id: UUID, properties: AWSResources | None) -> AWSRuntime:
    """Build the deployment runtime, rejecting properties that differ from it."""
    runtime = _http_deployment_runtime(org_id)
    if properties is not None and properties != runtime.resources:
        raise HTTPException(status_code=400, detail="Managed run properties must match the deployment AWS resources")
    return runtime.with_resources(properties) if properties is not None else runtime


def resolve_start_aws_runtime(
    request: Request,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntime:
    """Resolve deployment AWS authority for a new run."""
    _reject_harness_headers(request)
    if not config.AWS_MANAGED_SUBMISSIONS_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Managed AWS submissions are temporarily unavailable. Try again later or contact Vals support.",
        )

    return _deployment_runtime_with_submission_properties(org_id, properties)


def resolve_run_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntime:
    """Resolve deployment AWS authority for an existing run."""
    _reject_harness_headers(request)
    if not aws_managed:
        raise HTTPException(
            status_code=400,
            detail="Run has no deployment-managed AWS runtime. Start a new run.",
        )
    return _http_deployment_runtime(org_id, properties)


def resolve_run_metadata_aws_runtime(
    request: Request,
    *,
    aws_managed: bool,
    org_id: UUID,
    properties: AWSResources | None = None,
) -> AWSRuntime | None:
    """Resolve deployment AWS authority for metadata links on an existing run."""
    if not aws_managed:
        return None
    _reject_harness_headers(request)
    return _http_deployment_runtime(org_id, properties)


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
    """Resolve agent-library operations to deployment AWS authority."""
    _reject_harness_headers(request)
    return _http_deployment_runtime(org_id)


def resolve_aws_runtime_metadata(org_id: UUID) -> AWSResources | None:
    """Return deployment resources when managed submissions are available to the organization."""
    try:
        if not config.AWS_MANAGED_SUBMISSIONS_ENABLED or not organization_can_use_managed_aws(org_id):
            return None
        return _managed_resources()
    except ManagedAWSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
