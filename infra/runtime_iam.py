"""Managed AWS runtime environment and ECS task-role policies."""

from __future__ import annotations

import os
import re
from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_iam, aws_s3
from constructs import Construct

from stage import Stage
from stage_config import ManagedAWSRuntimeConfig

_S3_PREFIXES = ("agents/*", "benchmarks/*")
_SECRET_NAME = re.compile(r"[A-Za-z0-9/_+=.@-]{1,512}")
_TENANT_ID = re.compile(r"[^\s,]{1,256}")


def _environment_value(name: str, default: str) -> str:
    return os.environ.get(name) or default


def managed_runtime_environment(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> dict[str, str]:
    """Build the deployment-owned runtime configuration for an ECS container."""
    tenant_ids = tuple(
        tenant_id.strip()
        for tenant_id in _environment_value(
            "AWS_MANAGED_TENANT_IDS",
            ",".join(config.managed_tenant_ids),
        ).split(",")
        if tenant_id.strip()
    )
    submissions_enabled = _environment_value(
        "AWS_MANAGED_SUBMISSIONS_ENABLED",
        str(config.submissions_enabled).lower(),
    ).lower()
    sandbox_provider = _environment_value("AWS_DEPLOYMENT_SANDBOX_PROVIDER", config.sandbox_provider)
    sandbox_provider_secret_name = validate_secret_name(
        _environment_value(
            "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME",
            config.sandbox_provider_secret_name,
        )
    )
    worker_secret_names = _worker_secret_names(config)
    if submissions_enabled not in {"true", "false"}:
        raise ValueError("AWS_MANAGED_SUBMISSIONS_ENABLED must be true or false")
    if not tenant_ids:
        raise ValueError("AWS_MANAGED_TENANT_IDS must contain at least one tenant ID")
    if len(set(tenant_ids)) != len(tenant_ids) or any(not _TENANT_ID.fullmatch(tenant_id) for tenant_id in tenant_ids):
        raise ValueError("AWS_MANAGED_TENANT_IDS must contain unique, comma-separated tenant IDs")
    if submissions_enabled == "true" and (not sandbox_provider or not sandbox_provider_secret_name):
        raise ValueError("Managed submissions require a sandbox provider and secret name")

    return {
        "AWS_MANAGED_TENANT_IDS": ",".join(tenant_ids),
        "AWS_DEPLOYMENT_REGION": cdk.Stack.of(scope).region,
        "AWS_DEPLOYMENT_S3_BUCKET": bucket.bucket_name,
        "AWS_DEPLOYMENT_LOG_GROUP": stage.phys(config.benchmark_log_group_prefix),
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": str(config.benchmark_log_retention_days),
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER": sandbox_provider,
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME": sandbox_provider_secret_name,
        "AWS_MANAGED_AGENT_SECRET_NAMES": ",".join(worker_secret_names),
        "AWS_MANAGED_SUBMISSIONS_ENABLED": submissions_enabled,
        "BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX": (
            f"{stage.phys(config.benchmark_service_access_key_secret_prefix)}/"
        ),
    }


def create_tracker_task_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> aws_iam.Role:
    """Create the tracker application task role."""
    role = _task_role(scope, "TrackerTaskRole", stage.phys("ValkyrieTrackerTaskRole"))
    _add_s3_runtime_access(role, bucket)
    _add_tracker_log_access(role, stage, config.benchmark_log_group_prefix)
    _add_secret_access(role, config.tracker_secret_name_prefixes, ("secretsmanager:GetSecretValue",))
    _add_named_secret_access(role, _sandbox_provider_secret_name(config))
    _add_secret_access(
        role,
        (f"{stage.phys(config.benchmark_service_access_key_secret_prefix)}/",),
        (
            "secretsmanager:CreateSecret",
            "secretsmanager:GetSecretValue",
        ),
    )
    _add_lambda_access(role, config.tracker_lambda_function_name_patterns)
    _add_kms_access(role, config.kms_key_arns)
    return role


def create_worker_task_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> aws_iam.Role:
    """Create the worker application task role."""
    role = _task_role(scope, "WorkerTaskRole", stage.phys("ValkyrieWorkerTaskRole"))
    _add_s3_runtime_access(role, bucket)
    _add_artifact_generation_cleanup_access(role, bucket)
    _add_worker_log_access(role, stage, config.benchmark_log_group_prefix)
    _add_secret_access(role, config.worker_secret_name_prefixes, ("secretsmanager:GetSecretValue",))
    for secret_name in _worker_secret_names(config):
        _add_named_secret_access(role, secret_name)
    _add_named_secret_access(role, _sandbox_provider_secret_name(config))
    _add_secret_access(
        role,
        (f"{stage.phys(config.benchmark_service_access_key_secret_prefix)}/",),
        ("secretsmanager:GetSecretValue",),
    )
    _add_lambda_access(role, config.worker_lambda_function_name_patterns)
    _add_kms_access(role, config.kms_key_arns)
    return role


def _sandbox_provider_secret_name(config: ManagedAWSRuntimeConfig) -> str:
    return validate_secret_name(
        _environment_value(
            "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME",
            config.sandbox_provider_secret_name,
        )
    )


def _worker_secret_names(config: ManagedAWSRuntimeConfig) -> tuple[str, ...]:
    values = _environment_value(
        "AWS_MANAGED_AGENT_SECRET_NAMES",
        ",".join(config.worker_secret_names),
    )
    names = tuple(dict.fromkeys(value.strip() for value in values.split(",") if value.strip()))
    for name in names:
        validate_secret_name(name)
    return names


def validate_secret_name(name: str) -> str:
    if name and not _SECRET_NAME.fullmatch(name):
        raise ValueError("Secret name must be a Secrets Manager name")
    return name


def _add_named_secret_access(role: aws_iam.Role, secret_name: str) -> None:
    if not secret_name:
        return
    stack = cdk.Stack.of(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                stack.format_arn(
                    service="secretsmanager",
                    resource="secret",
                    resource_name=f"{secret_name}-??????",
                    arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                )
            ],
        )
    )


def _task_role(scope: Construct, construct_id: str, role_name: str) -> aws_iam.Role:
    return aws_iam.Role(
        scope,
        construct_id,
        role_name=role_name,
        assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com")),
    )


def _add_s3_runtime_access(role: aws_iam.Role, bucket: aws_s3.IBucket) -> None:
    # ListBucket must be unconditioned: S3 reports a missing object as 404 instead of
    # 403 only when the caller holds ListBucket, and HeadObject's request context has
    # no s3:prefix key for a prefix condition to match.
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[bucket.bucket_arn],
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[bucket.arn_for_objects(prefix) for prefix in _S3_PREFIXES],
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:AbortMultipartUpload", "s3:PutObject"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
        )
    )


def _add_artifact_generation_cleanup_access(
    role: aws_iam.Role,
    bucket: aws_s3.IBucket,
) -> None:
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:DeleteObject"],
            resources=[
                bucket.arn_for_objects(
                    "benchmarks/*/*/.valkyrie/artifacts/*/generations/*",
                )
            ],
        )
    )


def _benchmark_log_group_arn(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> str:
    stack = cdk.Stack.of(role)
    return stack.format_arn(
        service="logs",
        resource="log-group",
        resource_name=f"{stage.phys(log_group_prefix)}/*",
        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
    )


def _add_tracker_log_access(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> None:
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=[
                "logs:DescribeLogStreams",
                "logs:FilterLogEvents",
                "logs:GetLogEvents",
            ],
            resources=[_benchmark_log_group_arn(role, stage, log_group_prefix)],
        )
    )


def _add_worker_log_access(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> None:
    # CreateLogStream authorizes against the log-group ARN (whose IAM form ends in
    # `:*`), which a `...:log-stream:*` pattern never matches; the group wildcard
    # already covers both the groups and their streams.
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:PutRetentionPolicy", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[_benchmark_log_group_arn(role, stage, log_group_prefix)],
        )
    )


def _add_secret_access(
    role: aws_iam.Role,
    secret_name_prefixes: tuple[str, ...],
    actions: tuple[str, ...],
) -> None:
    if not secret_name_prefixes:
        return

    stack = cdk.Stack.of(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=list(actions),
            resources=[
                stack.format_arn(
                    service="secretsmanager",
                    resource="secret",
                    resource_name=f"{prefix}*",
                    arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                )
                for prefix in secret_name_prefixes
            ],
        )
    )


def _add_lambda_access(role: aws_iam.Role, function_name_patterns: tuple[str, ...]) -> None:
    if not function_name_patterns:
        return

    stack = cdk.Stack.of(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                stack.format_arn(
                    service="lambda",
                    resource="function",
                    resource_name=pattern,
                    arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                )
                for pattern in function_name_patterns
            ],
        )
    )


def _add_kms_access(role: aws_iam.Role, key_arns: tuple[str, ...]) -> None:
    if not key_arns:
        return

    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["kms:Decrypt", "kms:GenerateDataKey"],
            resources=list(key_arns),
        )
    )
