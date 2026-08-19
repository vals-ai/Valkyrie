"""Managed AWS runtime environment and ECS task-role policies."""

from __future__ import annotations

from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_iam, aws_s3
from constructs import Construct

from stage import Stage
from stage_config import ManagedAWSRuntimeConfig

_S3_PREFIXES = ("agents/*", "benchmarks/*")


def managed_runtime_environment(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> dict[str, str]:
    """Build the deployment-owned runtime configuration for an ECS container."""
    return {
        "AWS_DEPLOYMENT_ROLE_ORG_IDS": ",".join(config.deployment_role_org_ids),
        "AWS_DEPLOYMENT_REGION": cdk.Stack.of(scope).region,
        "AWS_DEPLOYMENT_S3_BUCKET": bucket.bucket_name,
        "AWS_DEPLOYMENT_LOG_GROUP": stage.phys(config.benchmark_log_group_prefix),
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": str(config.benchmark_log_retention_days),
        "AWS_MANAGED_SUBMISSIONS_ENABLED": str(config.submissions_enabled).lower(),
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
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:DeleteObject", "s3:DeleteObjectVersion"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
        )
    )
    _add_secret_access(role, config.tracker_secret_name_prefixes)
    _add_lambda_access(role, config.tracker_lambda_function_name_patterns)
    _add_kms_access(role, config.kms_key_arns)
    return role


def create_executor_task_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> aws_iam.Role:
    """Create the executor host application task role."""
    role = _task_role(scope, "ExecutorTaskRole", stage.phys("ValkyrieExecutorTaskRole"))
    _add_s3_runtime_access(role, bucket)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:AbortMultipartUpload"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
        )
    )
    _add_benchmark_log_access(role, stage, config.benchmark_log_group_prefix)
    if config.executor_all_secret_access:
        _add_all_secret_access(role)
    else:
        _add_secret_access(role, config.executor_secret_name_prefixes)
    _add_lambda_access(role, config.executor_lambda_function_name_patterns)
    _add_kms_access(role, config.kms_key_arns)
    return role


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
            actions=["s3:PutObject"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
        )
    )


def _add_benchmark_log_access(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> None:
    stack = cdk.Stack.of(role)
    log_group_arn = stack.format_arn(
        service="logs",
        resource="log-group",
        resource_name=f"{stage.phys(log_group_prefix)}/*",
        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
    )
    # Executors create per-run log groups and apply the deployment-owned retention policy.
    # CreateLogStream authorizes against the log-group ARN (whose IAM form ends in
    # `:*`), which a `...:log-stream:*` pattern never matches; the group wildcard
    # already covers both the groups and their streams.
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:PutRetentionPolicy", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=[log_group_arn],
        )
    )


def _add_secret_access(role: aws_iam.Role, secret_name_prefixes: tuple[str, ...]) -> None:
    if not secret_name_prefixes:
        return

    stack = cdk.Stack.of(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
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


def _add_all_secret_access(role: aws_iam.Role) -> None:
    stack = cdk.Stack.of(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                stack.format_arn(
                    service="secretsmanager",
                    resource="secret",
                    resource_name="*",
                    arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                )
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
