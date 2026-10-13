"""Managed AWS runtime environment and ECS task-role policies."""

from __future__ import annotations

from typing import Literal, cast

import aws_cdk as cdk
from aws_cdk import aws_iam, aws_s3
from constructs import Construct

from stage import Stage
from stage_config import ManagedAWSRuntimeConfig

_S3_PREFIXES = ("agents/*", "benchmarks/*")
_ORG_ID_TAG = "valkyrie:org-id"
_SECRET_SCOPE_TAG = "valkyrie:secret-scope"
_SESSION_TAGS = [_ORG_ID_TAG, _SECRET_SCOPE_TAG]
_SESSION_ORG = f"${{aws:PrincipalTag/{_ORG_ID_TAG}}}"
_AGENT_WRITE_ACTIONS = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"]
_TENANT_OBJECT_ACTIONS = {
    "Tracker": {
        "agents/*": ["s3:GetObject", *_AGENT_WRITE_ACTIONS],
        "benchmarks/*": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
    },
    "Executor": {
        "benchmarks/*": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"],
    },
}
TenantService = Literal["Tracker", "Executor"]


def managed_runtime_environment(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> dict[str, str]:
    """Build the deployment-owned runtime configuration for an ECS container."""
    return {
        "AWS_DEPLOYMENT_ACCOUNT_ID": cdk.Stack.of(scope).account,
        "AWS_DEPLOYMENT_ROLE_ORG_IDS": ",".join(config.deployment_role_org_ids),
        "AWS_SHARED_SECRET_TENANTS": ",".join(config.shared_secret_tenants),
        "AWS_DEPLOYMENT_REGION": cdk.Stack.of(scope).region,
        "AWS_DEPLOYMENT_S3_BUCKET": bucket.bucket_name,
        "AWS_DEPLOYMENT_LOG_GROUP": stage.phys(config.benchmark_log_group_prefix),
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": str(config.benchmark_log_retention_days),
        "AWS_MANAGED_SUBMISSIONS_ENABLED": str(config.submissions_enabled).lower(),
    }


def tenant_bucket_prefix(scope: Construct, stage: Stage) -> str:
    """Name prefix of the per-organization buckets Tracker creates in this account and stage."""
    return f"valkyrie-{cdk.Stack.of(scope).account}-{stage.name}"


def create_tracker_task_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> aws_iam.Role:
    """Create the tracker application task role."""
    role = _task_role(scope, "TrackerTaskRole", stage.phys("ValkyrieTrackerTaskRole"))
    _add_s3_runtime_access(role, bucket)
    account_conditions = _same_account_conditions(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=_AGENT_WRITE_ACTIONS,
            resources=[bucket.arn_for_objects("agents/*")],
            conditions=account_conditions,
        )
    )
    if config.tracker_agent_library_read_only:
        # The agent registry publishes this library; Tracker may only read it.
        _deny_agent_writes(role, bucket)

    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:DeleteObject", "s3:DeleteObjectVersion"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
            conditions=account_conditions,
        )
    )
    _add_tracker_tenant_bucket_access(role, stage)
    _add_secret_access(role, config.tracker_secret_name_prefixes)
    _add_lambda_access(role, config.tracker_lambda_function_name_patterns)
    _add_benchmark_log_read_access(role, stage, config.benchmark_log_group_prefix)
    _add_kms_access(role, config.kms_key_arns)
    return role


def create_executor_task_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
) -> aws_iam.Role:
    """Create the one-dispatch executor runner application task role."""
    role = _task_role(scope, "ExecutorRunnerTaskRole", stage.phys("ValkyrieExecutorRunnerTaskRole"))
    _add_s3_runtime_access(role, bucket)
    account_conditions = _same_account_conditions(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:AbortMultipartUpload"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
            conditions=account_conditions,
        )
    )
    _add_executor_tenant_bucket_access(role, stage)
    _add_benchmark_log_access(role, stage, config.benchmark_log_group_prefix)
    if config.executor_all_secret_access:
        _add_all_secret_access(role)
    else:
        _add_secret_access(role, config.executor_secret_name_prefixes)
    _add_lambda_access(role, config.executor_lambda_function_name_patterns)
    _add_kms_access(role, config.kms_key_arns)
    return role


def create_tenant_access_role(
    scope: Construct,
    stage: Stage,
    bucket: aws_s3.IBucket,
    config: ManagedAWSRuntimeConfig,
    *,
    service: TenantService,
    task_role: aws_iam.Role,
) -> aws_iam.Role:
    """Create the role whose session tags limit a service's bucket objects and secrets to one organization."""
    role = aws_iam.Role(
        scope,
        f"{service}TenantAccessRole",
        role_name=stage.phys(f"Valkyrie{service}TenantAccessRole"),
        assumed_by=cast(
            aws_iam.IPrincipal,
            aws_iam.SessionTagsPrincipal(
                aws_iam.PrincipalWithConditions(
                    cast(aws_iam.IPrincipal, task_role),
                    {
                        "StringLike": {f"aws:RequestTag/{_ORG_ID_TAG}": "?*"},
                        "ForAllValues:StringEquals": {"aws:TagKeys": _SESSION_TAGS},
                    },
                )
            ),
        ),
        max_session_duration=cdk.Duration.hours(1),
    )
    task_role.add_to_policy(
        aws_iam.PolicyStatement(actions=["sts:AssumeRole", "sts:TagSession"], resources=[role.role_arn])
    )
    stack = cdk.Stack.of(role)
    tenant_conditions = {
        "StringEquals": {
            f"aws:ResourceTag/{_ORG_ID_TAG}": f"${{aws:PrincipalTag/{_ORG_ID_TAG}}}",
            "s3:ResourceAccount": stack.account,
        }
    }
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:ListBucket"], resources=[f"arn:{stack.partition}:s3:::*"], conditions=tenant_conditions
        )
    )
    for prefix, actions in _TENANT_OBJECT_ACTIONS[service].items():
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=actions, resources=[f"arn:{stack.partition}:s3:::*/{prefix}"], conditions=tenant_conditions
            )
        )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[_secret_arn(stack, f"valkyrie/tenants/{_SESSION_ORG}/*")],
            conditions={"StringEquals": {f"secretsmanager:ResourceTag/{_ORG_ID_TAG}": _SESSION_ORG}},
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[_secret_arn(stack, "*")],
            conditions={
                "StringEquals": {
                    f"secretsmanager:ResourceTag/{_SECRET_SCOPE_TAG}": "shared-internal",
                    f"aws:PrincipalTag/{_SECRET_SCOPE_TAG}": "shared-internal",
                }
            },
        )
    )
    denied = [
        "secretsmanager:UntagResource",
        "s3:TagResource",
        "s3:UntagResource",
        "s3:PutBucketTagging",
        "s3:DeleteBucketTagging",
        "s3:PutBucketABAC",
    ]
    if service == "Tracker":
        _add_tenant_secret_management(role)
        if config.tracker_agent_library_read_only:
            _deny_agent_writes(role, bucket)
    else:
        denied.append("secretsmanager:TagResource")
    role.add_to_policy(aws_iam.PolicyStatement(effect=aws_iam.Effect.DENY, actions=denied, resources=["*"]))
    _add_kms_access(role, config.kms_key_arns)
    return role


def _add_tenant_secret_management(role: aws_iam.Role) -> None:
    """Let Tracker create, replace, and delete secrets only under its session organization's prefix.

    Listing is account-wide, because IAM can't scope ListSecrets; Tracker filters names by prefix.
    """
    own_secrets = _secret_arn(cdk.Stack.of(role), f"valkyrie/tenants/{_SESSION_ORG}/*")
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:CreateSecret", "secretsmanager:TagResource"],
            resources=[own_secrets],
            conditions={
                "StringEquals": {f"aws:RequestTag/{_ORG_ID_TAG}": _SESSION_ORG},
                "ForAllValues:StringEquals": {"aws:TagKeys": [_ORG_ID_TAG]},
            },
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"],
            resources=[own_secrets],
            conditions={"StringEquals": {f"secretsmanager:ResourceTag/{_ORG_ID_TAG}": _SESSION_ORG}},
        )
    )
    role.add_to_policy(aws_iam.PolicyStatement(actions=["secretsmanager:ListSecrets"], resources=["*"]))


def _secret_arn(stack: cdk.Stack, name: str) -> str:
    return stack.format_arn(
        service="secretsmanager", resource="secret", resource_name=name, arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME
    )


def _deny_agent_writes(role: aws_iam.Role, bucket: aws_s3.IBucket) -> None:
    role.add_to_policy(
        aws_iam.PolicyStatement(
            effect=aws_iam.Effect.DENY,
            actions=_AGENT_WRITE_ACTIONS,
            resources=[bucket.arn_for_objects("agents/*")],
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
    account_conditions = _same_account_conditions(role)
    # ListBucket must not use an s3:prefix condition: S3 reports a missing object as
    # 404 instead of 403 only when the caller holds ListBucket, and HeadObject's
    # request context has no s3:prefix key for a prefix condition to match.
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[bucket.bucket_arn],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[bucket.arn_for_objects(prefix) for prefix in _S3_PREFIXES],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=[bucket.arn_for_objects("benchmarks/*")],
            conditions=account_conditions,
        )
    )


def _tenant_buckets_arn(role: aws_iam.Role, stage: Stage) -> str:
    return f"arn:{cdk.Stack.of(role).partition}:s3:::{tenant_bucket_prefix(role, stage)}-*"


def _add_tracker_tenant_bucket_access(role: aws_iam.Role, stage: Stage) -> None:
    """Let Tracker create tagged, ABAC-enabled organization buckets and manage their objects."""
    buckets = _tenant_buckets_arn(role, stage)
    account_conditions = _same_account_conditions(role)
    role.add_to_policy(aws_iam.PolicyStatement(actions=["s3:CreateBucket", "s3:TagResource"], resources=[buckets]))
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=[
                "s3:ListBucket",
                "s3:GetBucketTagging",
                "s3:PutBucketVersioning",
                "s3:PutLifecycleConfiguration",
                "s3:PutBucketABAC",
            ],
            resources=[buckets],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[f"{buckets}/agents/*", f"{buckets}/benchmarks/*"],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=_AGENT_WRITE_ACTIONS,
            resources=[f"{buckets}/agents/*"],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"],
            resources=[f"{buckets}/benchmarks/*"],
            conditions=account_conditions,
        )
    )


def _add_executor_tenant_bucket_access(role: aws_iam.Role, stage: Stage) -> None:
    """Let executors read frozen agents and write run artifacts in organization buckets."""
    buckets = _tenant_buckets_arn(role, stage)
    account_conditions = _same_account_conditions(role)
    role.add_to_policy(
        aws_iam.PolicyStatement(actions=["s3:ListBucket"], resources=[buckets], conditions=account_conditions)
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[f"{buckets}/benchmarks/*"],
            conditions=account_conditions,
        )
    )
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["s3:PutObject", "s3:AbortMultipartUpload"],
            resources=[f"{buckets}/benchmarks/*"],
            conditions=account_conditions,
        )
    )


def _same_account_conditions(role: aws_iam.Role) -> dict[str, dict[str, str]]:
    return {"StringEquals": {"s3:ResourceAccount": cdk.Stack.of(role).account}}


def _benchmark_log_group_arn(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> str:
    return cdk.Stack.of(role).format_arn(
        service="logs",
        resource="log-group",
        resource_name=f"{stage.phys(log_group_prefix)}/*",
        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
    )


def _add_benchmark_log_read_access(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> None:
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["logs:GetLogEvents", "logs:FilterLogEvents"],
            resources=[_benchmark_log_group_arn(role, stage, log_group_prefix)],
        )
    )


def _add_benchmark_log_access(role: aws_iam.Role, stage: Stage, log_group_prefix: str) -> None:
    log_group_arn = _benchmark_log_group_arn(role, stage, log_group_prefix)
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
            resources=[_secret_arn(stack, f"{prefix}*") for prefix in secret_name_prefixes],
        )
    )


def _add_all_secret_access(role: aws_iam.Role) -> None:
    role.add_to_policy(
        aws_iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"], resources=[_secret_arn(cdk.Stack.of(role), "*")]
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
