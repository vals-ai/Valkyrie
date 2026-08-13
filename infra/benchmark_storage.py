"""Least-privilege workload access to Valkyrie's hosted benchmark results."""

from __future__ import annotations

from aws_cdk import aws_ecs, aws_iam


RUNTIME_S3_CREDENTIALS_ENV = "VALKYRIE_USE_RUNTIME_S3_CREDENTIALS"


def runtime_s3_environment() -> dict[str, str]:
    """Enable refreshable workload credentials in the hosted runtime."""
    return {RUNTIME_S3_CREDENTIALS_ENV: "true"}


def grant_benchmark_result_access(
    task_definition: aws_ecs.FargateTaskDefinition,
    *,
    bucket_name: str,
) -> None:
    """Grant only the result-prefix operations used by Tracker and ExecutorHost.

    Agent bundles remain caller-owned. Delete is intentionally excluded so this
    role cannot erase completed result objects.
    """
    object_arn = f"arn:aws:s3:::{bucket_name}/benchmarks/*"
    task_definition.add_to_task_role_policy(
        aws_iam.PolicyStatement(
            actions=[
                "s3:AbortMultipartUpload",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
            ],
            resources=[object_arn],
        )
    )
    task_definition.add_to_task_role_policy(
        aws_iam.PolicyStatement(
            actions=["s3:ListBucket"],
            resources=[f"arn:aws:s3:::{bucket_name}"],
            conditions={"StringLike": {"s3:prefix": ["benchmarks/*"]}},
        )
    )
