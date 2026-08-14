"""Constants for infrastructure configuration."""

import os

# VPC
VPC_CIDR = "10.0.0.0/16"
VPC_MAX_AZS = 2
VPC_NAT_GATEWAYS = 0

# ECS Cluster
CLUSTER_NAME = "AgenticHarnessCluster"

ALLOWED_IPS: list[tuple[str, str]] = [
    # (CIDR, description)
    ("0.0.0.0/0", "Allow all"),
]

# Service Discovery
NAMESPACE = "local"

# Tracker Service
TRACKER_LOG_GROUP_NAME = "/valkyrie/tracker"
TRACKER_DOMAIN = "benchmark-tracker.vals.ai"
TRACKER_SCALING_CPU_PERCENT = 70

# Health Checks
CONTAINER_HEALTH_INTERVAL_SECONDS = 60
CONTAINER_HEALTH_RETRIES = 3
CONTAINER_HEALTH_START_PERIOD_SECONDS = 15
CONTAINER_HEALTH_TIMEOUT_SECONDS = 5
ALB_HEALTH_INTERVAL_SECONDS = 60

# Ports
TRACKER_PORT = 8000
REDIS_PORT = 6379
POSTGRES_PORT = 5432

# ElastiCache Redis (shared by Tracker + ExecutorHost)
ELASTICACHE_NODE_TYPE = "cache.t4g.micro"

# ExecutorHost and retained legacy log history
WORKER_LOG_GROUP_NAME = "/valkyrie/worker"
EXECUTOR_HOST_LOG_GROUP_NAME = "/valkyrie/executor-host"
DRIVER_LOG_GROUP_NAME = "/valkyrie/package-r-driver"
WORKER_SCALING_CPU_PERCENT = 70
WORKER_STOP_TIMEOUT_SECONDS = 120  # If protection is enabled the task will not be deleted

# Sandbox cleanup
SANDBOX_CLEANUP_FUNCTION_NAME = "valkyrie-sandbox-cleanup"
SANDBOX_CLEANUP_LOG_GROUP_NAME = "/valkyrie/sandbox-cleanup"
SANDBOX_CLEANUP_SCHEDULE_NAME = "valkyrie-sandbox-cleanup"
SANDBOX_CLEANUP_DLQ_NAME = "valkyrie-sandbox-cleanup-dlq"
SANDBOX_CLEANUP_SECRET_NAME = "YourSandboxProviderSecret"

# PostgreSQL
POSTGRES_HEALTH_INTERVAL_SECONDS = 60
POSTGRES_HEALTH_START_PERIOD_SECONDS = 10
POSTGRES_USER = "tracker"
POSTGRES_DB = "tracker"

RDS_SECRET_NAME = "tracker-db-credentials"

# Load Balancer
ALB_IDLE_TIMEOUT_SECONDS = 60

# S3
S3_BUCKET_NAME = "agentic-harness"
EXECUTOR_RELEASE_BUCKET_NAME = "valkyrie-executor-releases"
EXECUTOR_RELEASE_PREFIX = "releases"
EXECUTOR_RELEASE_ROLE_NAME = "ValkyrieExecutorRelease"

DOCKER_ASSET_EXCLUDES = (
    ".git/**",
    ".pi-subagents/**",
    ".scratch/**",
    ".venv/**",
    "**/.pytest_cache/**",
    "**/.ruff_cache/**",
    "**/.venv/**",
    "**/__pycache__/**",
    "**/node_modules/**",
    "infra/cdk.out/**",
)


def executor_release_launch_parameter(stage_name: str) -> str:
    return f"/valkyrie/{stage_name}/executor-release/launch-config"


# Release-test service images. Dedicated repositories keep deployment writes
# inside the stage boundary instead of the account-wide CDK bootstrap repository.
RELEASE_TEST_TRACKER_REPOSITORY_NAME = "valkyrie/release-test/tracker"
RELEASE_TEST_EXECUTOR_HOST_REPOSITORY_NAME = "valkyrie/release-test/executor-host"
RELEASE_TEST_IMAGE_TAG_ENV = "RELEASE_TEST_IMAGE_TAG"


# Stage-scoped account contract parameters. Release-test uses the same dev
# account but must not overwrite dev's resource contract.
def stage_parameter_name(dev_parameter: str, stage_name: str) -> str:
    if stage_name == "dev":
        return dev_parameter
    return dev_parameter.replace("/dev/", f"/{stage_name}/", 1)


# Dev account prerequisites
DEV_TRACKER_HOSTED_ZONE_ID_PARAMETER = "/valkyrie/dev/dns/tracker/hosted-zone-id"

# Dev shared-resource contract published for benchmark-service consumers.
# The benchmark-services registry resolves these names at deploy time; renaming
# any of them is a cross-repo breaking change.
DEV_SHARED_VPC_ID_PARAMETER = "/valkyrie/dev/shared/vpc-id"
DEV_SHARED_AVAILABILITY_ZONES_PARAMETER = "/valkyrie/dev/shared/availability-zones"
DEV_SHARED_PUBLIC_SUBNET_IDS_PARAMETER = "/valkyrie/dev/shared/public-subnet-ids"
DEV_SHARED_CLUSTER_NAME_PARAMETER = "/valkyrie/dev/shared/cluster-name"
DEV_SHARED_NAMESPACE_NAME_PARAMETER = "/valkyrie/dev/shared/cloud-map-namespace-name"
DEV_SHARED_NAMESPACE_ID_PARAMETER = "/valkyrie/dev/shared/cloud-map-namespace-id"
DEV_SHARED_NAMESPACE_ARN_PARAMETER = "/valkyrie/dev/shared/cloud-map-namespace-arn"
DEV_SHARED_ARTIFACT_BUCKET_PARAMETER = "/valkyrie/dev/shared/artifact-bucket-name"
DEV_SHARED_TRACKER_REPOSITORY_URI_PARAMETER = "/valkyrie/dev/shared/tracker-repository-uri"
DEV_SHARED_EXECUTOR_HOST_REPOSITORY_URI_PARAMETER = "/valkyrie/dev/shared/executor-host-repository-uri"
DEV_TRACKER_SECURITY_GROUP_PARAMETER = "/valkyrie/dev/tracker/security-group-id"
DEV_TRACKER_ALB_DNS_PARAMETER = "/valkyrie/dev/tracker/alb-dns-name"
DEV_DRIVER_TASK_DEFINITION_PARAMETER = "/valkyrie/dev/driver/task-definition-arn"
DEV_DRIVER_SECURITY_GROUP_PARAMETER = "/valkyrie/dev/driver/security-group-id"
DEV_DRIVER_LOG_GROUP_PARAMETER = "/valkyrie/dev/driver/log-group-name"
DEV_DRIVER_OPERATOR_ROLE_PARAMETER = "/valkyrie/dev/driver/operator-role-arn"

RELEASE_TEST_DRIVER_SECRET_ARN_ENV = "RELEASE_TEST_DRIVER_SECRET_ARN"
RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN_ENV = "RELEASE_TEST_SANDBOX_PROVIDER_SECRET_ARN"
RELEASE_TEST_OPERATOR_PRINCIPAL_ARN_ENV = "RELEASE_TEST_OPERATOR_PRINCIPAL_ARN"

# Slack notifications
SLACK_WORKSPACE_ID_ENV = "SLACK_WORKSPACE_ID"
VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV = "VALKYRIE_ALERTS_SLACK_CHANNEL_ID"
DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID_ENV = "DEPLOYMENT_NOTIFICATIONS_SLACK_CHANNEL_ID"


def get_slack_notification_config(channel_id_env_var: str) -> tuple[str, str] | None:
    workspace_id = os.environ.get(SLACK_WORKSPACE_ID_ENV)
    channel_id = os.environ.get(channel_id_env_var)

    if channel_id and workspace_id:
        return workspace_id, channel_id

    if not channel_id:
        return None

    raise RuntimeError(
        "Incomplete Slack notification environment configuration. "
        f"Set {SLACK_WORKSPACE_ID_ENV} when setting {channel_id_env_var}. "
        f"Missing: {SLACK_WORKSPACE_ID_ENV}"
    )
