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

# ElastiCache Redis (shared by tracker + worker)
ELASTICACHE_NODE_TYPE = "cache.t4g.micro"

# Worker Service
WORKER_LOG_GROUP_NAME = "/valkyrie/worker"
WORKER_SCALING_CPU_PERCENT = 70
WORKER_STOP_TIMEOUT_SECONDS = 120  # If protection is enabled the task will not be deleted

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

# Dev account prerequisites
DEV_TRACKER_CERTIFICATE_ARN_PARAMETER = "/valkyrie/dev/dns/tracker/certificate-arn"
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
DEV_TRACKER_SECURITY_GROUP_PARAMETER = "/valkyrie/dev/tracker/security-group-id"
DEV_TRACKER_ALB_DNS_PARAMETER = "/valkyrie/dev/tracker/alb-dns-name"

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
