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
TRACKER_CPU = 1024
TRACKER_MEMORY = 2048
TRACKER_DOMAIN = "benchmark-tracker.vals.ai"
TRACKER_MIN_TASKS = 1
TRACKER_MAX_TASKS = 2
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
WORKER_CPU = 4096
WORKER_MEMORY = 8192
WORKER_MIN_TASKS = 2
WORKER_MAX_TASKS = 4
WORKER_SCALING_CPU_PERCENT = 70
WORKER_STOP_TIMEOUT_SECONDS = 120  # If protection is enabled the task will not be deleted

# PostgreSQL
POSTGRES_HEALTH_INTERVAL_SECONDS = 60
POSTGRES_HEALTH_START_PERIOD_SECONDS = 10
POSTGRES_USER = "tracker"
POSTGRES_DB = "tracker"

# RDS
RDS_INSTANCE_CLASS = "t4g.small"
RDS_ALLOCATED_STORAGE_GB = 20
RDS_SECRET_NAME = "tracker-db-credentials"

# Load Balancer
ALB_IDLE_TIMEOUT_SECONDS = 60

# S3
S3_BUCKET_NAME = "agentic-harness"


def get_slack_notification_config() -> tuple[str, str] | None:
    workspace_id = os.environ.get("SLACK_WORKSPACE_ID")
    channel_id = os.environ.get("SLACK_CHANNEL_ID")

    if workspace_id and channel_id:
        return workspace_id, channel_id

    if not workspace_id and not channel_id:
        return None

    missing = [
        name
        for name, value in (
            ("SLACK_WORKSPACE_ID", workspace_id),
            ("SLACK_CHANNEL_ID", channel_id),
        )
        if not value
    ]
    raise RuntimeError(
        "Incomplete Slack notification environment configuration. "
        "Set both SLACK_WORKSPACE_ID and SLACK_CHANNEL_ID, or neither. "
        "Missing: " + ", ".join(missing)
    )
