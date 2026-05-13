"""Main CDK application - deploys all services to shared infrastructure."""

import os

import aws_cdk as cdk
from monitoring_stack import MonitoringStack
from shared import SharedStack
from tracker_stack import TrackerStack
from stage_tags import tag_with_deployment_stage
from worker_stack import WorkerStack

app = cdk.App()

stage_name = app.node.try_get_context("stage") or "prod"

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Shared infrastructure (VPC, cluster, service discovery, Route53)
shared = SharedStack(app, "SharedStack", env=env)
tag_with_deployment_stage(shared, stage_name)

# Tracker service (public-facing with ALB) + RDS database
tracker = TrackerStack(
    app,
    "TrackerStack",
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    hosted_zone=shared.hosted_zone,
    bucket=shared.bucket,
    redis_url=shared.redis_url,
    env=env,
)
tag_with_deployment_stage(tracker, stage_name)

# Worker service (Taskiq worker) - deployed independently
worker = WorkerStack(
    app,
    "WorkerStack",
    vpc=shared.vpc,
    cluster=shared.cluster,
    redis_url=shared.redis_url,
    bucket=shared.bucket,
    database=tracker.database,
    db_credentials=tracker.db_credentials,
    tracker_service=tracker.tracker_fargate_service,
    env=env,
)
tag_with_deployment_stage(worker, stage_name)

monitoring = MonitoringStack(
    app,
    "MonitoringStack",
    cluster=shared.cluster,
    tracker_service=tracker.tracker_fargate_service,
    worker_service=worker.worker_service,
    load_balancer=tracker.service.load_balancer,
    target_group=tracker.service.target_group,
    database=tracker.database,
    redis_cluster=shared.redis_cluster,
    env=env,
)
tag_with_deployment_stage(monitoring, stage_name)

# Deployment order
tracker.add_dependency(shared)
worker.add_dependency(tracker)
monitoring.add_dependency(worker)

app.synth()
