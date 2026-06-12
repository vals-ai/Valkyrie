"""Main CDK application - deploys all services to shared infrastructure."""

import os

import aws_cdk as cdk
from monitoring_stack import MonitoringStack
from shared import SharedStack
from stage import resolve
from tracker_stack import TrackerStack
from worker_stack import WorkerStack

app = cdk.App()
stage = resolve(app)

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Shared infrastructure (VPC, cluster, service discovery, Route53)
shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=env)

# Tracker service (public-facing with ALB) + RDS database
tracker = TrackerStack(
    app,
    stage.stack_id("TrackerStack"),
    stage=stage,
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    hosted_zone=shared.hosted_zone,
    bucket=shared.bucket,
    redis_url=shared.redis_url,
    env=env,
)

# Worker service (Taskiq worker) - deployed independently
worker = WorkerStack(
    app,
    stage.stack_id("WorkerStack"),
    stage=stage,
    vpc=shared.vpc,
    cluster=shared.cluster,
    redis_url=shared.redis_url,
    bucket=shared.bucket,
    database=tracker.database,
    db_credentials=tracker.db_credentials,
    tracker_service=tracker.tracker_fargate_service,
    env=env,
)

monitoring = MonitoringStack(
    app,
    stage.stack_id("MonitoringStack"),
    stage=stage,
    cluster=shared.cluster,
    tracker_service=tracker.tracker_fargate_service,
    worker_service=worker.worker_service,
    load_balancer=tracker.service.load_balancer,
    target_group=tracker.service.target_group,
    database=tracker.database,
    redis_cluster=shared.redis_cluster,
    env=env,
)

# Deployment order
tracker.add_dependency(shared)
worker.add_dependency(tracker)
monitoring.add_dependency(worker)

app.synth()
