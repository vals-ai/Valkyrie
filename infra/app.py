"""Main CDK application - deploys all services to shared infrastructure."""

import os
from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_ecr, aws_secretsmanager
from constants import RELEASE_TEST_IMAGE_TAG_ENV
from deployment_target import enforce_deployment_target
from driver_stack import DriverStack
from monitoring_stack import MonitoringStack
from shared import SharedStack
from stage import resolve
from executor_stack import ExecutorStack
from tracker_stack import TrackerStack

app = cdk.App()
stage = resolve(app)
if not stage.is_bench:
    enforce_deployment_target(stage.name, os.environ)

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Shared infrastructure (VPC, cluster, service discovery, Route53)
shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=env)

tracker_repository: aws_ecr.IRepository | None = None
executor_host_repository: aws_ecr.IRepository | None = None
release_test_image_tag: str | None = None
if stage.is_release_test:
    if shared.tracker_repository is None or shared.executor_host_repository is None:
        raise RuntimeError("Release-test shared image repositories were not created")
    release_test_image_tag = os.environ.get(RELEASE_TEST_IMAGE_TAG_ENV)
    if not release_test_image_tag:
        raise ValueError(f"Release-test synthesis requires {RELEASE_TEST_IMAGE_TAG_ENV}")
    tracker_repository = cast(aws_ecr.IRepository, shared.tracker_repository)
    executor_host_repository = cast(aws_ecr.IRepository, shared.executor_host_repository)

# Tracker service (public-facing with ALB) + RDS database
tracker = TrackerStack(
    app,
    stage.stack_id("TrackerStack"),
    stage=stage,
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    hosted_zone=shared.hosted_zone,
    bucket_name=shared.bucket_name,
    redis_url=shared.redis_url,
    redis_security_group=shared.redis_security_group,
    tracker_repository=tracker_repository,
    image_tag=release_test_image_tag,
    env=env,
)

# ExecutorHost and its sealed release-control resources
executor = ExecutorStack(
    app,
    # ExecutorStack retains the deployed WorkerStack identity for in-place updates.
    stage.stack_id("WorkerStack"),
    stage=stage,
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    redis_url=shared.redis_url,
    bucket_name=shared.bucket_name,
    database=tracker.database,
    db_credentials=tracker.db_credentials,
    tracker_service=tracker.tracker_fargate_service,
    tracker_image=tracker.tracker_image,
    executor_host_repository=executor_host_repository,
    image_tag=release_test_image_tag,
    env=env,
)

if stage.is_release_test:
    assert tracker_repository is not None
    assert release_test_image_tag is not None
    driver = DriverStack(
        app,
        stage.stack_id("DriverStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        bucket=shared.bucket,
        tracker_repository=tracker_repository,
        image_tag=release_test_image_tag,
        db_host=tracker.database.db_instance_endpoint_address,
        db_port=tracker.database.db_instance_endpoint_port,
        db_credentials=cast(aws_secretsmanager.ISecret, tracker.db_credentials),
        redis_url=shared.redis_url,
        redis_security_group=shared.redis_security_group,
        env=env,
    )
    driver.add_dependency(tracker)

monitoring = MonitoringStack(
    app,
    stage.stack_id("MonitoringStack"),
    stage=stage,
    cluster=shared.cluster,
    tracker_service=tracker.tracker_fargate_service,
    load_balancer=tracker.service.load_balancer,
    target_group=tracker.service.target_group,
    database=tracker.database,
    redis_cluster=shared.redis_cluster,
    env=env,
)

# Deployment order
tracker.add_dependency(shared)
executor.add_dependency(tracker)
monitoring.add_dependency(tracker)

app.synth()
