"""Main CDK application - deploys all services to shared infrastructure."""

import os

import aws_cdk as cdk
from shared import SharedStack
from swebench_stack import SwebenchStack
from tracker_stack import TrackerStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Shared infrastructure (VPC, cluster, service discovery, Route53)
shared = SharedStack(app, "SharedStack", env=env)

# Tracker service (public-facing with ALB)
tracker = TrackerStack(
    app,
    "TrackerStack",
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    hosted_zone=shared.hosted_zone,
    bucket=shared.bucket,
    env=env,
)

# SWE-bench service (private, only accessible from tracker)
swebench = SwebenchStack(
    app,
    "SwebenchStack",
    vpc=shared.vpc,
    cluster=shared.cluster,
    namespace=shared.namespace,
    tracker_security_group=tracker.service.service.connections.security_groups[0],
    env=env,
)

# Deployment order
tracker.add_dependency(shared)
swebench.add_dependency(tracker)  # Need tracker's security group

app.synth()
