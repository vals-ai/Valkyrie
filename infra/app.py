"""Valkyrie CDK application."""

import os

import aws_cdk as cdk
from deployment_access_stack import DeploymentAccessStack
from deployment_target import DeploymentTargetError, target_from_environment
from dns_stack import DnsStack
from monitoring_stack import MonitoringStack
from shared import SharedStack
from stage import Stage, resolve
from tracker_stack import TrackerStack
from worker_stack import WorkerStack

DEPLOYMENT_ACCESS_SCOPE = "deployment-access"
DNS_ZONE_SCOPE = "dns-zone"
APPLICATION_SCOPES = ("shared", "tracker", "worker", "monitoring", "all")
SCOPES = (DEPLOYMENT_ACCESS_SCOPE, DNS_ZONE_SCOPE, *APPLICATION_SCOPES)


def build_stacks(
    app: cdk.App,
    stage: Stage,
    deployment_scope: str,
    env: cdk.Environment,
) -> tuple[cdk.Stack, ...]:
    """Construct only the stacks needed for one deployment scope."""
    if deployment_scope not in SCOPES:
        raise ValueError(f"unknown deployment scope {deployment_scope!r}; expected one of {SCOPES!r}")

    if deployment_scope in (DEPLOYMENT_ACCESS_SCOPE, DNS_ZONE_SCOPE):
        if stage.is_prod:
            raise ValueError(f"{deployment_scope} is only available for the dev stage")
        if deployment_scope == DEPLOYMENT_ACCESS_SCOPE:
            return (DeploymentAccessStack(app, stage.stack_id("DeploymentAccessStack"), env=env),)
        return (DnsStack(app, stage.stack_id("DnsZoneStack"), env=env),)

    shared = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=env)
    if deployment_scope == "shared":
        return (shared,)

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
    tracker.add_dependency(shared)
    if deployment_scope == "tracker":
        return shared, tracker

    worker = WorkerStack(
        app,
        stage.stack_id("WorkerStack"),
        stage=stage,
        vpc=shared.vpc,
        cluster=shared.cluster,
        namespace=shared.namespace,
        redis_url=shared.redis_url,
        bucket=shared.bucket,
        database=tracker.database,
        db_credentials=tracker.db_credentials,
        tracker_service=tracker.tracker_fargate_service,
        env=env,
    )
    worker.add_dependency(tracker)
    if deployment_scope == "worker":
        return shared, tracker, worker

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
    monitoring.add_dependency(worker)
    return shared, tracker, worker, monitoring


def main() -> None:
    app = cdk.App()
    stage = resolve(app)
    target = target_from_environment(os.environ)
    if target.stage != stage.name:
        raise DeploymentTargetError(f"CDK stage context is {stage.name}; STAGE selects {target.stage}.")
    deployment_scope = app.node.try_get_context("scope") or "all"
    env = cdk.Environment(account=target.account_id, region=target.region)
    build_stacks(app, stage, deployment_scope, env)
    app.synth()


if __name__ == "__main__":
    main()
