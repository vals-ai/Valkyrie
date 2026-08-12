"""Notify when a new production ExecutorHost task definition is running.

The monitor deliberately does not wait for the ECS service to reach steady state:
protected tasks from the previous deployment may still be draining when the new
PRIMARY deployment can accept work.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import boto3

from constants import CLUSTER_NAME

_SERVICE = "ExecutorHost"
_IN_PROGRESS_ALERT_TEXT = (
    "New ExecutorHost revision is running and available for new runs. "
    "Rollout remains in progress; previous tasks may still be draining."
)
_COMPLETED_ALERT_TEXT = "New ExecutorHost revision is running and available for new runs. Rollout is complete."
_ALERTABLE_ROLLOUT_STATES = frozenset({"IN_PROGRESS", "COMPLETED"})


class EcsClient(Protocol):
    def describe_services(self, *, cluster: str, services: Sequence[str]) -> Mapping[str, object]: ...


class StsClient(Protocol):
    def assume_role(self, *, RoleArn: str, RoleSessionName: str) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class PrimaryDeployment:
    task_definition: str
    running_count: int
    rollout_state: str


class MonitorError(RuntimeError):
    """Raised when ECS does not return the one expected service deployment."""


def create_ecs_client(*, lookup_role_arn: str) -> EcsClient:
    client_factory = cast(Callable[..., object], boto3.client)
    sts = cast(StsClient, client_factory("sts"))
    response = sts.assume_role(
        RoleArn=lookup_role_arn,
        RoleSessionName="valkyrie-executor-rollout-monitor",
    )
    credentials_value = response.get("Credentials")
    if not isinstance(credentials_value, Mapping):
        raise MonitorError("STS AssumeRole returned no credentials")
    credentials = cast(Mapping[str, object], credentials_value)
    access_key = credentials.get("AccessKeyId")
    secret_key = credentials.get("SecretAccessKey")
    session_token = credentials.get("SessionToken")
    if (
        not isinstance(access_key, str)
        or not access_key
        or not isinstance(secret_key, str)
        or not secret_key
        or not isinstance(session_token, str)
        or not session_token
    ):
        raise MonitorError("STS AssumeRole returned malformed credentials")
    return cast(
        EcsClient,
        client_factory(
            "ecs",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
        ),
    )


def describe_service(client: EcsClient, *, cluster: str, service: str) -> Mapping[str, object]:
    response = client.describe_services(cluster=cluster, services=[service])
    failures = response.get("failures")
    if failures:
        raise MonitorError(f"ECS could not describe {service}: {failures!r}")
    return response


def primary_deployment(response: Mapping[str, object]) -> PrimaryDeployment | None:
    services_value = response.get("services")
    if not isinstance(services_value, list):
        return None
    services = cast(list[object], services_value)
    if len(services) != 1:
        return None
    service_value = services[0]
    if not isinstance(service_value, Mapping):
        return None
    service = cast(Mapping[str, object], service_value)
    deployments_value = service.get("deployments")
    if not isinstance(deployments_value, list):
        return None
    deployments = cast(list[object], deployments_value)
    primary: list[Mapping[str, object]] = []
    for deployment_value in deployments:
        if not isinstance(deployment_value, Mapping):
            continue
        deployment = cast(Mapping[str, object], deployment_value)
        if deployment.get("status") == "PRIMARY":
            primary.append(deployment)
    if len(primary) != 1:
        return None

    deployment = primary[0]
    task_definition = deployment.get("taskDefinition")
    running_count = deployment.get("runningCount")
    rollout_state = deployment.get("rolloutState")
    if (
        not isinstance(task_definition, str)
        or not task_definition
        or not isinstance(running_count, int)
        or isinstance(running_count, bool)
        or running_count < 0
        or not isinstance(rollout_state, str)
    ):
        return None
    return PrimaryDeployment(
        task_definition=task_definition,
        running_count=running_count,
        rollout_state=rollout_state,
    )


def is_new_revision_available(deployment: PrimaryDeployment | None, *, baseline_task_definition: str) -> bool:
    return (
        deployment is not None
        and deployment.task_definition != baseline_task_definition
        and deployment.running_count >= 1
        and deployment.rollout_state in _ALERTABLE_ROLLOUT_STATES
    )


def capture_baseline(client: EcsClient, *, cluster: str, service: str) -> str:
    deployment = primary_deployment(describe_service(client, cluster=cluster, service=service))
    if deployment is None:
        raise MonitorError(f"ECS returned no valid PRIMARY deployment for {service}")
    return deployment.task_definition


def monitor_rollout(
    *,
    describe: Callable[[], Mapping[str, object]],
    baseline_task_definition: str,
    notify: Callable[[PrimaryDeployment], None],
    timeout_seconds: float,
    poll_seconds: float,
    stop_requested: Callable[[], bool] = lambda: False,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    deadline = monotonic() + timeout_seconds
    while True:
        deployment = primary_deployment(describe())
        if is_new_revision_available(deployment, baseline_task_definition=baseline_task_definition):
            assert deployment is not None
            notify(deployment)
            return True
        if stop_requested():
            return False
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False
        sleep(min(poll_seconds, remaining))


def post_slack(webhook_url: str, *, deployment: PrimaryDeployment, run_url: str) -> None:
    alert_text = _COMPLETED_ALERT_TEXT if deployment.rollout_state == "COMPLETED" else _IN_PROGRESS_ALERT_TEXT
    context = f"Environment: prod · Service: {_SERVICE} · Task definition: {deployment.task_definition}"
    if run_url:
        context += f" · <{run_url}|Deploy run>"
    payload = json.dumps(
        {
            "text": alert_text,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": f":large_green_circle: *{alert_text}*"}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": context}]},
            ],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10):  # noqa: S310 - URL is a trusted GitHub secret.
        pass


def _warn(message: str) -> None:
    print(f"::warning::{message}", file=sys.stderr)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("capture-baseline", "monitor"))
    parser.add_argument("--baseline-task-definition")
    parser.add_argument("--lookup-role-arn", required=True)
    parser.add_argument("--cluster", default=CLUSTER_NAME)
    parser.add_argument("--service", default=_SERVICE)
    parser.add_argument("--timeout-seconds", type=float, default=3600)
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--run-url", default="")
    parser.add_argument("--stop-file")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "capture-baseline":
            client = create_ecs_client(lookup_role_arn=args.lookup_role_arn)
            print(capture_baseline(client, cluster=args.cluster, service=args.service))
            return 0

        if not args.baseline_task_definition:
            _warn("Skipping early ExecutorHost alert because no baseline task definition was captured.")
            return 0
        webhook_url = os.environ.get("SLACK_DEPLOY_WEBHOOK_URL", "").strip()
        if not webhook_url:
            _warn("SLACK_DEPLOY_WEBHOOK_URL is not set; skipping early ExecutorHost alert.")
            return 0

        stop_file = Path(args.stop_file) if args.stop_file else None
        client = create_ecs_client(lookup_role_arn=args.lookup_role_arn)
        notified = monitor_rollout(
            describe=lambda: describe_service(client, cluster=args.cluster, service=args.service),
            baseline_task_definition=args.baseline_task_definition,
            notify=lambda deployment: post_slack(webhook_url, deployment=deployment, run_url=args.run_url),
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            stop_requested=lambda: stop_file is not None and stop_file.exists(),
        )
        if not notified:
            if stop_file is not None and stop_file.exists():
                _warn("ExecutorHost deployment finished before a new running PRIMARY was observed.")
            else:
                _warn("Timed out before a new running ExecutorHost PRIMARY deployment was observed.")
        return 0
    except Exception as error:
        if args.command == "monitor":
            _warn(f"Early ExecutorHost alert failed without affecting deployment: {error}")
            return 0
        raise


if __name__ == "__main__":
    raise SystemExit(main())
