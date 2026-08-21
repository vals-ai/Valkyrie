"""Validate the AWS account and Region selected for a CDK operation."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast

from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from mypy_boto3_sts import STSClient as BotoStsClient

DEPLOYMENT_REGION = "us-east-1"

_ACCOUNT_ID_PATTERN = re.compile(r"^[0-9]{12}$")


class DeploymentTargetError(ValueError):
    """Raised when deployment credentials do not match the requested target."""


@dataclass(frozen=True)
class DeploymentTarget:
    stage: str
    account_id: str
    region: str


class StsClient(Protocol):
    def get_caller_identity(self) -> Mapping[str, object]: ...


def target_from_environment(environment: Mapping[str, str]) -> DeploymentTarget:
    """Build and validate a deployment target without calling AWS."""
    stage = _required(environment, "STAGE")
    if stage not in ("dev", "release-test", "prod"):
        raise DeploymentTargetError("STAGE must be 'dev', 'release-test', or 'prod'.")

    region = _required(environment, "AWS_REGION")
    if region != DEPLOYMENT_REGION:
        raise DeploymentTargetError(f"AWS_REGION must be {DEPLOYMENT_REGION}; got {region}.")

    production_account_id = _required(environment, "PRODUCTION_ACCOUNT_ID")
    if not _ACCOUNT_ID_PATTERN.fullmatch(production_account_id):
        raise DeploymentTargetError("PRODUCTION_ACCOUNT_ID must be a 12-digit AWS account ID.")

    if stage in ("dev", "release-test"):
        account_id = _required(environment, "DEV_ACCOUNT_ID")
        if not _ACCOUNT_ID_PATTERN.fullmatch(account_id):
            raise DeploymentTargetError("DEV_ACCOUNT_ID must be a 12-digit AWS account ID.")
        if stage == "dev" and account_id == production_account_id:
            raise DeploymentTargetError("DEV_ACCOUNT_ID must not be the production AWS account.")
    else:
        account_id = production_account_id

    cdk_account_id = _required(environment, "CDK_DEFAULT_ACCOUNT")
    if cdk_account_id != account_id:
        raise DeploymentTargetError(f"CDK_DEFAULT_ACCOUNT must be {account_id} for {stage}; got {cdk_account_id}.")

    cdk_region = _required(environment, "CDK_DEFAULT_REGION")
    if cdk_region != region:
        raise DeploymentTargetError(f"CDK_DEFAULT_REGION must match AWS_REGION ({region}); got {cdk_region}.")

    return DeploymentTarget(stage=stage, account_id=account_id, region=region)


def validate_caller_identity(target: DeploymentTarget, caller_identity: Mapping[str, object]) -> None:
    """Validate an STS caller identity against a deployment target."""
    caller_account_id = caller_identity.get("Account")
    if not isinstance(caller_account_id, str) or not caller_account_id:
        raise DeploymentTargetError("STS GetCallerIdentity did not return an AWS account ID.")
    if caller_account_id != target.account_id:
        raise DeploymentTargetError(
            f"AWS credentials belong to account {caller_account_id}; expected {target.account_id} for {target.stage}."
        )


def validate_sts_caller(target: DeploymentTarget, sts_client: StsClient) -> None:
    """Fetch and validate the active AWS caller identity."""
    try:
        caller_identity = sts_client.get_caller_identity()
    except (BotoCoreError, ClientError) as exc:
        raise DeploymentTargetError(
            "AWS caller identity could not be read; check the selected credentials and try again."
        ) from exc
    validate_caller_identity(target, caller_identity)


def enforce_deployment_target(stage_name: str, environment: Mapping[str, str]) -> DeploymentTarget:
    """Validate target environment consistency for a CDK app whose stage comes from context.

    Performs no AWS calls: the CDK CLI itself refuses to deploy when the active
    credentials do not belong to the stack's account, and the STS check runs in
    the Make preflight. This keeps offline synth with a synthetic account working.
    """
    merged = dict(environment)
    merged["STAGE"] = stage_name
    return target_from_environment(merged)


def _required(environment: Mapping[str, str], variable: str) -> str:
    value = environment.get(variable, "").strip()
    if not value:
        raise DeploymentTargetError(f"{variable} is required.")
    return value


def main() -> int:
    try:
        target = target_from_environment(os.environ)
        profile = os.environ.get("PROFILE", "").strip() or None
        session = Session(profile_name=profile, region_name=target.region)
        boto_sts_client: BotoStsClient = session.client(  # pyright: ignore[reportUnknownMemberType]
            "sts",
            region_name=target.region,
        )
        validate_sts_caller(target, cast(StsClient, boto_sts_client))
    except (BotoCoreError, ClientError) as exc:
        raise SystemExit(
            "AWS caller identity could not be read; check the selected credentials and try again."
        ) from exc
    except DeploymentTargetError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Validated {target.stage} deployment credentials for AWS account {target.account_id} in {target.region}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
