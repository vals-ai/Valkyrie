"""Validation for deployment-managed owner buckets."""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError

from tracker import config
from tracker.aws.runtime import AWSRuntime

_ACCOUNT_ID_PATTERN = re.compile(r"^[0-9]{12}$")
_BUCKET_NAME_PATTERN = re.compile(r"^vs-(dev|prod)-[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_COLLISION_SUFFIX_PATTERN = re.compile(r"^[0-9a-f]{8}$")
_LOGIN_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$")
_OWNER_ID_PATTERN = re.compile(r"^[1-9][0-9]*$")
_VALID_ENVIRONMENTS = frozenset({"dev", "prod"})

_BAD_CONFIGURATION_MESSAGE = "Managed storage configuration is invalid"
_DENIED_MESSAGE = "Managed storage bucket is not authorized"
_INVALID_NAME_MESSAGE = "Invalid managed storage bucket name"
_UNAVAILABLE_MESSAGE = "Managed storage validation is temporarily unavailable"


@dataclass(frozen=True)
class ManagedStoragePolicy:
    expected_account_id: str
    org_environments: Mapping[UUID, frozenset[str]]


class ManagedStorageError(ValueError):
    """Safe managed-storage failure with an HTTP status classification."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


def load_managed_storage_policy() -> ManagedStoragePolicy:
    """Parse the trusted deployment policy without accepting partial values."""
    try:
        account_id = config.AWS_DEPLOYMENT_ACCOUNT_ID
        if account_id is None or _ACCOUNT_ID_PATTERN.fullmatch(account_id) is None:
            raise ValueError

        configured_mapping = json.loads(config.AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS)
        if not isinstance(configured_mapping, dict):
            raise ValueError

        eligible_org_ids = {
            UUID(value.strip()) for value in config.AWS_DEPLOYMENT_ROLE_ORG_IDS.split(",") if value.strip()
        }
        org_environments: dict[UUID, frozenset[str]] = {}
        for raw_org_id, raw_environments in cast(dict[object, object], configured_mapping).items():
            if not isinstance(raw_org_id, str) or not isinstance(raw_environments, list):
                raise ValueError

            org_id = UUID(raw_org_id)
            if raw_org_id != str(org_id) or org_id not in eligible_org_ids:
                raise ValueError

            environments = cast(list[Any], raw_environments)
            if (
                not environments
                or any(not isinstance(environment, str) for environment in environments)
                or len(environments) != len(set(environments))
                or not set(environments).issubset(_VALID_ENVIRONMENTS)
            ):
                raise ValueError

            org_environments[org_id] = frozenset(cast(list[str], environments))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ManagedStorageError(_BAD_CONFIGURATION_MESSAGE, status_code=500) from error

    return ManagedStoragePolicy(expected_account_id=account_id, org_environments=org_environments)


def _validate_name_before_aws(bucket_name: str) -> str:
    if len(bucket_name) > 63 or "--" in bucket_name or _BUCKET_NAME_PATTERN.fullmatch(bucket_name) is None:
        raise ManagedStorageError(_INVALID_NAME_MESSAGE, status_code=400)

    _, environment, remainder = bucket_name.split("-", 2)
    components = remainder.split("-")
    final_component = components[-1]
    has_owner_id = _OWNER_ID_PATTERN.fullmatch(final_component) is not None
    has_owner_id_and_suffix = (
        len(components) >= 3
        and _COLLISION_SUFFIX_PATTERN.fullmatch(final_component) is not None
        and _OWNER_ID_PATTERN.fullmatch(components[-2]) is not None
    )
    if len(components) < 2 or not (has_owner_id or has_owner_id_and_suffix):
        raise ManagedStorageError(_INVALID_NAME_MESSAGE, status_code=400)

    return environment


def _name_matches_owner_tag(bucket_name: str, environment: str, owner_id: str) -> bool:
    if _OWNER_ID_PATTERN.fullmatch(owner_id) is None:
        return False

    remainder = bucket_name.removeprefix(f"vs-{environment}-")
    owner_marker = f"-{owner_id}"
    if remainder.endswith(owner_marker):
        login = remainder[: -len(owner_marker)]
    else:
        candidate, separator, suffix = remainder.rpartition("-")
        if not separator or _COLLISION_SUFFIX_PATTERN.fullmatch(suffix) is None or not candidate.endswith(owner_marker):
            return False
        login = candidate[: -len(owner_marker)]

    return len(login) <= 39 and "--" not in login and _LOGIN_PATTERN.fullmatch(login) is not None


def _safe_aws_error(error: ClientError) -> ManagedStorageError:
    code = error.response.get("Error", {}).get("Code")
    if code in {"403", "404", "AccessDenied", "Forbidden", "NoSuchBucket", "NoSuchTagSet", "NotFound"}:
        return ManagedStorageError(_DENIED_MESSAGE, status_code=403)

    return ManagedStorageError(_UNAVAILABLE_MESSAGE, status_code=503)


async def validate_managed_storage_bucket(
    runtime: AWSRuntime,
    *,
    org_id: UUID,
    bucket_name: str,
    policy: ManagedStoragePolicy,
) -> None:
    """Verify a requested bucket before deployment-managed authority uses it."""
    if runtime.clients.credential_source != "managed":
        raise ManagedStorageError("Managed storage requires deployment AWS authority", status_code=400)

    if runtime.expected_bucket_owner != policy.expected_account_id:
        raise ManagedStorageError(_BAD_CONFIGURATION_MESSAGE, status_code=500)

    allowed_environments = policy.org_environments.get(org_id)
    if allowed_environments is None:
        raise ManagedStorageError(_DENIED_MESSAGE, status_code=403)

    environment = _validate_name_before_aws(bucket_name)
    if environment not in allowed_environments:
        raise ManagedStorageError(_DENIED_MESSAGE, status_code=403)

    request = {"Bucket": bucket_name, "ExpectedBucketOwner": policy.expected_account_id}
    try:
        async with runtime.clients.s3_client() as client:
            head_response = await client.head_bucket(**request)
            if head_response.get("BucketRegion") != runtime.resources.region:
                raise ManagedStorageError(_DENIED_MESSAGE, status_code=403)

            tag_response = await client.get_bucket_tagging(**request)
    except ClientError as error:
        raise _safe_aws_error(error) from error
    except BotoCoreError as error:
        raise ManagedStorageError(_UNAVAILABLE_MESSAGE, status_code=503) from error

    tags = {
        tag.get("Key"): tag.get("Value")
        for tag in tag_response.get("TagSet", [])
        if tag.get("Key") is not None and tag.get("Value") is not None
    }
    owner_id = tags.get("valsmith:owner-account-id")
    if (
        tags.get("valsmith:environment") != environment
        or owner_id is None
        or tags.get("valsmith:backup") != "true"
        or tags.get("valsmith:valkyrie-org-id") != str(org_id)
        or not _name_matches_owner_tag(bucket_name, environment, owner_id)
    ):
        raise ManagedStorageError(_DENIED_MESSAGE, status_code=403)
