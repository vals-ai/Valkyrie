"""AWS Secrets Manager utilities."""

import json
from typing import Any

import boto3

from tracker.logger import get_logger

logger = get_logger(__name__)

_secret_cache: dict[str, dict[str, Any]] = {}


def fetch_aws_secret(secret_name: str) -> dict[str, Any]:
    """Fetch a JSON secret from AWS Secrets Manager by name.

    Results are cached for the lifetime of the process since secrets
    do not change during a container's lifetime.

    Args:
        secret_name: The name or ARN of the secret to retrieve.

    Returns:
        Parsed JSON contents of the secret as a dict.
    """
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]

    client = boto3.client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]
    response: dict[str, Any] = client.get_secret_value(SecretId=secret_name)  # pyright: ignore[reportUnknownMemberType]
    secret: dict[str, Any] = json.loads(str(response["SecretString"]))  # pyright: ignore[reportUnknownArgumentType]

    _secret_cache[secret_name] = secret

    return secret
