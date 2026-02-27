"""AWS Secrets Manager utilities."""

import json
from typing import Any

import boto3

from tracker.logger import get_logger

logger = get_logger(__name__)


def fetch_aws_secret(secret_name: str) -> dict[str, Any] | str:
    """Fetch a JSON secret from AWS Secrets Manager by name.

    Args:
        secret_name: The name of the secret to retrieve.

    Returns:
        Parsed JSON contents of the secret as a dict or the string value
    """

    client = boto3.client("secretsmanager")  # pyright: ignore[reportUnknownMemberType]
    response: dict[str, Any] = client.get_secret_value(SecretId=secret_name)  # pyright: ignore[reportUnknownMemberType]
    secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]

    try:
        return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
    except json.JSONDecodeError:
        return secret_string
