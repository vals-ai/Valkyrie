"""AWS Secrets Manager utilities."""

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tracker.exceptions import SecretsError
from tracker.logger import get_logger

logger = get_logger(__name__)


def fetch_aws_secret(secret_name: str) -> dict[str, Any] | str:
    """Fetch a JSON secret from AWS Secrets Manager by name.

    Args:
        secret_name: The name of the secret to retrieve.

    Returns:
        Parsed JSON contents of the secret as a dict or the string value

    Raises:
        SecretsError: If the secret does not exist or cannot be retrieved.
    """
    client = boto3.client("secretsmanager")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    try:
        response: dict[str, Any] = client.get_secret_value(SecretId=secret_name)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ResourceNotFoundException":
            raise SecretsError(f"Secret '{secret_name}' does not exist in AWS Secrets Manager") from e
        if error_code == "AccessDeniedException":
            raise SecretsError(f"Access denied when retrieving secret '{secret_name}'") from e
        raise SecretsError(f"Failed to retrieve secret '{secret_name}': {e}") from e

    secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]

    try:
        return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
    except json.JSONDecodeError:
        return secret_string
