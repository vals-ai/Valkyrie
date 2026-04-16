"""AWS Secrets Manager utilities."""

import json
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError

from tracker.exceptions import SecretsError
from tracker.logging import get_logger
from tracker.types import AWSCredentials

logger = get_logger(__name__)


@lru_cache(maxsize=32)
def _secretsmanager_client(aws: AWSCredentials) -> Any:
    """Cached Secrets Manager client, shared per credential tuple."""
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "secretsmanager",
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        region_name=aws.aws_default_region,
    )


def fetch_aws_secret(secret_name: str, aws: AWSCredentials) -> dict[str, Any] | str:
    """Fetch a JSON secret from AWS Secrets Manager by name.

    Args:
        secret_name: The name of the secret to retrieve.
        aws: User's AWS credentials for accessing Secrets Manager.

    Returns:
        Parsed JSON contents of the secret as a dict or the string value

    Raises:
        SecretsError: If the secret does not exist or cannot be retrieved.
    """
    client = _secretsmanager_client(aws)  # pyright: ignore[reportUnknownVariableType]
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


def resolve_secrets(secrets: dict[str, str], aws: AWSCredentials) -> dict[str, str]:
    """Resolve AWS secret references to actual values

    Args:
        secrets: Mapping of {ENV_VAR_NAME: aws_secret_name}.
        aws: User's AWS credentials for accessing Secrets Manager.

    Returns:
        Mapping of {ENV_VAR_NAME: actual_secret_value}.

    Raises:
        SecretsError: If a secret cannot be fetched or a key is missing.
    """
    if not secrets:
        return {}

    resolved: dict[str, str] = {}

    for env_name, secret_name in secrets.items():
        secret_value = fetch_aws_secret(secret_name, aws)

        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_name}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = secret_value

    return resolved
