"""AWS Secrets Manager utilities."""

import json
from typing import Any

from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import SecretsError
from tracker.logging import get_logger

logger = get_logger(__name__)


def fetch_aws_secret(secret_name: str, client_provider: AWSClientProvider) -> dict[str, Any] | str:
    """Fetch a JSON secret from AWS Secrets Manager by name.

    Args:
        secret_name: The name of the secret to retrieve.
        client_provider: AWS clients for the operation.

    Returns:
        Parsed JSON contents of the secret as a dict or the string value

    Raises:
        SecretsError: If the secret does not exist or cannot be retrieved.
    """
    client = client_provider.secretsmanager_client()
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


def resolve_secrets(secrets: dict[str, str], client_provider: AWSClientProvider) -> dict[str, str]:
    """Resolve AWS secret references to actual values

    Args:
        secrets: Mapping of {ENV_VAR_NAME: aws_secret_name}.
        client_provider: AWS clients for the operation.

    Returns:
        Mapping of {ENV_VAR_NAME: actual_secret_value}.

    Raises:
        SecretsError: If a secret cannot be fetched or a key is missing.
    """
    if not secrets:
        return {}

    resolved: dict[str, str] = {}

    for env_name, secret_name in secrets.items():
        secret_value = fetch_aws_secret(secret_name, client_provider)

        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_name}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = secret_value

    return resolved
