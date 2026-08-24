"""AWS Secrets Manager adapters."""

import json
from typing import Any

from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import SecretsError
from tracker.runtime.secrets import SecretValue


class SecretsManagerStore:
    """Read named values through an already-selected AWS client provider."""

    def __init__(self, clients: AWSClientProvider) -> None:
        self._clients = clients

    def get(self, name: str) -> SecretValue:
        """Fetch and decode one AWS Secrets Manager value."""
        client = self._clients.secretsmanager_client()
        try:
            response: dict[str, Any] = client.get_secret_value(SecretId=name)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                raise SecretsError(f"Secret '{name}' does not exist in AWS Secrets Manager") from error
            if error_code == "AccessDeniedException":
                raise SecretsError(f"Access denied when retrieving secret '{name}'") from error
            raise SecretsError(f"Failed to retrieve secret '{name}': {error}") from error

        secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]
        try:
            return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
        except json.JSONDecodeError:
            return secret_string
