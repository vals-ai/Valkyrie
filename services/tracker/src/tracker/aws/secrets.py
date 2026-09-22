"""AWS Secrets Manager adapters."""

import json
from typing import Any

from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import SecretsError
from tracker.runtime.secrets import SecretValue


def _client_error(name: str, error: ClientError) -> SecretsError:
    error_code = error.response.get("Error", {}).get("Code", "")
    if error_code == "ResourceNotFoundException":
        return SecretsError(f"Secret '{name}' does not exist in AWS Secrets Manager")
    if error_code == "AccessDeniedException":
        return SecretsError(f"Access denied when retrieving secret '{name}'")
    return SecretsError(f"Failed to retrieve secret '{name}': {error}")


def _decode_secret(response: dict[str, Any]) -> SecretValue:
    secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]
    try:
        return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
    except json.JSONDecodeError:
        return secret_string


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
            raise _client_error(name, error) from error
        return _decode_secret(response)

    async def get_async(self, name: str) -> SecretValue:
        """Fetch and decode one value through a cancellable async client."""
        async with self._clients.secretsmanager_async_client() as client:
            try:
                response: dict[str, Any] = await client.get_secret_value(  # pyright: ignore[reportUnknownMemberType]
                    SecretId=name
                )
            except ClientError as error:
                raise _client_error(name, error) from error
        return _decode_secret(response)
