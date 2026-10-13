"""AWS Secrets Manager adapters."""

import json
from typing import Any

from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.exceptions import SecretNotFoundError, SecretsError
from tracker.runtime.secrets import SecretValue, secret_display_name


def _error_code(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Code", "")


class SecretBeingDeletedError(SecretsError):
    """A secret with this name was just deleted and AWS hasn't finished removing it."""


class SecretsManagerStore:
    """Read and manage named values through an already-selected AWS client provider."""

    def __init__(self, clients: AWSClientProvider) -> None:
        self._clients = clients

    async def get(self, name: str) -> SecretValue:
        """Fetch and decode one value through a cancellable async client."""
        async with self._clients.secretsmanager_async_client() as client:
            try:
                response: dict[str, Any] = await client.get_secret_value(  # pyright: ignore[reportUnknownMemberType]
                    SecretId=name
                )
            except ClientError as error:
                error_code = _error_code(error)
                if error_code in {"ResourceNotFoundException", "AccessDeniedException"}:
                    raise SecretNotFoundError(f"Secret '{secret_display_name(name)}' does not exist") from error
                raise SecretsError(f"Failed to retrieve secret '{secret_display_name(name)}' ({error_code})") from error

        secret_string = str(response["SecretString"])  # pyright: ignore[reportUnknownArgumentType]
        try:
            return json.loads(secret_string)  # pyright: ignore[reportUnknownVariableType]
        except json.JSONDecodeError:
            return secret_string

    async def put(self, name: str, value: str, *, tags: dict[str, str]) -> None:
        """Create the secret with its tags, or replace the value of the one already there."""
        async with self._clients.secretsmanager_async_client() as client:
            try:
                try:
                    await client.create_secret(  # pyright: ignore[reportUnknownMemberType]
                        Name=name, SecretString=value, Tags=[{"Key": key, "Value": tag} for key, tag in tags.items()]
                    )
                except ClientError as error:
                    if _error_code(error) != "ResourceExistsException":
                        raise
                    await client.put_secret_value(SecretId=name, SecretString=value)  # pyright: ignore[reportUnknownMemberType]
            except ClientError as error:
                if _error_code(error) == "InvalidRequestException" and "scheduled for deletion" in str(error):
                    raise SecretBeingDeletedError(
                        f"Secret '{secret_display_name(name)}' is still being deleted"
                    ) from error
                raise SecretsError(
                    f"Failed to save secret '{secret_display_name(name)}' ({_error_code(error)})"
                ) from error

    async def list_names(self, prefix: str) -> list[str]:
        """Return the names of secrets that start with the prefix."""
        async with self._clients.secretsmanager_async_client() as client:
            pages = client.get_paginator("list_secrets").paginate(Filters=[{"Key": "name", "Values": [prefix]}])  # pyright: ignore[reportUnknownMemberType]
            try:
                names = [str(secret["Name"]) async for page in pages for secret in page.get("SecretList", [])]  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            except ClientError as error:
                raise SecretsError(f"Failed to list secrets ({_error_code(error)})") from error
        return sorted(name for name in names if name.startswith(prefix))

    async def delete(self, name: str) -> bool:
        """Delete the secret immediately, returning False when there is no secret this caller may delete.

        A forced delete never reports a missing secret; under the tenant role, a missing one is denied.
        """
        async with self._clients.secretsmanager_async_client() as client:
            try:
                await client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)  # pyright: ignore[reportUnknownMemberType]
            except ClientError as error:
                if _error_code(error) == "AccessDeniedException":
                    return False
                raise SecretsError(
                    f"Failed to delete secret '{secret_display_name(name)}' ({_error_code(error)})"
                ) from error
        return True
