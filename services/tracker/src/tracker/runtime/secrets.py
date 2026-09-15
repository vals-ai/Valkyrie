"""Provider-neutral secret reads and reference resolution."""

from typing import Protocol, TypeAlias

from benchmark_service import SandboxProviderConfig, sandbox_provider_config_from_mapping

from tracker.exceptions import SecretsError, TrackerServiceError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SecretValue: TypeAlias = JsonValue


class SecretStore(Protocol):
    """Synchronous access to named secret values."""

    def get(self, name: str) -> SecretValue:
        """Return decoded JSON, or the raw string when the value is not JSON."""
        raise NotImplementedError


class AsyncSecretStore(Protocol):
    """Asynchronous access to named secret values."""

    async def get_async(self, name: str) -> SecretValue:
        """Return decoded JSON, or the raw string when the value is not JSON."""
        raise NotImplementedError


def resolve_secrets(secrets: dict[str, str], secret_store: SecretStore) -> dict[str, str]:
    """Resolve environment-variable secret references to their current values."""
    if not secrets:
        return {}

    resolved: dict[str, str] = {}
    for env_name, secret_name in secrets.items():
        secret_value = secret_store.get(secret_name)
        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_name}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = str(secret_value)
    return resolved


def _sandbox_provider_config_from_secret(secret: SecretValue, provider_type: str) -> SandboxProviderConfig:
    if not isinstance(secret, dict):
        raise TrackerServiceError("Expected sandbox provider secret to be a JSON object")
    return sandbox_provider_config_from_mapping({**secret, "type": provider_type})


def fetch_sandbox_provider_config(
    secret_name: str,
    secret_store: SecretStore,
    provider_type: str,
) -> SandboxProviderConfig:
    """Resolve sandbox provider config from the selected provider type and secret."""
    return _sandbox_provider_config_from_secret(secret_store.get(secret_name), provider_type)


async def fetch_sandbox_provider_config_async(
    secret_name: str,
    secret_store: AsyncSecretStore,
    provider_type: str,
) -> SandboxProviderConfig:
    """Resolve sandbox provider config without blocking the caller's event loop."""
    return _sandbox_provider_config_from_secret(await secret_store.get_async(secret_name), provider_type)
