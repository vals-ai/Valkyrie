"""Provider-neutral secret reads and reference resolution."""

from typing import Protocol, TypeAlias

from benchmark_service import SandboxProviderConfig, sandbox_provider_config_from_mapping

from tracker.exceptions import InvalidSandboxConfigurationError, SecretsError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SecretValue: TypeAlias = JsonValue


def sandbox_provider_config_from_secret(secret: SecretValue, provider_type: str) -> SandboxProviderConfig:
    """Validate a provider secret and apply the selected provider type."""
    if not isinstance(secret, dict):
        raise InvalidSandboxConfigurationError("Expected sandbox provider secret to be a JSON object")
    return sandbox_provider_config_from_mapping({**secret, "type": provider_type})


class SecretStore(Protocol):
    """Asynchronous access to named secret values."""

    async def get(self, name: str) -> SecretValue:
        """Return decoded JSON, or the raw string when the value is not JSON."""
        raise NotImplementedError


async def resolve_secrets(secrets: dict[str, str], secret_store: SecretStore) -> dict[str, str]:
    """Resolve environment-variable secret references to their current values."""
    resolved: dict[str, str] = {}
    for env_name, secret_name in secrets.items():
        secret_value = await secret_store.get(secret_name)
        if isinstance(secret_value, dict):
            if env_name not in secret_value:
                raise SecretsError(f"Key '{env_name}' not found in JSON secret '{secret_name}'")
            resolved[env_name] = str(secret_value[env_name])
        else:
            resolved[env_name] = str(secret_value)
    return resolved
