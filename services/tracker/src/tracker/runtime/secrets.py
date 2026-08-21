"""Provider-neutral secret reads and reference resolution."""

from typing import Protocol, TypeAlias

from tracker.exceptions import SecretsError


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SecretValue: TypeAlias = JsonValue


class SecretStore(Protocol):
    """Synchronous access to named secret values."""

    def get(self, name: str) -> SecretValue:
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
