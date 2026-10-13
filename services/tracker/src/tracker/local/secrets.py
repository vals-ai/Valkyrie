"""Resolve only the credentials supplied for the current local execution."""

from collections.abc import Mapping
from pathlib import Path

from benchmark_service import SandboxProviderConfig
from benchmark_service.sandbox import DaytonaProviderConfig, ModalProviderConfig
from dotenv import dotenv_values

from tracker.exceptions import InvalidSandboxConfigurationError, SecretsError
from tracker.runtime.secrets import SecretValue, sandbox_provider_config_from_secret

PROVIDER_SETTING_NAMES = frozenset(
    name for config in (DaytonaProviderConfig, ModalProviderConfig) for name in config.model_fields if name.isupper()
)


class InMemorySecretStore:
    """Group transient environment values by the contract's existing references."""

    def __init__(self, references: Mapping[str, str], values: Mapping[str, str]) -> None:
        missing = references.keys() - values.keys()
        if missing:
            raise SecretsError(f"Local execution secrets missing keys: {', '.join(sorted(missing))}")
        self._values: dict[str, dict[str, SecretValue]] = {}
        for name, reference in references.items():
            self._values.setdefault(reference, {})[name] = values[name]

    async def get(self, name: str) -> SecretValue:
        try:
            return self._values[name]
        except KeyError:
            raise SecretsError(f"Local execution has no values for secret reference '{name}'") from None


def load_execution_secrets(path: Path | None, references: Mapping[str, str]) -> dict[str, str]:
    """Read only contract-declared values from the configured source file."""
    if not references or path is None:
        return {}
    if reserved := sorted(references.keys() & PROVIDER_SETTING_NAMES):
        raise SecretsError(f"Agent secrets cannot read sandbox provider settings: {', '.join(reserved)}")
    if not path.is_file():
        raise SecretsError("Configured local secrets file does not exist")
    values = dotenv_values(path, interpolate=False)
    return {name: value for name in references if (value := values.get(name)) is not None}


def load_sandbox_provider_config(path: Path | None, provider: str) -> SandboxProviderConfig:
    """Read the selected provider's settings from the configured source file."""
    if path is None:
        raise InvalidSandboxConfigurationError(f"Local {provider} execution requires a secrets_file")
    values: dict[str, SecretValue] = {
        name: value for name, value in dotenv_values(path, interpolate=False).items() if value is not None
    }
    return sandbox_provider_config_from_secret(values, provider)
