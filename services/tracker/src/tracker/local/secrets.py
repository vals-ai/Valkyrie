"""Resolve only the credentials supplied for the current local execution."""

from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

from tracker.exceptions import SecretsError
from tracker.runtime.secrets import SecretValue


class InMemorySecretStore:
    """Group transient environment values by the contract's existing references."""

    def __init__(self, references: Mapping[str, str], values: Mapping[str, str]) -> None:
        missing = references.keys() - values.keys()
        unexpected = values.keys() - references.keys()
        if missing or unexpected:
            details: list[str] = []
            if missing:
                details.append(f"missing keys: {', '.join(sorted(missing))}")
            if unexpected:
                details.append(f"undeclared keys: {', '.join(sorted(unexpected))}")
            raise SecretsError("Invalid local execution secrets; " + "; ".join(details))
        self._values: dict[str, dict[str, SecretValue]] = {}
        for name, reference in references.items():
            self._values.setdefault(reference, {})[name] = values[name]

    def get(self, name: str) -> SecretValue:
        try:
            return dict(self._values[name])
        except KeyError:
            raise SecretsError(f"Local execution has no values for secret reference '{name}'") from None

    async def get_async(self, name: str) -> SecretValue:
        return self.get(name)

    def close(self) -> None:
        self._values.clear()


def load_execution_secrets(path: Path | None, references: Mapping[str, str]) -> dict[str, str]:
    """Read only contract-declared values from the configured source file."""
    if not references or path is None:
        return {}
    if not path.is_file():
        raise SecretsError("Configured local secrets file does not exist")
    values = dotenv_values(path, interpolate=False)
    return {name: value for name in references if (value := values.get(name)) is not None}
