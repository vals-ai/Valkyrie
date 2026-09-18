"""Resolve only the credentials supplied for the current local execution."""

from collections.abc import Mapping

from tracker.exceptions import SecretsError
from tracker.runtime.secrets import SecretValue


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

    def close(self) -> None:
        self._values.clear()
