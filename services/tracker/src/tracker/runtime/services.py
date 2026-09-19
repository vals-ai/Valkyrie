"""Services shared by one API operation or executor execution."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from benchmark_service import SandboxProviderConfig

from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.runtime.logs import BenchmarkLogLocations, BenchmarkLogSink, LogProvider
from tracker.runtime.secrets import SecretStore, resolve_secrets, sandbox_provider_config_from_secret
from tracker.runtime.storage import ArtifactLocations, ObjectStore


if TYPE_CHECKING:
    from tracker.types import FinalViewResponse, StartBenchmarkRequest


@dataclass
class RuntimeServices(ABC):
    """Storage, secrets, logs, and execution-scoped sandbox access."""

    objects: ObjectStore
    secrets: SecretStore
    logs: BenchmarkLogSink
    log_reader: LogProvider
    log_locations: BenchmarkLogLocations
    artifacts: ArtifactLocations
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None

    @abstractmethod
    async def prepare_execution(self, request: "StartBenchmarkRequest", benchmark_id: UUID) -> None:
        """Prepare backend resources before sandbox work."""
        raise NotImplementedError

    @abstractmethod
    async def run_completion_callback(self, final_view: "FinalViewResponse") -> None:
        """Run the configured completion callback after results are stored."""
        raise NotImplementedError

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        """Resolve provider credentials only when sandbox access is requested."""
        if not self.sandbox_provider_secret_name:
            raise InvalidSandboxConfigurationError("Sandbox access requires a provider secret name")

        return await self._load_sandbox_provider_config(self.sandbox_provider_secret_name)

    async def _load_sandbox_provider_config(self, secret_name: str) -> SandboxProviderConfig:
        secret = await self.secrets.get(secret_name)
        return sandbox_provider_config_from_secret(secret, self.sandbox_provider)

    async def resolve_secrets(self, references: dict[str, str]) -> dict[str, str]:
        """Resolve agent environment values without blocking execution."""
        return await resolve_secrets(references, self.secrets)
