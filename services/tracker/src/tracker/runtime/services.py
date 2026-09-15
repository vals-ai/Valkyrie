"""Services shared by one API operation or executor execution."""

from asyncio import Lock
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from benchmark_service import SandboxProvider, SandboxProviderConfig

from tracker.runtime.logs import BenchmarkLogLocations, BenchmarkLogSink, LogProvider
from tracker.runtime.secrets import AsyncSecretStore, SecretStore
from tracker.runtime.storage import ArtifactLocations, ObjectStore


@dataclass
class RuntimeServices:
    """Storage, secrets, logs, and lazily constructed sandbox access."""

    objects: ObjectStore
    secrets: SecretStore
    async_secrets: AsyncSecretStore
    logs: BenchmarkLogSink
    log_reader: LogProvider
    log_locations: BenchmarkLogLocations
    artifacts: ArtifactLocations
    _load_sandbox_config: Callable[[], Awaitable[SandboxProviderConfig]] = field(repr=False)
    _sandbox_config: SandboxProviderConfig | None = field(default=None, init=False, repr=False)
    _sandbox_provider: SandboxProvider | None = field(default=None, init=False, repr=False)

    _config_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        """Resolve provider credentials only when sandbox access is requested."""
        async with self._config_lock:
            if self._sandbox_config is None:
                self._sandbox_config = await self._load_sandbox_config()
            return self._sandbox_config

    async def get_sandbox_provider(self) -> SandboxProvider:
        """Reuse one provider for this runtime's lifetime."""
        config = await self.get_sandbox_provider_config()
        if self._sandbox_provider is None:
            self._sandbox_provider = config.create_provider()
        return self._sandbox_provider

    async def close(self) -> None:
        """Release any provider created during this operation."""
        try:
            if self._sandbox_provider is not None:
                await self._sandbox_provider.close()
        finally:
            self._sandbox_provider = None
            self._sandbox_config = None
