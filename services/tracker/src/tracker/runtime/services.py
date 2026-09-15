"""Services shared by one API operation or executor execution."""

from asyncio import Lock, Task, create_task, to_thread
from dataclasses import dataclass, field

from benchmark_service import SandboxProvider, SandboxProviderConfig

from tracker.exceptions import InvalidSandboxConfigurationError, TrackerServiceError
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.logs import BenchmarkLogLocations, BenchmarkLogSink, LogProvider
from tracker.runtime.secrets import AsyncSecretStore, SecretStore, fetch_sandbox_provider_config_async, resolve_secrets
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
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None

    _sandbox_config: SandboxProviderConfig | None = field(default=None, init=False, repr=False)
    _sandbox_provider: SandboxProvider | None = field(default=None, init=False, repr=False)
    _config_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _close_task: Task[None] | None = field(default=None, init=False, repr=False)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        """Resolve provider credentials only when sandbox access is requested."""
        if not self.sandbox_provider_secret_name:
            raise InvalidSandboxConfigurationError("Sandbox access requires a provider secret name")

        async with self._config_lock:
            self._require_open()
            if self._sandbox_config is None:
                self._sandbox_config = await fetch_sandbox_provider_config_async(
                    self.sandbox_provider_secret_name, self.async_secrets, self.sandbox_provider
                )
            self._require_open()
            return self._sandbox_config

    async def get_sandbox_provider(self) -> SandboxProvider:
        """Reuse one provider for this runtime's lifetime."""
        config = await self.get_sandbox_provider_config()
        if self._sandbox_provider is None:
            self._sandbox_provider = config.create_provider()

        return self._sandbox_provider

    async def resolve_secrets(self, references: dict[str, str]) -> dict[str, str]:
        """Resolve agent environment values without blocking execution."""
        return await to_thread(resolve_secrets, references, self.secrets)

    def _require_open(self) -> None:
        if self._closed:
            raise TrackerServiceError("Runtime services are closed")

    async def close(self) -> None:
        """Finish one shutdown even if the caller is cancelled."""
        if self._close_task is None:
            self._closed = True
            self._close_task = create_task(self._close())

        await finish_cleanup(self._close_task)

    async def _close(self) -> None:
        async with self._config_lock:
            provider = self._sandbox_provider
            self._sandbox_provider = None
            self._sandbox_config = None

        if provider is not None:
            await provider.close()
