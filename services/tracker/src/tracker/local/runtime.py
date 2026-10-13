"""Compose local runtime services from process configuration."""

from asyncio import to_thread
from dataclasses import dataclass
from uuid import UUID

from benchmark_service import DockerProviderConfig, SandboxProviderConfig

from tracker.aws.clients import AWSClientProvider
from tracker.aws.secrets import SecretsManagerStore
from tracker.local.logs import FilesystemLogs
from tracker.local.resources import LocalResources
from tracker.local.secrets import InMemorySecretStore, load_sandbox_provider_config
from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.secrets import SecretStore
from tracker.runtime.services import RuntimeServices
from tracker.types import FinalViewResponse, StartBenchmarkRequest


@dataclass(kw_only=True)
class LocalRuntimeServices(RuntimeServices):
    """Filesystem storage and logs with secrets from a local file or AWS Secrets Manager."""

    resources: LocalResources

    async def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Initialize the run log file."""
        await self.logs.create_benchmark(str(benchmark_id), retention_days=0)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        if self.sandbox_provider == "docker":
            return DockerProviderConfig()
        if self.resources.secrets_manager_region is not None:
            return await super().get_sandbox_provider_config()
        return await to_thread(load_sandbox_provider_config, self.resources.secrets_file, self.sandbox_provider)

    async def run_completion_callback(self, final_view: FinalViewResponse) -> None:
        return None


class LocalRuntimeFactory:
    """Scope local storage by organization and credentials by operation."""

    @staticmethod
    def create_runtime(
        resources: LocalResources,
        org_id: UUID,
        *,
        secrets: SecretStore | None = None,
        sandbox_provider: str = "docker",
        sandbox_provider_secret_name: str | None = None,
    ) -> LocalRuntimeServices:
        root = resources.data_root / "orgs" / str(org_id)
        if secrets is None:
            region = resources.secrets_manager_region
            secrets = InMemorySecretStore({}, {}) if region is None else SecretsManagerStore(AWSClientProvider(region))
        logs = FilesystemLogs(root / "logs")
        objects = FilesystemObjectStore(root / "objects", root / "staging")
        return LocalRuntimeServices(
            objects=objects,
            secrets=secrets,
            logs=logs,
            log_reader=logs,
            log_locations=logs,
            artifacts=objects,
            sandbox_provider=sandbox_provider,
            sandbox_provider_secret_name=sandbox_provider_secret_name,
            resources=resources,
        )
