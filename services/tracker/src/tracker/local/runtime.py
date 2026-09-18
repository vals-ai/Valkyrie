"""Compose local runtime services from process configuration."""

from pathlib import Path
from uuid import UUID

from benchmark_service import DockerProviderConfig, SandboxProviderConfig

from tracker.local.logs import FilesystemLogs
from tracker.local.secrets import InMemorySecretStore
from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.services import RuntimeServices
from tracker.types import FinalViewResponse, StartBenchmarkRequest


class LocalRuntimeServices(RuntimeServices):
    """Local adapters with no AWS credential or resource resolution."""

    async def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Initialize the run log file."""
        await self.logs.create_benchmark(str(benchmark_id), retention_days=0)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        return DockerProviderConfig()

    async def run_completion_callback(self, final_view: FinalViewResponse) -> None:
        return None


class LocalRuntimeFactory:
    """Scope local storage by organization and credentials by operation."""

    @staticmethod
    def create_runtime(
        data_root: Path, org_id: UUID, *, secrets: InMemorySecretStore | None = None
    ) -> LocalRuntimeServices:
        root = data_root / "orgs" / str(org_id)
        secrets = secrets if secrets is not None else InMemorySecretStore({}, {})
        logs = FilesystemLogs(root / "logs")
        objects = FilesystemObjectStore(root / "objects")
        return LocalRuntimeServices(
            objects=objects,
            secrets=secrets,
            logs=logs,
            log_reader=logs,
            log_locations=logs,
            artifacts=objects,
            sandbox_provider="docker",
        )
