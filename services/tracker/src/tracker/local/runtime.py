"""Compose local runtime services from process configuration."""

from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
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

    def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Initialize the run log file."""
        self.logs.create_benchmark(str(benchmark_id), retention_days=0)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        return DockerProviderConfig()

    async def run_completion_callback(self, final_view: FinalViewResponse) -> None:
        return None


class LocalRuntimeFactory:
    """Scope local storage by organization and credentials by operation."""

    @staticmethod
    @asynccontextmanager
    async def open(
        data_root: Path,
        org_id: UUID,
        *,
        secret_references: Mapping[str, str] | None = None,
        execution_secrets: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[LocalRuntimeServices]:
        root = data_root / "orgs" / str(org_id)
        secrets = InMemorySecretStore(secret_references or {}, execution_secrets or {})
        logs = FilesystemLogs(root / "logs")
        objects = FilesystemObjectStore(root / "objects")
        try:
            yield LocalRuntimeServices(
                objects=objects,
                secrets=secrets,
                async_secrets=secrets,
                logs=logs,
                log_reader=logs,
                log_locations=logs,
                artifacts=objects,
                sandbox_provider="docker",
            )
        finally:
            secrets.close()
