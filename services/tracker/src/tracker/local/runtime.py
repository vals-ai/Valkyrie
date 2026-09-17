"""Compose local runtime services from process configuration."""

import os
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from benchmark_service import DockerProviderConfig, SandboxProviderConfig
from pydantic import BaseModel, ConfigDict, Field, field_validator

from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.local.logs import FilesystemLogs
from tracker.local.secrets import InMemorySecretStore
from tracker.local.storage import FilesystemArtifactLocations, FilesystemObjectStore
from tracker.runtime.services import RuntimeServices
from tracker.types import FinalViewResponse, StartBenchmarkRequest


class LocalRuntimeConfig(BaseModel):
    """Trusted paths and Docker settings supplied by the local deployment."""

    model_config = ConfigDict(frozen=True)

    data_root: Path
    host_data_root: Path
    docker: DockerProviderConfig = Field(default_factory=DockerProviderConfig)

    @field_validator("data_root", "host_data_root")
    @classmethod
    def validate_root(cls, value: Path) -> Path:
        if not value.is_absolute() or ".." in value.parts:
            raise ValueError("Local data roots must be absolute paths without traversal")
        return value

    @classmethod
    def from_env(cls) -> "LocalRuntimeConfig":
        """Keep host and container paths explicit when composing local services."""
        return cls(
            data_root=Path(os.environ["VALKYRIE_LOCAL_DATA_ROOT"]),
            host_data_root=Path(os.environ["VALKYRIE_LOCAL_HOST_DATA_ROOT"]),
            docker=DockerProviderConfig.from_env(),
        )


@dataclass(kw_only=True)
class LocalRuntimeServices(RuntimeServices):
    """Local adapters with no AWS credential or resource resolution."""

    config: LocalRuntimeConfig

    def prepare_execution(self, request: StartBenchmarkRequest, benchmark_id: UUID) -> None:
        """Reject cloud-only options and initialize persistent logs for this run."""
        if request.sandbox_provider != "docker":
            raise InvalidSandboxConfigurationError("Local execution requires the Docker sandbox provider")
        if any(
            (
                request.properties,
                request.harness_config,
                request.lambda_function,
                request.sandbox_provider_secret_name,
                request.service_auth_secret_name,
                request.webhook_secret_name,
                request.service_headers,
            )
        ):
            raise InvalidSandboxConfigurationError(
                "Local execution does not accept AWS configuration, stored secret references, or service credentials"
            )
        self.logs.create_benchmark(str(benchmark_id), retention_days=0)

    async def get_sandbox_provider_config(self) -> SandboxProviderConfig:
        return self.config.docker

    async def run_completion_callback(self, final_view: FinalViewResponse) -> None:
        if final_view.benchmark_arguments.lambda_function:
            raise InvalidSandboxConfigurationError("Local execution does not support Lambda callbacks")


class LocalRuntimeFactory:
    """Scope local storage by organization and credentials by operation."""

    @staticmethod
    @asynccontextmanager
    async def open(
        config: LocalRuntimeConfig,
        org_id: UUID,
        *,
        secret_references: Mapping[str, str] | None = None,
        execution_secrets: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[LocalRuntimeServices]:
        root = config.data_root / "orgs" / str(org_id)
        host_root = config.host_data_root / "orgs" / str(org_id)
        secrets = InMemorySecretStore(secret_references or {}, execution_secrets or {})
        logs = FilesystemLogs(root / "logs", host_root / "logs")
        try:
            yield LocalRuntimeServices(
                config=config,
                objects=FilesystemObjectStore(root / "objects"),
                secrets=secrets,
                async_secrets=secrets,
                logs=logs,
                log_reader=logs,
                log_locations=logs,
                artifacts=FilesystemArtifactLocations(root / "objects", host_root / "objects"),
                sandbox_provider="docker",
            )
        finally:
            secrets.close()
