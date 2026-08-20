"""Agent contract models used by SDK run requests."""

from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel, Field, SerializerFunctionWrapHandler, field_validator, model_serializer

from valkyrie.sdk.models._base import ResponseModel

MAX_OUTPUT_ARTIFACT_COUNT = 10


def _source_has_glob(source: str) -> bool:
    return any(char in source for char in "*?[")


def _source_glob_root(source: str) -> str:
    glob_indices = [source.find(char) for char in "*?[" if source.find(char) != -1]
    first_glob_index = min(glob_indices)
    return source[:first_glob_index].rsplit("/", 1)[0] or "/"


class OutputArtifact(BaseModel):
    """One artifact copied from a sandbox after an agent run."""

    path: str
    source: str | None = None
    required: bool = True

    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.required:
            data.pop("required", None)
        return data

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str | None) -> str | None:
        """Validate an optional absolute sandbox source path."""
        if not value:
            return None
        if not value.startswith("/"):
            raise ValueError("output_artifacts source paths must be absolute sandbox paths")

        path = PurePosixPath(value)
        if not path.parts or ".." in path.parts or "." in path.parts:
            raise ValueError("output_artifacts source paths cannot contain empty, '.', or '..' path parts")
        if _source_has_glob(value) and _source_glob_root(value) == "/":
            raise ValueError("output_artifacts glob sources must include a non-root directory prefix")
        return value


OutputArtifactSpec = str | OutputArtifact


class AgentContractRequest(BaseModel):
    """Agent definition submitted when starting a run."""

    name: str
    model: str | None = None
    install_cmd: str = ""
    run_cmd: str = ""
    final_output: str | None = None
    output_artifacts: list[OutputArtifactSpec] = Field(default_factory=list)
    egress_allowlist: list[str] = Field(default_factory=list)
    secrets: dict[str, str] = Field(default_factory=dict)
    kwargs: dict[str, str] = Field(default_factory=dict)
    # Tracker-owned; cleared on every incoming request.
    inference_settings_attested: bool = False

    @field_validator("output_artifacts")
    @classmethod
    def validate_output_artifacts(cls, value: list[OutputArtifactSpec]) -> list[OutputArtifactSpec]:
        """Validate and normalize artifact destination paths."""
        if len(value) > MAX_OUTPUT_ARTIFACT_COUNT:
            raise ValueError(f"output_artifacts cannot contain more than {MAX_OUTPUT_ARTIFACT_COUNT} entries")

        normalized_artifacts: list[OutputArtifactSpec] = []
        normalized_paths: set[str] = set()
        for artifact in value:
            artifact_path = artifact if isinstance(artifact, str) else artifact.path
            path = PurePosixPath(artifact_path)
            if path.is_absolute():
                raise ValueError("output_artifacts paths must be relative paths")
            if not path.parts or ".." in path.parts or "." in path.parts:
                raise ValueError("output_artifacts paths cannot contain empty, '.', or '..' path parts")
            normalized_path = str(path)
            if normalized_path in normalized_paths:
                raise ValueError(f"output_artifacts cannot contain duplicate paths: {normalized_path}")
            normalized_paths.add(normalized_path)
            normalized_artifacts.append(
                normalized_path if isinstance(artifact, str) else artifact.model_copy(update={"path": normalized_path})
            )
        return normalized_artifacts


class AgentEntry(ResponseModel):
    """One uploaded agent visible to the configured tenant."""

    name: str
    last_modified: str | None = None


class AgentsResponse(ResponseModel):
    """Uploaded agents visible to the configured tenant."""

    agents: list[AgentEntry]


class AgentDownloadURLResponse(ResponseModel):
    """Temporary URL for downloading an uploaded agent archive."""

    name: str
    download_url: str
    expires_in: int
