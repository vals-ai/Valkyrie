"""Agent contract models used by SDK run requests."""

from pathlib import PurePosixPath
from typing import Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    field_validator,
    model_serializer,
    model_validator,
)

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


ModelGatewayConfigValue = bool | float | int | str


class ModelGatewayPolicyConfig(BaseModel):
    """Closed model settings allowed in a task-scoped gateway capability."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    client_scope: Literal["shared"]
    max_tokens: int | None = Field(default=None, gt=0)
    temperature: FiniteFloat | None = None
    top_p: FiniteFloat | None = None
    top_k: int | None = Field(default=None, gt=0)
    reasoning: bool | None = None
    reasoning_effort: str | bool | None = None
    compute_effort: str | int | None = None

    @model_serializer(mode="plain")
    def serialize_config(self) -> dict[str, ModelGatewayConfigValue]:
        """Preserve only the immutable settings authored in the contract row."""
        return {
            name: cast(ModelGatewayConfigValue, getattr(self, name))
            for name in type(self).model_fields
            if name in self.model_fields_set and getattr(self, name) is not None
        }


class ModelGatewayTaskCapabilityPolicy(BaseModel):
    """One selected model policy carried on a resolved agent contract."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["task_capability"]
    model: str
    config: ModelGatewayPolicyConfig
    max_queries: int = Field(ge=1, le=2000)
    max_sessions: int = Field(ge=1, le=16)

    @field_validator("model")
    @classmethod
    def validate_model(cls, value: str) -> str:
        """Require the exact non-blank registry key selected by the contract."""
        if not value or value != value.strip():
            raise ValueError("model must be a non-blank exact model key")
        return value


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
    model_gateway_policy: ModelGatewayTaskCapabilityPolicy | None = None

    @field_validator("output_artifacts")
    @classmethod
    def validate_output_artifacts(cls, value: list[OutputArtifactSpec]) -> list[OutputArtifactSpec]:
        """Validate and normalize artifact destination paths."""
        if len(value) > MAX_OUTPUT_ARTIFACT_COUNT:
            raise ValueError(f"output_artifacts cannot contain more than {MAX_OUTPUT_ARTIFACT_COUNT} entries")

        normalized_artifacts: list[OutputArtifactSpec] = []
        for artifact in value:
            artifact_path = artifact if isinstance(artifact, str) else artifact.path
            path = PurePosixPath(artifact_path)
            if path.is_absolute():
                raise ValueError("output_artifacts paths must be relative paths")
            if not path.parts or ".." in path.parts or "." in path.parts:
                raise ValueError("output_artifacts paths cannot contain empty, '.', or '..' path parts")
            normalized_artifacts.append(
                str(path) if isinstance(artifact, str) else artifact.model_copy(update={"path": str(path)})
            )
        return normalized_artifacts

    @model_validator(mode="after")
    def validate_model_gateway_policy(self) -> "AgentContractRequest":
        """Bind the selected capability policy to the request's exact model."""
        if self.model_gateway_policy is not None and self.model_gateway_policy.model != self.model:
            raise ValueError("model_gateway_policy.model must exactly match contract.model")
        return self
