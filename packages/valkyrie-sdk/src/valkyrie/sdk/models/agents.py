"""Agent contract models used by SDK run requests."""

from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    Field,
    SerializerFunctionWrapHandler,
    ValidationError,
    create_model,
    field_validator,
    model_serializer,
    model_validator,
)

from valkyrie.sdk.agent_bundle import validate_agent_name
from valkyrie.sdk.errors import ValkyrieContractError
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


def _normalize_output_artifacts(value: list[OutputArtifactSpec]) -> list[OutputArtifactSpec]:
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


OutputArtifacts = Annotated[list[OutputArtifactSpec], AfterValidator(_normalize_output_artifacts)]
EgressPolicy = Literal["*"] | list[str]


class AgentContractRequest(BaseModel):
    """Agent definition submitted when starting a run."""

    name: str
    model: str | None = None
    install_cmd: str = ""
    run_cmd: str = ""
    final_output: str | None = None
    output_artifacts: OutputArtifacts = Field(default_factory=list)
    egress_allowlist: list[str] = Field(default_factory=list)
    install_egress: EgressPolicy | None = None
    secrets: dict[str, str] = Field(default_factory=dict)
    kwargs: dict[str, str] = Field(default_factory=dict)
    # Tracker-owned; cleared on every incoming request.
    inference_settings_attested: bool = False

    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        if isinstance(value, dict) and "egress" in value:
            raise ValueError("'egress' is no longer supported; use 'install_egress'")
        return value


_PARAMETER_TYPES: dict[str, type] = {"float": float, "int": int, "bool": bool, "str": str, "dict": dict}


class Parameter(BaseModel):
    """Represents one parameter an agent accepts, declared under `defaults` or `kwargs` in its contract."""

    type: Literal["float", "bool", "dict", "int", "str"] = Field(
        description='Type a passed value is converted to, so a temperature passed as "0.5" becomes the float 0.5.'
    )
    required: bool = Field(description="Whether a run must pass a value. A required parameter ignores its default.")
    default: float | bool | dict[str, Any] | int | str | None = Field(
        default=None, description="Value used when a run doesn't pass one."
    )
    choices: list[str] | None = Field(
        default=None, description="When set, the value must be one of these strings, and type isn't applied."
    )
    description: str | None = None


class AgentContract(BaseModel):
    """Represents an agent's `contract.yaml`, which says how to install and run the agent and which parameters it accepts.

    Use `from_directory()` to load and check one before publishing, and `validate_kwargs()` to check a
    run's `-k` values. Tracker checks the published contract again when a run starts.
    """

    name: str
    install_cmd: str
    final_output: Path | None = None
    output_artifacts: OutputArtifacts = Field(default_factory=list)
    egress_allowlist: list[str] = Field(default_factory=list)
    install_egress: EgressPolicy | None = None
    secrets: dict[str, str] = Field(default_factory=dict)
    ingest_lambda: str | None = None
    defaults: dict[str, Parameter] = Field(
        default_factory=dict, description="Parameters Valkyrie fills itself, such as the model from --model."
    )
    kwargs: dict[str, Parameter] = Field(
        default_factory=dict,
        description="The agent's own parameters, passed with -k. A name here overrides the same name in defaults.",
    )
    run_cmd: str

    @classmethod
    def from_directory(cls, agent_path: Path) -> "AgentContract":
        """Read `contract.yaml`, or else `contract.yml`, from an agent directory and validate it.

        Raises ValkyrieContractError when neither file exists, the YAML doesn't parse to a mapping,
        or a field is invalid (one line per field, prefixed with the contract's path).
        """
        contract_path = next(
            (agent_path / name for name in ("contract.yaml", "contract.yml") if (agent_path / name).is_file()),
            None,
        )
        if contract_path is None:
            raise ValkyrieContractError(f"No contract.yaml or contract.yml in {agent_path}")

        display_name = "/".join(contract_path.parts[-2:])
        try:
            data: object = yaml.safe_load(contract_path.read_text())
        except yaml.YAMLError as error:
            raise ValkyrieContractError(f"Failed to parse YAML contract from `{display_name}`:\n{error}") from error
        if not isinstance(data, dict):
            raise ValkyrieContractError(f"Invalid contract `{display_name}`: expected a mapping")

        try:
            return cls.model_validate(data)
        except ValidationError as error:
            raise ValkyrieContractError.from_validation_error(
                error, context=f"Invalid contract `{display_name}`"
            ) from error

    @model_validator(mode="before")
    @classmethod
    def reject_obsolete_egress(cls, value: Any) -> Any:
        if isinstance(value, dict) and "egress" in value:
            raise ValueError("'egress' is no longer supported; use 'install_egress'")
        return value

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return validate_agent_name(value)

    @field_validator("run_cmd")
    @classmethod
    def validate_run_cmd(cls, value: str) -> str:
        if "{problem_statement_path}" not in value:
            raise ValueError("run_cmd must contain {problem_statement_path}")
        return value

    def validate_kwargs(self, kwargs: Mapping[str, Any], *, model: str | None = None) -> dict[str, Any]:
        """Validate a run's kwargs against the contract and return them with defaults filled in.

        `model` is used for a `model` parameter the kwargs leave out, the way `--model` is at run
        start. Raises ValkyrieContractError listing each missing, mistyped or disallowed value.
        """
        schema = {**self.defaults, **self.kwargs}
        if not schema:
            return {}

        values = dict(kwargs)
        if model and "model" not in values:
            values["model"] = model

        fields: dict[str, Any] = {
            name: (
                Literal[tuple(parameter.choices)] if parameter.choices else _PARAMETER_TYPES[parameter.type],
                ... if parameter.required else parameter.default,
            )
            for name, parameter in schema.items()
        }
        try:
            validated = create_model("KwargsModel", **fields)(**values)
        except ValidationError as error:
            raise ValkyrieContractError.from_validation_error(error) from error

        return validated.model_dump(exclude_none=True)


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
