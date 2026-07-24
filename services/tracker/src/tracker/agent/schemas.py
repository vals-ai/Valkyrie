"""Agent contract schemas (declarative YAML contract + AgentConfig)."""

import hashlib
import re
import shlex
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ValidationError, create_model, field_validator

from tracker.database.models import OutputArtifact, OutputArtifactSpec
from tracker.exceptions import ContractValidationError


__all__ = [
    "AgentConfig",
    "AgentContract",
    "OutputArtifact",
    "OutputArtifactSpec",
    "Parameter",
    "bind_shell_variables",
    "prepare_shell_command",
    "validate_agent_name",
]


_AGENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_SHELLS = {"ash", "bash", "dash", "ksh", "sh", "zsh"}
_SHELL_OPERATORS = {"&", "&&", "(", ")", ";", ";;", "<", ">", "|", "||"}


def validate_agent_name(name: str) -> str:
    """Validate an agent name before it is used as an S3 key and bundle folder name."""
    if not name or not _AGENT_NAME_PATTERN.match(name) or name in {".", ".."}:
        raise ValueError(f"Invalid agent name {name!r}: use only letters, digits, dots, dashes, or underscores.")
    return name


def _shell_variable_name(name: str) -> str:
    digest = hashlib.sha256(name.encode()).hexdigest().upper()
    return f"VALKYRIE_ARG_{digest}"


def _validate_shell_template(command: str) -> None:
    if "$(" in command or "`" in command:
        raise ValueError("run_cmd cannot use command substitution with dynamic arguments")

    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()<>")
    lexer.commenters = "#"
    lexer.whitespace_split = True
    try:
        tokens = list(lexer)
    except ValueError as exc:
        raise ValueError("run_cmd must be valid shell syntax") from exc

    if any(token.startswith("<<") for token in tokens):
        raise ValueError("run_cmd cannot use heredocs with dynamic arguments")
    if "eval" in tokens:
        raise ValueError("run_cmd cannot evaluate dynamic arguments")

    for index, token in enumerate(tokens):
        if Path(token).name not in _SHELLS:
            continue
        for argument in tokens[index + 1 :]:
            if argument in _SHELL_OPERATORS:
                break
            if argument.startswith("-") and "c" in argument.removeprefix("-"):
                raise ValueError("run_cmd cannot pass dynamic arguments to a shell command string")


def prepare_shell_command(command: str, parameter_names: Iterable[str]) -> str:
    names = tuple(dict.fromkeys(parameter_names))
    if not any(f"{{{name}}}" in command for name in names):
        return command

    _validate_shell_template(command)
    for name in names:
        placeholder = f"{{{name}}}"
        for match in re.finditer(re.escape(placeholder), command):
            before = command[match.start() - 1] if match.start() else ""
            after = command[match.end()] if match.end() < len(command) else ""
            if (before and not before.isspace()) or (after and not after.isspace()):
                raise ValueError(f"{placeholder} must be a standalone shell argument")
        command = command.replace(placeholder, f'"${{{_shell_variable_name(name)}}}"')
    return command


def bind_shell_variables(command: str, values: Mapping[str, object]) -> str:
    if not values:
        return command

    statements: list[str] = []
    for name, value in values.items():
        variable = _shell_variable_name(name)
        if f"${{{variable}}}" not in command:
            continue
        string_value = str(value)
        if "\0" in string_value:
            raise ValueError(f"{{{name}}} cannot contain a null byte")
        statements.extend((f"{variable}={shlex.quote(string_value)}", f"export {variable}"))
    return "; ".join((*statements, command))


class Defaults(str, Enum):
    """Values auto injected into users run cmd if specified"""

    PROBLEM_STATEMENT_PATH = "problem_statement_path"
    TASK_ID = "task_id"


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str | None = None
    """Model key (e.g., openai/gpt-4o)"""

    kwargs: dict[str, Any] = {}
    """Additional arguments to pass into the agent"""


ParameterType = Literal["float", "bool", "dict", "int", "str"]


class Parameter(BaseModel):
    type: ParameterType
    required: bool
    default: float | bool | dict[str, Any] | int | str | None = None
    choices: list[str] | None = None
    description: str | None = None


class AgentContract(BaseModel):
    """Declarative YAML agent contract."""

    name: str
    install_cmd: str
    final_output: Path | None = None
    output_artifacts: list[OutputArtifactSpec] = []
    egress_allowlist: list[str] = []
    secrets: dict[str, str] = {}
    ingest_lambda: str | None = None
    defaults: dict[str, Parameter] = {}
    kwargs: dict[str, Parameter] = {}
    run_cmd: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return validate_agent_name(v)

    @field_validator("run_cmd")
    @classmethod
    def validate_run_cmd(cls, v: str) -> str:
        if f"{{{Defaults.PROBLEM_STATEMENT_PATH.value}}}" not in v:
            raise ValueError(f"run_cmd must contain {{{Defaults.PROBLEM_STATEMENT_PATH.value}}}")

        return v

    def format_run_cmd(self, kwargs: dict[str, Any]) -> str:
        parameter_names = (
            *(default.value for default in Defaults),
            *self.defaults,
            *self.kwargs,
        )
        command = prepare_shell_command(self.run_cmd, parameter_names)
        return bind_shell_variables(command, kwargs)

    def validate_kwargs(self, schema: dict[str, Parameter], values: dict[str, Any]) -> dict[str, Any]:
        if not schema:
            return {}

        type_map = {"float": float, "int": int, "bool": bool, "str": str, "dict": dict}

        fields: dict[Any, Any] = {}
        for name, param in schema.items():
            if param.choices:
                python_type = Literal[tuple(param.choices)]  # type: ignore[valid-type]
            else:
                python_type = type_map[param.type]
            default = param.default if not param.required else ...
            fields[name] = (python_type, default)

        DynamicModel = create_model("KwargsModel", **fields)

        try:
            validated = DynamicModel(**values)
        except ValidationError as e:
            raise ContractValidationError(e) from e

        return validated.model_dump(exclude_none=True)
