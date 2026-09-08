"""Reflect the public Click and SDK surfaces into stable documentation records."""

from __future__ import annotations

import inspect
import re
import typing
from collections.abc import Callable, Sequence
from enum import Enum
from pathlib import Path
from typing import Any

import click
from pydantic import BaseModel

from .model import (
    CLICommandReference,
    CLIParameterReference,
    SDKEnumReference,
    SDKExceptionReference,
    SDKFieldReference,
    SDKMethodReference,
    SDKModelReference,
    SDKParameterReference,
    SDKReference,
    SDKResourceReference,
)


def _clean_docstring(value: object) -> str:
    return inspect.cleandoc(value) if isinstance(value, str) else ""


def _format_value(value: object) -> str:
    if isinstance(value, Enum):
        return f"{type(value).__name__}.{value.name}"
    if isinstance(value, Path):
        return repr(str(value))
    if isinstance(value, type):
        return value.__name__
    if value is None or isinstance(value, (bool, str, int, float, list, tuple, dict)):
        return repr(typing.cast(object, value))
    raise TypeError(f"Unsupported default value of type {type(value).__name__!r}: {value!r}")


def _format_annotation(annotation: object) -> str:
    if annotation is inspect.Parameter.empty:
        return "Any"
    if annotation is None or annotation is type(None):
        return "None"
    if isinstance(annotation, str):
        return annotation

    formatted = inspect.formatannotation(annotation).replace("typing.", "")
    return re.sub(r"\b(?:[A-Za-z_]\w*\.)+([A-Za-z_]\w*)\b", r"\1", formatted)


def _sanitize_public_text(value: str) -> str:
    value = re.sub(r"(-s\s+ANTHROPIC_API_KEY)\s+[A-Za-z][A-Za-z0-9_-]+", r"\1 AnthropicApiKey", value)
    return re.sub(r"\b([A-Za-z0-9._%+-]+)@vals\.ai\b", r"\1@example.com", value)


def _format_click_type(parameter_type: click.ParamType) -> tuple[str, tuple[str, ...]]:
    constraints: list[str] = []
    if isinstance(parameter_type, click.Tuple):
        item_types = ", ".join(_format_click_type(item)[0] for item in parameter_type.types)
        return f"tuple[{item_types}]", ()
    if isinstance(parameter_type, click.Choice):
        choices = tuple(str(choice) for choice in typing.cast(Sequence[object], parameter_type.choices))
        constraints.append("one of " + ", ".join(f"`{choice}`" for choice in choices))
        if not parameter_type.case_sensitive:
            constraints.append("case-insensitive")
        return "choice", tuple(constraints)
    if isinstance(parameter_type, (click.IntRange, click.FloatRange)):
        for attribute, label in (("min", ">="), ("max", "<=")):
            if (bound := getattr(parameter_type, attribute)) is not None:
                constraints.append(f"{label} {bound}")
        if parameter_type.clamp:
            constraints.append("clamped")
        type_name = "integer" if isinstance(parameter_type, click.IntRange) else "number"
        return type_name, tuple(constraints)
    if isinstance(parameter_type, click.Path):
        if parameter_type.exists:
            constraints.append("must exist")
        if parameter_type.file_okay != parameter_type.dir_okay:
            constraints.append("file" if parameter_type.file_okay else "directory")
        if parameter_type.readable:
            constraints.append("readable")
        if parameter_type.writable:
            constraints.append("writable")
        return "path", tuple(constraints)
    return parameter_type.name.replace(" range", ""), ()


def _collect_click_parameter(parameter: click.Parameter) -> CLIParameterReference:
    type_name, type_constraints = _format_click_type(parameter.type)
    constraints = list(type_constraints)
    if parameter.nargs != 1:
        constraints.append(f"{parameter.nargs} values")
    if parameter.multiple:
        constraints.append("repeatable")

    is_option = isinstance(parameter, click.Option)
    if is_option:
        declarations = tuple((*parameter.opts, *parameter.secondary_opts))
    else:
        declarations = (parameter.human_readable_name.upper(),)
    has_click_default = isinstance(parameter.default, Enum) and type(parameter.default).__module__.startswith("click.")

    return CLIParameterReference(
        name=parameter.name or declarations[0].lower(),
        kind="option" if is_option else "argument",
        declarations=declarations,
        type_name=type_name,
        required=parameter.required,
        default=None if has_click_default else _format_value(parameter.default),
        multiple=parameter.multiple,
        nargs=parameter.nargs,
        metavar=parameter.metavar,
        constraints=tuple(constraints),
        help=_sanitize_public_text(parameter.help or "") if is_option else "",
    )


def collect_cli_commands() -> tuple[CLICommandReference, ...]:
    """Collect registered leaf commands without invoking callbacks."""
    from valkyrie.cli.main import cli

    commands: list[CLICommandReference] = []

    def visit(command: click.Command, path: tuple[str, ...]) -> None:
        if command.hidden:
            return
        if isinstance(command, click.Group):
            for name, child in command.commands.items():
                visit(child, (*path, name))
            return

        help_text = _sanitize_public_text(_clean_docstring(command.help))
        example_match = re.search(r"\n\nExample:\s*(.+)$", help_text, flags=re.DOTALL)
        summary = help_text[: example_match.start()].strip() if example_match else help_text.strip()
        example = None
        if example_match:
            example = "\n".join(line.strip() for line in example_match.group(1).strip().splitlines())
        parameters = tuple(
            _collect_click_parameter(parameter)
            for parameter in command.params
            if not (isinstance(parameter, click.Option) and parameter.hidden)
        )
        commands.append(CLICommandReference(path=path, summary=summary, example=example, parameters=parameters))

    visit(cli, ())
    return tuple(commands)


def _format_callable_parameter(parameter: inspect.Parameter) -> str:
    if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
        prefix = "*"
    elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
        prefix = "**"
    else:
        prefix = ""

    result = prefix + parameter.name
    if parameter.annotation is not inspect.Parameter.empty:
        result += f": {_format_annotation(parameter.annotation)}"
    if parameter.default is not inspect.Parameter.empty:
        result += f" = {_format_value(parameter.default)}"
    return result


def _format_signature(name: str, function: Callable[..., object]) -> str:
    signature = inspect.signature(function, eval_str=False)
    parameters = list(signature.parameters.values())
    if parameters and parameters[0].name in {"self", "cls"}:
        parameters.pop(0)

    rendered_parameters: list[str] = []
    has_keyword_marker = False
    for index, parameter in enumerate(parameters):
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and not has_keyword_marker:
            rendered_parameters.append("*")
            has_keyword_marker = True
        rendered_parameters.append(_format_callable_parameter(parameter))
        has_keyword_marker |= parameter.kind is inspect.Parameter.VAR_POSITIONAL
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY and (
            index + 1 == len(parameters) or parameters[index + 1].kind is not inspect.Parameter.POSITIONAL_ONLY
        ):
            rendered_parameters.append("/")

    prefix = "async " if inspect.iscoroutinefunction(function) or inspect.isasyncgenfunction(function) else ""
    return_type = _format_annotation(signature.return_annotation)
    one_line = f"{prefix}def {name}({', '.join(rendered_parameters)}) -> {return_type}"
    if len(one_line) <= 110:
        return one_line
    body = "\n".join(f"    {parameter}," for parameter in rendered_parameters)
    return f"{prefix}def {name}(\n{body}\n) -> {return_type}"


def _collect_method(name: str, function: Callable[..., object]) -> SDKMethodReference:
    parameters = list(inspect.signature(function, eval_str=False).parameters.values())
    if parameters and parameters[0].name in {"self", "cls"}:
        parameters.pop(0)
    overloads = typing.get_overloads(function) or (function,)
    documented_parameters = tuple(
        SDKParameterReference(
            name=parameter.name,
            type_name=_format_annotation(parameter.annotation),
            required=parameter.default is inspect.Parameter.empty,
            default=None if parameter.default is inspect.Parameter.empty else _format_value(parameter.default),
        )
        for parameter in parameters
    )
    return SDKMethodReference(
        name=name,
        description=_clean_docstring(inspect.getdoc(function)),
        signatures=tuple(_format_signature(name, overload) for overload in overloads),
        parameters=documented_parameters,
        return_types=tuple(
            _format_annotation(inspect.signature(overload, eval_str=False).return_annotation) for overload in overloads
        ),
    )


def _type_slug(name: str) -> str:
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1-\2", name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", value)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower()
    if not slug or slug in {".", ".."} or "/" in slug or "\\" in slug:
        raise ValueError(f"Unsafe public type slug for {name!r}: {slug!r}")
    return slug


def _family(value: type[object]) -> str:
    return value.__module__.rsplit(".", 1)[-1].replace("_", " ").title()


def _field_default(field: Any) -> str | None:
    if field.is_required():
        return None
    if field.default_factory is None:
        return _format_value(field.default)
    if field.default_factory in {list, dict, set}:
        return {list: "[]", dict: "{}", set: "set()"}[field.default_factory]
    factory_name = getattr(field.default_factory, "__name__", "generated value")
    return "generated value" if factory_name == "<lambda>" else f"{factory_name}()"


def _field_constraints(metadata: Sequence[object]) -> tuple[str, ...]:
    labels = (
        ("gt", ">"),
        ("ge", ">="),
        ("lt", "<"),
        ("le", "<="),
        ("multiple_of", "multiple of"),
        ("min_length", "minimum length"),
        ("max_length", "maximum length"),
        ("pattern", "pattern"),
    )
    constraints: list[str] = []
    for item in metadata:
        for attribute, label in labels:
            if (value := getattr(item, attribute, None)) is not None:
                constraints.append(f"{label} {_format_value(value)}")
    return tuple(constraints)


def _collect_model(model: type[BaseModel]) -> SDKModelReference:
    fields = tuple(
        SDKFieldReference(
            name=name,
            type_name=_format_annotation(field.annotation),
            required=field.is_required(),
            default=_field_default(field),
            alias=field.alias if field.alias != name else None,
            constraints=_field_constraints(field.metadata),
            description=field.description or "",
        )
        for name, field in model.model_fields.items()
    )
    return SDKModelReference(
        name=model.__name__,
        family=_family(model),
        slug=_type_slug(model.__name__),
        description=_clean_docstring(inspect.getdoc(model)),
        fields=fields,
    )


def _collect_exception(exception: type[Exception]) -> SDKExceptionReference:
    try:
        signature = "(*args: object)" if "__init__" not in exception.__dict__ else str(inspect.signature(exception))
    except (TypeError, ValueError):
        signature = "(*args: object)"
    parent = exception.__base__.__name__ if exception.__base__ is not None else "Exception"
    return SDKExceptionReference(
        name=exception.__name__,
        parent=parent,
        description=_clean_docstring(inspect.getdoc(exception)),
        signature=exception.__name__ + signature,
    )


def _collect_resource(
    name: str,
    client_attribute: str,
    resource_class: type[object],
) -> SDKResourceReference:
    methods = tuple(
        _collect_method(method_name, value)
        for method_name, value in resource_class.__dict__.items()
        if not method_name.startswith("_") and callable(value)
    )
    return SDKResourceReference(
        name=name,
        client_attribute=client_attribute,
        description=_clean_docstring(inspect.getdoc(resource_class)),
        methods=methods,
    )


def _validate_type_entries(entries: Sequence[SDKModelReference | SDKEnumReference]) -> None:
    names = [entry.name for entry in entries]
    anchors = [(entry.family, entry.slug) for entry in entries]
    if len(names) != len(set(names)) or len(anchors) != len(set(anchors)):
        raise ValueError("Public SDK type names and family anchors must be unique")


def collect_sdk_reference() -> SDKReference:
    """Collect the tested top-level SDK contract."""
    import valkyrie.sdk as sdk
    from valkyrie.sdk.client import ValkyrieClient
    from valkyrie.sdk.resources import AgentsResource, BenchmarksResource, BenchmarkServicesResource, RunsResource

    resource_types = (
        ("RunsResource", "client.runs", RunsResource),
        ("BenchmarksResource", "client.benchmarks", BenchmarksResource),
        ("AgentsResource", "client.agents", AgentsResource),
        ("BenchmarkServicesResource", "client.services", BenchmarkServicesResource),
    )
    resources = tuple(_collect_resource(name, attribute, resource) for name, attribute, resource in resource_types)

    models: list[SDKModelReference] = []
    enums: list[SDKEnumReference] = []
    exceptions: list[SDKExceptionReference] = []
    for name in sdk.__all__:
        value = getattr(sdk, name)
        if inspect.isclass(value) and issubclass(value, Enum):
            enums.append(
                SDKEnumReference(
                    name=name,
                    family=_family(value),
                    slug=_type_slug(name),
                    description=_clean_docstring(inspect.getdoc(value)),
                    members=tuple((member.name, _format_value(member.value)) for member in value),
                )
            )
        elif inspect.isclass(value) and issubclass(value, BaseModel):
            models.append(_collect_model(value))
        elif inspect.isclass(value) and issubclass(value, Exception):
            exceptions.append(_collect_exception(value))

    type_entries = (*models, *enums)
    _validate_type_entries(type_entries)

    constructor = _collect_method("Constructor", ValkyrieClient.__init__)
    constructor_signature = _format_signature("ValkyrieClient", ValkyrieClient.__init__)
    constructor = constructor._replace(
        description=_clean_docstring(inspect.getdoc(ValkyrieClient)),
        signatures=(constructor_signature.removeprefix("def ").rsplit(" -> ", 1)[0] + " -> ValkyrieClient",),
        return_types=("ValkyrieClient",),
    )
    client_methods = (
        constructor,
        _collect_method("from_config", ValkyrieClient.from_config.__func__),
        _collect_method("close", ValkyrieClient.close),
    )
    return SDKReference(
        exports=tuple(sdk.__all__),
        resources=resources,
        models=tuple(models),
        enums=tuple(enums),
        exceptions=tuple(exceptions),
        client_methods=client_methods,
    )
