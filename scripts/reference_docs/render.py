"""Render collected reference metadata as deterministic Mintlify MDX and JSON."""

from __future__ import annotations

import html
import json
import re
from collections.abc import Sequence
from pathlib import Path

from .collect import collect_cli_commands, collect_sdk_reference
from .model import (
    CLI_CARDS,
    CLI_INDEX,
    CLI_NAVIGATION,
    CLI_ROOT,
    GENERATED_MARKER,
    GUIDE_LINKS,
    READ_ONLY_COMMANDS,
    REDIRECTS,
    RESOURCE_CARDS,
    SDK_CLIENT,
    SDK_ERRORS,
    SDK_INDEX,
    SDK_NAVIGATION,
    SDK_ROOT,
    STATIC_REDIRECTS,
    TYPE_CARDS,
    TYPE_INDEX,
    TYPE_ROOT,
    CLICommandReference,
    CLIParameterReference,
    SDKEnumReference,
    SDKMethodReference,
    SDKModelReference,
    SDKParameterReference,
    SDKReference,
    SDKResourceReference,
)

TypeEntry = SDKModelReference | SDKEnumReference


def _route(path: Path) -> str:
    return path.with_suffix("").as_posix()


def _page_path(root: Path, name: str) -> Path:
    return (root / name).with_suffix(".mdx")


def _join_blocks(*blocks: str, separator: str = "\n\n") -> str:
    return separator.join(block for block in blocks if block)


def _page(title: str, description: str, *blocks: str) -> str:
    frontmatter = (
        f"---\ntitle: {json.dumps(title)}\ndescription: {json.dumps(description)}\n---\n\n{GENERATED_MARKER}\n\n"
    )
    return frontmatter + _join_blocks(*blocks).rstrip() + "\n"


def _card(title: str, icon: str, path: Path, description: str) -> str:
    return f'  <Card title="{title}" icon="{icon}" href="/{_route(path)}">\n    {description}\n  </Card>'


def _card_group(cards: Sequence[str]) -> str:
    return "<CardGroup cols={2}>\n" + "\n".join(cards) + "\n</CardGroup>"


def _summary(value: str, fallback: str) -> tuple[str, tuple[str, ...]]:
    paragraphs = tuple(" ".join(block.split()) for block in value.strip().split("\n\n") if block.strip())
    return (paragraphs[0], paragraphs[1:]) if paragraphs else (fallback, ())


def _groups(commands: Sequence[CLICommandReference]) -> dict[str, list[CLICommandReference]]:
    groups: dict[str, list[CLICommandReference]] = {}
    for command in commands:
        groups.setdefault(command.path[0], []).append(command)
    return groups


def _type_entries(reference: SDKReference) -> tuple[TypeEntry, ...]:
    entries = {entry.name: entry for entry in (*reference.models, *reference.enums)}
    return tuple(entries[name] for name in reference.exports if name in entries)


def _type_url(entry: TypeEntry) -> str:
    return f"/{_route(_page_path(TYPE_ROOT, entry.family.lower()))}#{entry.slug}"


def _jsx(value: str) -> str:
    return "{" + json.dumps(value, ensure_ascii=False) + "}"


def _html(value: str) -> str:
    return html.escape(value, quote=False).replace("{", "&#123;").replace("}", "&#125;").replace("\n", "<br />")


def _linked_type(value: str, routes: dict[str, str]) -> str:
    parts = re.split(r"([A-Za-z_][A-Za-z0-9_]*)", value)
    return "".join(
        f'<a href="{routes[part]}"><code>{part}</code></a>' if part in routes else _html(part) for part in parts
    )


def _row(
    name: str,
    type_name: str,
    required: bool,
    default: str | None,
    *,
    constraints: Sequence[str] = (),
    description: str = "",
    alias: str | None = None,
    type_markup: bool = False,
) -> str:
    rendered_type = type_name if type_markup else f"<code>{_jsx(type_name)}</code>"
    metadata = ["required" if required else "optional", rendered_type]
    if alias is not None:
        metadata.append(f"alias: <code>{_jsx(alias)}</code>")
    if default is not None:
        metadata.append(f"default: <code>{_jsx(default)}</code>")
    if constraints:
        metadata.append("constraints: " + _jsx("; ".join(constraints)))
    detail = f'\n  <p className="mb-0 mt-1">{_jsx(description)}</p>' if description else ""
    return (
        '<div className="border-b border-gray-200 py-3 dark:border-gray-800">\n'
        '  <div className="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-1">'
        f'<span className="break-all font-mono text-sm">{_jsx(name)}</span>'
        '<span className="min-w-0 break-words text-sm text-gray-500 dark:text-gray-400">'
        f"{' · '.join(metadata)}</span></div>{detail}\n</div>"
    )


def _enum_row(name: str, value: str) -> str:
    return (
        '<div className="border-b border-gray-200 py-3 dark:border-gray-800">\n'
        '  <div className="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-1">'
        f'<span className="break-all font-mono text-sm">{_jsx(name)}</span>'
        '<span className="text-sm text-gray-500 dark:text-gray-400">'
        f"<code>{_jsx(value)}</code></span></div>\n</div>"
    )


def _parameter_rows(parameters: Sequence[SDKParameterReference], routes: dict[str, str], *, heading: str) -> str:
    rows: list[str] = []
    for parameter in parameters:
        type_name = _linked_type(parameter.type_name, routes)
        rows.append(_row(parameter.name, type_name, parameter.required, parameter.default, type_markup=True))
    return f"{heading}\n\n" + _join_blocks(*rows) if rows else ""


def _cli_parameters(parameters: Sequence[CLIParameterReference], kind: str) -> str:
    selected = [parameter for parameter in parameters if parameter.kind == kind]
    if not selected:
        return ""
    title = "Arguments" if kind == "argument" else "Options"
    rows = (
        _row(
            ", ".join(item.declarations),
            item.type_name,
            item.required,
            item.default,
            constraints=item.constraints,
            description=item.help,
        )
        for item in selected
    )
    return f"**{title}**\n\n" + _join_blocks(*rows)


def _metavar(parameter: CLIParameterReference) -> str:
    if parameter.metavar:
        values = [f"<{parameter.metavar}>" for _ in range(max(parameter.nargs, 1))]
    elif parameter.kind == "argument":
        values = [f"<{parameter.declarations[0]}>" for _ in range(max(parameter.nargs, 1))]
    elif parameter.type_name.startswith("tuple["):
        values = [f"<{item.upper()}>" for item in parameter.type_name[6:-1].split(", ")]
    elif parameter.type_name == "boolean":
        values = []
    else:
        values = [f"<{parameter.type_name.upper()}>" for _ in range(max(parameter.nargs, 1))]
    return " ".join(values)


def _usage(command: CLICommandReference) -> str:
    fragments = ["valkyrie " + " ".join(command.path)]
    for parameter in command.parameters:
        value = _metavar(parameter)
        fragment = (
            value if parameter.kind == "argument" else "|".join(parameter.declarations) + (f" {value}" if value else "")
        )
        fragment += "..." if parameter.multiple else ""
        fragments.append(fragment if parameter.required else f"[{fragment}]")
    return " \\\n  ".join(fragments)


def _format_example(command: CLICommandReference, example: str) -> str:
    comments = [line for line in example.splitlines() if line.startswith("#")]
    command_line = next((line for line in example.splitlines() if line and not line.startswith("#")), "")
    tokens, prefix_length = command_line.split(), 1 + len(command.path)
    if len(tokens) <= prefix_length:
        return example
    options = {
        declaration: parameter
        for parameter in command.parameters
        if parameter.kind == "option"
        for declaration in parameter.declarations
    }
    groups: list[list[str]] = []
    index = prefix_length
    while index < len(tokens):
        parameter = options.get(tokens[index])
        count = max(parameter.nargs, 1) if parameter and parameter.type_name != "boolean" else 0
        groups.append(tokens[index : index + count + 1])
        index += count + 1
    if len(groups) <= 1:
        return example
    lines = [*comments, " ".join(tokens[:prefix_length]) + " \\"]
    lines.extend(
        "  " + " ".join(group) + (" \\" if position < len(groups) - 1 else "") for position, group in enumerate(groups)
    )
    return "\n".join(lines)


def _example(command: CLICommandReference) -> str | None:
    if command.example is None:
        return None
    example = command.example
    if command.path == ("run", "stop"):
        example = "valkyrie run stop $RUN_ID"
    elif command.path == ("config", "auth", "set"):
        example = "valkyrie config auth set benchmark-name $BENCHMARK_CREDENTIAL"
    if command.path not in READ_ONLY_COMMANDS:
        example = "# Example only, review before running\n" + example
    return _format_example(command, example)


def _command_section(command: CLICommandReference) -> str:
    name = "valkyrie " + " ".join(command.path)
    summary, paragraphs = _summary(command.summary, f"Reference for {name}.")
    blocks = [
        f"## `{name}` {{#{'-'.join(command.path[1:])}}}",
        summary,
        *paragraphs,
        f"```bash Syntax\n{_usage(command)}\n```",
    ]
    if (example := _example(command)) is not None:
        blocks.append(f"```bash Example\n{example}\n```")
    if command.path == ("run", "retry"):
        blocks.append(
            "`retry` is a command alias for the retry mode of `run resume` and shares its options. It always retries tasks in `ERROR` status; use `run resume` to admit pending tasks instead."
        )
    blocks.extend(
        (
            _cli_parameters(command.parameters, "argument"),
            _cli_parameters(command.parameters, "option"),
            f"Task guidance: [{command.path[0].title()} guides]({GUIDE_LINKS[command.path[0]]}).",
        )
    )
    return _join_blocks(*blocks)


def _cli_pages(commands: Sequence[CLICommandReference]) -> dict[Path, str]:
    groups = _groups(commands)
    cards: list[str] = []
    pages: dict[Path, str] = {}
    for group, group_commands in groups.items():
        icon, purpose = CLI_CARDS[group]
        count = len(group_commands)
        noun = "command" if count == 1 else "commands"
        cards.append(
            _card(
                f"{group.title()} commands",
                icon,
                _page_path(CLI_ROOT, group),
                f"{count} {noun} for {purpose}.",
            )
        )
        sections = _join_blocks(
            *(_command_section(command) for command in group_commands),
            separator="\n\n---\n\n",
        )
        pages[_page_path(CLI_ROOT, group)] = _page(
            f"valkyrie {group}",
            f"Complete generated reference for every `valkyrie {group}` command.",
            f"This page documents all {count} commands in the `{group}` group in registration order. Use the table of contents to jump to a command.",
            sections,
        )
    index = _page(
        "CLI reference",
        "Complete generated reference for every Valkyrie CLI command.",
        "This reference is generated from the CLI itself. Use the task guides for complete workflows.",
        "## Command groups",
        _card_group(cards),
    )
    navigation = json.dumps([_route(CLI_INDEX), *(_route(path) for path in pages)], indent=2) + "\n"
    return {CLI_INDEX: index, CLI_NAVIGATION: navigation, **pages}


def _returns(method: SDKMethodReference, routes: dict[str, str], heading: str) -> str:
    if len(method.return_types) == 1:
        return f"{heading}\n\n{_linked_type(method.return_types[0], routes)}"
    rows = "\n".join(
        f"| {index} | {_linked_type(value, routes)} |" for index, value in enumerate(method.return_types, 1)
    )
    return f"{heading}\n\nThe selected overload determines the return type.\n\n| Overload | Return type |\n| ---: | --- |\n{rows}"


def _signatures(method: SDKMethodReference) -> str:
    return _join_blocks(
        *(
            f"```python {'Signature' if len(method.signatures) == 1 else f'Overload {index}'}\n{signature}\n```"
            for index, signature in enumerate(method.signatures, 1)
        )
    )


def _method_section(resource: SDKResourceReference, method: SDKMethodReference, routes: dict[str, str]) -> str:
    title = f"{resource.client_attribute}.{method.name}"
    summary, paragraphs = _summary(method.description, f"Reference for {title}.")
    guide = "/sdk/runs" if resource.client_attribute.endswith(".runs") else "/sdk/resources"
    blocks = (
        f"## `{title}` {{#{method.name.replace('_', '-')}}}",
        summary,
        *paragraphs,
        _signatures(method),
        _parameter_rows(method.parameters, routes, heading="**Parameters**"),
        _returns(method, routes, "**Returns**"),
        f"For examples, see the [Python SDK guide]({guide}).",
    )
    return _join_blocks(*blocks)


def _type_section(entry: TypeEntry, routes: dict[str, str]) -> str:
    kind = "enum" if isinstance(entry, SDKEnumReference) else "model"
    items = entry.members if isinstance(entry, SDKEnumReference) else entry.fields
    noun = ("member" if kind == "enum" else "field") + ("s" if len(items) != 1 else "")
    blocks = [
        f"## `{entry.name}` {{#{entry.slug}}}",
        entry.description,
        f"{entry.family} {kind} · {len(items)} {noun}",
        f"**{'Members' if kind == 'enum' else 'Fields'}**",
    ]
    if isinstance(entry, SDKEnumReference):
        rows = [_enum_row(name, value) for name, value in entry.members]
    else:
        rows = [
            _row(
                field.name,
                _linked_type(field.type_name, routes),
                field.required,
                field.default,
                alias=field.alias,
                constraints=field.constraints,
                description=field.description,
                type_markup=True,
            )
            for field in entry.fields
        ]
    return _join_blocks(*blocks, *rows)


def _sdk_pages(reference: SDKReference) -> dict[Path, str]:
    entries = _type_entries(reference)
    expected_families = tuple(TYPE_CARDS)
    actual_families = {entry.family for entry in entries}
    if actual_families != set(expected_families):
        raise ValueError(f"SDK type families must match {expected_families}; got {tuple(sorted(actual_families))}")

    routes = {entry.name: _type_url(entry) for entry in entries}
    resource_cards: list[str] = []
    resource_pages: dict[Path, str] = {}
    for resource in reference.resources:
        page_name = resource.client_attribute.rsplit(".", 1)[-1]
        path = _page_path(SDK_ROOT, page_name)
        title, icon, purpose = RESOURCE_CARDS[resource.name]
        method_count = len(resource.methods)
        noun = "method" if method_count == 1 else "methods"
        resource_cards.append(_card(title, icon, path, f"{method_count} {noun} for {purpose}."))
        description, body = _summary(resource.description, f"Reference for {resource.client_attribute}.")
        resource_pages[path] = _page(
            f"{title} resource",
            description,
            *body,
            f"Access these methods through `{resource.client_attribute}`.",
            *(_method_section(resource, method, routes) for method in resource.methods),
        )
    index = _page(
        "Python SDK reference",
        "Complete generated reference for the public Valkyrie Python SDK.",
        "This reference follows the tested `valkyrie.sdk` public contract. Internal models and helpers are excluded.",
        "## Client",
        f"Use [`ValkyrieClient`](/{_route(SDK_CLIENT)}) to configure the async client and access resource namespaces.",
        "## Resource namespaces",
        _card_group(resource_cards),
        "## Types and errors",
        f"Browse [public models and enums](/{_route(TYPE_INDEX.parent)}) or the [SDK error hierarchy](/{_route(SDK_ERRORS)}).",
    )
    client_sections = [
        _join_blocks(
            f"## {method.name}",
            method.description,
            _signatures(method),
            _parameter_rows(method.parameters, routes, heading="### Parameters"),
            _returns(method, routes, "### Returns"),
        )
        for method in reference.client_methods
    ]
    client = _page(
        "ValkyrieClient",
        "Construct and close the async Valkyrie SDK client.",
        "`ValkyrieClient` owns the async HTTP connection pool and exposes the public resource namespaces.",
        *client_sections,
        "## Async context manager",
        "The client closes its connection pool when an `async with` block exits.",
        '```python\nasync with ValkyrieClient(config) as client:\n    run = await client.runs.start(agent="sweagent", benchmark="swebench")\n```',
    )
    type_cards: list[str] = []
    type_pages: dict[Path, str] = {}
    for family, (icon, purpose) in TYPE_CARDS.items():
        family_entries = [entry for entry in entries if entry.family == family]
        path = _page_path(TYPE_ROOT, family.lower())
        count = len(family_entries)
        noun = "type" if count == 1 else "types"
        type_cards.append(_card(family, icon, path, f"{count} {noun} for {purpose}."))
        sections = _join_blocks(
            *(_type_section(entry, routes) for entry in family_entries),
            separator="\n\n---\n\n",
        )
        type_pages[path] = _page(
            f"{family} types",
            f"Public {family.lower()} models and enums exported by the Valkyrie Python SDK.",
            f"This page documents all {count} public types in the `{family}` family in source order. Use the table of contents to jump to a type.",
            sections,
        )
    types_index = _page(
        "Python SDK types",
        "Public models and enums exported by the Valkyrie Python SDK.",
        "Only public models and enums exported from `valkyrie.sdk` appear here.",
        "## Type families",
        _card_group(type_cards),
    )
    error_rows = "\n".join(
        f"| [`{error.name}`](#{error.name.lower()}) | `{error.parent}` |" for error in reference.exceptions
    )
    error_sections = "\n\n".join(
        f"## `{error.name}`\n\n{error.description}\n\n```python\n{error.signature}\n```"
        for error in reference.exceptions
    )
    errors = _page(
        "Python SDK errors",
        "Generated exception hierarchy for the public Valkyrie SDK.",
        "Catch `ValkyrieSDKError` to handle every SDK-defined failure, or catch a specific subclass.",
        f"| Exception | Parent |\n| --- | --- |\n{error_rows}",
        error_sections,
    )
    navigation: list[object] = [
        _route(SDK_INDEX),
        _route(SDK_CLIENT),
        *(_route(path) for path in resource_pages),
        {"group": "Types", "pages": [_route(TYPE_INDEX), *(_route(path) for path in type_pages), _route(SDK_ERRORS)]},
    ]
    return {
        SDK_INDEX: index,
        SDK_CLIENT: client,
        SDK_NAVIGATION: json.dumps(navigation, indent=2) + "\n",
        **resource_pages,
        TYPE_INDEX: types_index,
        **type_pages,
        SDK_ERRORS: errors,
    }


def _redirects(commands: Sequence[CLICommandReference], reference: SDKReference) -> list[dict[str, str]]:
    cli = [
        {
            "source": "/" + CLI_ROOT.joinpath(*command.path).as_posix(),
            "destination": f"/{_route(_page_path(CLI_ROOT, command.path[0]))}#{'-'.join(command.path[1:])}",
        }
        for command in commands
    ]
    methods = [
        {
            "source": "/" + (SDK_ROOT / resource.client_attribute.rsplit(".", 1)[-1] / method.name).as_posix(),
            "destination": f"/{_route(_page_path(SDK_ROOT, resource.client_attribute.rsplit('.', 1)[-1]))}#{method.name.replace('_', '-')}",
        }
        for resource in reference.resources
        for method in resource.methods
    ]
    types = [
        {"source": "/" + (TYPE_ROOT / entry.family.lower() / entry.slug).as_posix(), "destination": _type_url(entry)}
        for entry in _type_entries(reference)
    ]
    return [*STATIC_REDIRECTS, *cli, *methods, *types]


def render_reference() -> dict[Path, str]:
    """Collect each source surface once and render the generated manifest."""
    commands = collect_cli_commands()
    sdk_reference = collect_sdk_reference()
    return {
        **_cli_pages(commands),
        **_sdk_pages(sdk_reference),
        REDIRECTS: json.dumps(_redirects(commands, sdk_reference), indent=2) + "\n",
    }
