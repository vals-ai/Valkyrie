"""Create, list, update, and delete the projects runs are started in, and describe a run's project."""

import re

import click
from click.core import ParameterSource
from valkyrie.sdk import SPEND_SUBCATEGORIES, ProjectCreateRequest, ProjectResponse, SpendType
from valkyrie.sdk.models import FetchBenchmarkMetadataResponse

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table, terminal_safe

_CORE_FIELDS = frozenset(name for name in ProjectCreateRequest.model_fields if name != "extra_tags")

_ALL_SUBCATEGORIES = tuple(subcategory for choices in SPEND_SUBCATEGORIES.values() for subcategory in choices)


_HANDLE_PATTERN = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
_HANDLE_MAX_LENGTH = 64


class _SpendSubcategory(click.Choice[str]):
    def convert(self, value: object, param: click.Parameter | None, ctx: click.Context | None) -> str:
        return str(value)

    def get_missing_message(self, param: click.Parameter, ctx: click.Context | None) -> str:
        if ctx is None:
            return f"Choose a subcategory. Allowed: {', '.join(_ALL_SUBCATEGORIES)}."
        spend_type = SpendType(ctx.params["spend_type"])
        allowed = ", ".join(SPEND_SUBCATEGORIES[spend_type])
        return f"Choose a subcategory for spend type '{spend_type.value}'. Allowed: {allowed}."


class _ProjectName(click.types.StringParamType):
    def convert(self, value: object, param: click.Parameter | None, ctx: click.Context | None) -> str:
        name = super().convert(value, param, ctx).strip()
        if not name:
            self.fail("must name the project this run is for (e.g., router-bench)", param, ctx)
        return name

    def get_missing_message(self, param: click.Parameter, ctx: click.Context | None) -> str:
        return "Name the project this run is for, e.g. router-bench."


class _ProjectHandle(click.types.StringParamType):
    def convert(self, value: object, param: click.Parameter | None, ctx: click.Context | None) -> str:
        handle = super().convert(value, param, ctx).strip()
        if len(handle) > _HANDLE_MAX_LENGTH or not _HANDLE_PATTERN.fullmatch(handle):
            self.fail(
                f"must be lowercase letters, digits and hyphens, at most {_HANDLE_MAX_LENGTH} characters, e.g. my-project",
                param,
                ctx,
            )
        return handle


class _ProjectLabel(click.types.StringParamType):
    def convert(self, value: object, param: click.Parameter | None, ctx: click.Context | None) -> str:
        label = super().convert(value, param, ctx).strip()
        if not label:
            self.fail('must name the project for people to read (e.g., "My Project")', param, ctx)
        return label


project_option = click.option(
    "--project",
    type=_ProjectName(),
    required=True,
    help="Handle of the project this run is for (e.g., router-bench); create one with `valkyrie project create`",
)


def describe_project(project: ProjectResponse) -> str:
    """Join the project's spend type, handle, subcategory, customer and extra tags into one display line."""
    parts = [project.spend_type.value, project.handle, project.subcategory, project.customer]
    parts += [f"{key}={value}" for key, value in project.extra_tags.items()]
    return terminal_safe(" · ".join(part for part in parts if part), preserve_newlines=False)


def describe_run_spend(metadata: FetchBenchmarkMetadataResponse | None) -> str:
    """Show a run's project on one line, or say why there is none to show."""
    if metadata is None:
        return "unavailable"
    if metadata.project is None:
        return "untagged"
    return describe_project(metadata.project)


def _parse_tags(_context: click.Context, _parameter: click.Parameter, values: tuple[str, ...]) -> dict[str, str]:
    tags: dict[str, str] = {}
    for value in values:
        key, separator, tag_value = value.partition("=")
        if not separator or not key:
            raise click.BadParameter(f"'{value}' is not key=value (e.g., --tag engagement=q4)")
        if key in _CORE_FIELDS:
            raise click.BadParameter(f"'{key}' is a core project field and cannot be an extra tag")
        if key in tags:
            raise click.BadParameter(f"'{key}' is given twice")
        tags[key] = tag_value
    return tags


@click.group()
def project() -> None:
    """Manage the projects runs are started in"""


@project.command(name="create")
@click.argument("handle", metavar="HANDLE", type=_ProjectHandle())
@click.option(
    "--name",
    type=_ProjectLabel(),
    required=True,
    help='Name people read (e.g., "My Project"); the HANDLE is what runs are started with and never changes',
)
@click.option(
    "--spend-type",
    type=click.Choice([spend_type.value for spend_type in SpendType]),
    required=True,
    help="Why this project's runs spend money: development, internal, or customer",
)
@click.option(
    "--subcategory",
    type=_SpendSubcategory(_ALL_SUBCATEGORIES),
    required=True,
    help=(
        "development: New Benchmark Generation (new_benchmark_generation), New Benchmark Launch Runs "
        "(new_benchmark_launch_runs), Post-Launch Benchmark Update (post_launch_benchmark_update); customer: "
        "Report (report), Paid Run (paid_run), Eval Server (eval_server), Platform (platform); internal: Unpaid "
        "Leaderboard Run (unpaid_leaderboard_run), Other Unpaid Run (other_unpaid_run), Fellows/Interns "
        "(fellows_interns), Blog (blog), Miscellaneous (miscellaneous)"
    ),
)
@click.option(
    "--customer",
    type=str,
    default=None,
    help="Customer the project is for; required with --spend-type customer",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    callback=_parse_tags,
    help="Extra tag as key=value, repeatable (e.g., --tag engagement=q4)",
)
def create_project(
    handle: str, name: str, spend_type: str, subcategory: str, customer: str | None, tags: dict[str, str]
) -> None:
    """Create a project that runs can be started in with `valkyrie run start --project HANDLE`.

    HANDLE is the project's permanent handle: lowercase letters, digits and hyphens (e.g., my-project).

    Example:
        valkyrie project create my-project --name "My Project" --spend-type development --subcategory new_benchmark_generation
    """
    resolved_type = SpendType(spend_type)
    allowed = SPEND_SUBCATEGORIES[resolved_type]
    if subcategory not in allowed:
        raise click.BadParameter(
            f"'{subcategory}' is not a subcategory of spend type '{spend_type}'. Allowed: {', '.join(allowed)}",
            param_hint="'--subcategory'",
        )
    if customer is not None:
        customer = customer.strip() or None
    if resolved_type is SpendType.CUSTOMER and not customer:
        raise click.UsageError("Missing option '--customer'. --spend-type customer requires the customer's name.")
    created = call(
        lambda client: client.projects.create(
            handle, name=name, spend_type=resolved_type, subcategory=subcategory, customer=customer, extra_tags=tags
        )
    )
    click.echo(click.style(f"✓ Project created: {describe_project(created)}", fg="green", bold=True))


@project.command(name="list")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
def list_projects(output_format: str) -> None:
    """List your organization's projects."""
    response = call(lambda client: client.projects.list())
    if output_format == "json":
        click.echo(response.model_dump_json())
        return
    format_table(
        [
            {
                "Handle": terminal_safe(entry.handle, preserve_newlines=False),
                "Name": terminal_safe(entry.name, preserve_newlines=False),
                "Spend type": entry.spend_type.value,
                "Subcategory": terminal_safe(entry.subcategory or "", preserve_newlines=False),
                "Customer": terminal_safe(entry.customer or "", preserve_newlines=False),
                "Tags": terminal_safe(
                    ", ".join(f"{key}={value}" for key, value in entry.extra_tags.items()), preserve_newlines=False
                ),
            }
            for entry in response.projects
        ],
        ["Handle", "Name", "Spend type", "Subcategory", "Customer", "Tags"],
        item_name="project",
    )


@project.command(name="update")
@click.argument("handle", metavar="HANDLE", type=_ProjectHandle())
@click.option("--name", type=_ProjectLabel(), help="New name people read.")
@click.option(
    "--spend-type",
    type=click.Choice([spend_type.value for spend_type in SpendType]),
    help="New reason this project's runs spend money.",
)
@click.option("--subcategory", type=str, help="New spend subcategory.")
@click.option("--customer", type=str, help="New customer name.")
@click.option("--clear-customer", is_flag=True, help="Remove the saved customer.")
@click.option(
    "--tag",
    "tags",
    multiple=True,
    callback=_parse_tags,
    help="Replace all extra tags with key=value values; repeatable.",
)
@click.option("--clear-tags", is_flag=True, help="Remove all extra tags.")
@click.pass_context
def update_project(
    context: click.Context,
    handle: str,
    name: str | None,
    spend_type: str | None,
    subcategory: str | None,
    customer: str | None,
    clear_customer: bool,
    tags: dict[str, str],
    clear_tags: bool,
) -> None:
    """Update a project's shared spend metadata without changing HANDLE."""
    tags_given = context.get_parameter_source("tags") is ParameterSource.COMMANDLINE
    if customer is not None and clear_customer:
        raise click.UsageError("Use either --customer or --clear-customer, not both.")
    if tags_given and clear_tags:
        raise click.UsageError("Use either --tag or --clear-tags, not both.")
    if not any(
        (
            name is not None,
            spend_type is not None,
            subcategory is not None,
            customer is not None,
            clear_customer,
            tags_given,
            clear_tags,
        )
    ):
        raise click.UsageError("Provide at least one field to update.")
    if customer is not None:
        customer = customer.strip()
        if not customer:
            raise click.BadParameter("must not be blank; use --clear-customer to remove it", param_hint="--customer")
    projects = call(lambda client: client.projects.list()).projects
    saved = next((entry for entry in projects if entry.handle == handle), None)
    if saved is None:
        raise click.ClickException(f"Project '{handle}' was not found.")
    resolved_subcategory = subcategory if subcategory is not None else saved.subcategory
    if resolved_subcategory is None:
        raise click.ClickException("This project needs a subcategory before it can be updated.")
    resolved_name = name if name is not None else saved.name
    resolved_type = SpendType(spend_type) if spend_type is not None else saved.spend_type
    resolved_customer = None if clear_customer else customer if customer is not None else saved.customer
    resolved_tags = {} if clear_tags else tags if tags_given else saved.extra_tags
    if (
        resolved_name,
        resolved_type,
        resolved_subcategory,
        resolved_customer,
        resolved_tags,
    ) == (saved.name, saved.spend_type, saved.subcategory, saved.customer, saved.extra_tags):
        click.echo(f"No changes to project '{handle}'.")
        return
    updated = call(
        lambda client: client.projects.update(
            handle,
            name=resolved_name,
            spend_type=resolved_type,
            subcategory=resolved_subcategory,
            customer=resolved_customer,
            extra_tags=resolved_tags,
        )
    )
    click.echo(click.style(f"✓ Project updated: {describe_project(updated)}", fg="green", bold=True))


@project.command(name="delete")
@click.argument("handle", metavar="HANDLE", type=_ProjectHandle())
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def delete_project(handle: str, yes: bool) -> None:
    """Delete an unused project. Projects with run history cannot be deleted."""
    if not yes and not click.confirm(f"Delete project '{handle}'? Only projects without run history can be deleted."):
        click.echo("Cancelled.")
        return
    deleted = call(lambda client: client.projects.delete(handle))
    click.echo(click.style(f"✓ Project deleted: {describe_project(deleted)}", fg="green", bold=True))
