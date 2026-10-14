import json
from datetime import datetime
from pathlib import Path

import click
from valkyrie.sdk import ValkyrieClient

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table, local_time, paginate_cli_pages


@click.command(name="install", help="Install a GitHub agent through Tracker, replacing an existing alias.")
@click.argument("github_url", type=str)
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to the contract name)",
)
def install(github_url: str, name: str | None):
    """Install an agent from a GitHub repository or a subfolder within a repository.

    Supports both full repository URLs and subfolder paths using GitHub's tree syntax.

    Example:
        valkyrie agent install https://github.com/user/my-agent
        valkyrie agent install https://github.com/user/my-agent --name my-custom-name
        valkyrie agent install https://github.com/org/registry/tree/main/agents/codex
        valkyrie agent install https://github.com/org/registry/tree/main/agents/codex --name my-agent
    """
    entry = call(lambda client: client.agents.install(github_url, name=name))
    click.echo(click.style(f"✓ Agent '{entry.name}' installed successfully!", fg="green", bold=True))


@click.command(name="push", help="Upload a local agent through Tracker, replacing an existing alias.")
@click.argument("agent_path", type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True))
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to the contract name)",
)
def push(agent_path: Path, name: str | None):
    entry = call(lambda client: client.agents.push(agent_path, name=name or None))
    click.echo(click.style(f"✓ Agent '{entry.name}' pushed successfully!", fg="green", bold=True))


@click.command(name="remove", help="Remove an installed agent")
@click.argument("agent_name", type=str)
def agent_remove(agent_name: str):
    if not click.confirm(f"Are you sure you want to remove agent '{agent_name}'?"):
        click.echo("Cancelled.")
        return

    call(lambda client: client.agents.remove(agent_name))
    click.echo(click.style(f"✓ Agent '{agent_name}' removed successfully!", fg="green", bold=True))


@click.command(name="download", help="Download an installed agent")
@click.argument("agent_name", type=str)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path, file_okay=False, dir_okay=True),
    default=None,
    required=False,
    help="Output directory for downloaded agent (default: current directory)",
)
@click.option("--overwrite", is_flag=True, help="Replace an existing agent directory after validating the download.")
def download(agent_name: str, output_dir: Path | None, overwrite: bool):
    """Download and safely extract an agent through the SDK.

    Example:
        valkyrie agent download my-agent
    """
    call(lambda client: client.agents.download(agent_name, output_dir, overwrite=overwrite))
    click.echo(click.style(f"✓ Agent '{agent_name}' downloaded successfully!", fg="green", bold=True))


@click.command(name="list", help="List installed agents")
@click.option("--all", "all_agents", is_flag=True, help="Show every agent without interactive paging.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. JSON includes every agent without interactive paging.",
)
def list_installed_agents(all_agents: bool, output_format: str):
    """List all installed agents through Tracker.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie agent list
    """

    async def list_agents(client: ValkyrieClient) -> list[tuple[str, datetime | None]]:
        response = await client.agents.list()
        return [
            (agent.name, datetime.fromisoformat(agent.last_modified) if agent.last_modified else None)
            for agent in response.agents
        ]

    agents = call(list_agents)

    if output_format == "json":
        click.echo(
            json.dumps(
                {
                    "agents": [
                        {"name": name, "last_modified": modified.isoformat() if modified else None}
                        for name, modified in agents
                    ]
                }
            )
        )
        return

    if not agents:
        click.echo(click.style("No agents found.", fg="yellow"))
        return

    if all_agents:
        _format_agents_response(agents, 1, 1, len(agents))
    else:
        paginate_agents(agents)


def _format_agents_response(
    agents: list[tuple[str, datetime | None]],
    current_page: int,
    total_pages: int,
    total_count: int,
) -> None:
    rows = [
        {"Agent": name, "Last Modified": local_time(last_modified) if last_modified else "-"}
        for name, last_modified in agents
    ]

    format_table(rows, ["Agent", "Last Modified"], current_page, total_pages, total_count, "agent")


def paginate_agents(agents: list[tuple[str, datetime | None]], limit: int = 10) -> None:
    """Interactively page through installed agents."""
    paginate_cli_pages(
        lambda offset, page_limit: (len(agents), agents[offset : offset + page_limit]),
        _format_agents_response,
        limit=limit,
        render_empty=lambda: click.echo(click.style("No agents found.", fg="yellow")),
    )
