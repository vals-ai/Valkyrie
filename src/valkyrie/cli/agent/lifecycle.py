import asyncio
from datetime import datetime
from pathlib import Path

import click
from tracker.exceptions import S3Error

from valkyrie.cli.agent.storage import download_agent, install_agent, list_agents, push_agent, remove_agent
from valkyrie.cli.display import format_table, local_time


@click.command(name="install", help="Installs agent from a github project to the users aws environment")
@click.argument("github_url", type=str)
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to repository name or subfolder name)",
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
    try:
        resolved_name = asyncio.run(install_agent(name, github_url))
        click.echo(click.style(f"✓ Agent '{resolved_name}' installed successfully!", fg="green", bold=True))
    except (RuntimeError, S3Error) as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@click.command(name="push", help="Pushes agent to the users aws environment from the local filesystem")
@click.argument("agent_path", type=click.Path(exists=True, path_type=Path, file_okay=False, dir_okay=True))
@click.option(
    "--name",
    "-n",
    type=str,
    required=False,
    help="Agent name (defaults to path stem)",
)
def push(agent_path: Path, name: str | None):
    """Push a local agent to S3.

    Example:
        valkyrie agent push ./agents/my-agent
        valkyrie agent push ./agents/my-agent --name my-agent
    """
    try:
        agent_name = name or agent_path.stem
        asyncio.run(push_agent(agent_name, agent_path))
        click.echo(click.style(f"✓ Agent '{agent_name}' pushed successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@click.command(name="remove", help="Remove an installed agent")
@click.argument("agent_name", type=str)
def agent_remove(agent_name: str):
    """Remove an agent from S3.

    Example:
        valkyrie agent remove my-agent
    """
    try:
        if not click.confirm(f"Are you sure you want to remove agent '{agent_name}'?"):
            click.echo("Cancelled.")
            return

        asyncio.run(remove_agent(agent_name))
        click.echo(click.style(f"✓ Agent '{agent_name}' removed successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


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
def download(agent_name: str, output_dir: Path | None):
    """Download an agent from S3.

    Example:
        valkyrie agent download my-agent
    """
    try:
        asyncio.run(download_agent(agent_name, output_dir))
        click.echo(click.style(f"✓ Agent '{agent_name}' downloaded successfully!", fg="green", bold=True))
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


@click.command(name="list", help="List installed agents")
def list_installed_agents():
    """List all installed agents in S3.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie agent list
    """
    try:
        agents = asyncio.run(list_agents())

        if not agents:
            click.echo(click.style("\r\033[KNo agents found.", fg="yellow"))
            return

        paginate_agents(agents)
    except S3Error as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(f"Unexpected error: {str(e)}")


def _clear_pager() -> None:
    click.echo("\033[2J\033[3J\033[1;1H", nl=False, color=True)


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
    current_page = 1
    offset = 0
    total_count = len(agents)
    total_pages = max(1, (total_count + limit - 1) // limit)

    while True:
        _clear_pager()

        if total_count == 0:
            click.echo(click.style("No agents found.", fg="yellow"))
            break

        page_agents = agents[offset : offset + limit]
        _format_agents_response(page_agents, current_page, total_pages, total_count)

        if total_pages <= 1:
            break

        char = click.getchar()

        if char == "l" and current_page < total_pages:
            current_page += 1
            offset += limit
        elif char == "h" and current_page > 1:
            current_page -= 1
            offset -= limit
        elif char == "q" or char == "\x03":
            break
