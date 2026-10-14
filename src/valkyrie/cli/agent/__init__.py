import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    lazy_commands={
        "install": (
            "valkyrie.cli.agent.lifecycle:install",
            "Install a GitHub agent through Tracker, replacing an existing alias.",
        ),
        "push": (
            "valkyrie.cli.agent.lifecycle:push",
            "Upload a local agent through Tracker, replacing an existing alias.",
        ),
        "remove": ("valkyrie.cli.agent.lifecycle:agent_remove", "Remove an installed agent"),
        "download": ("valkyrie.cli.agent.lifecycle:download", "Download an installed agent"),
        "list": ("valkyrie.cli.agent.lifecycle:list_installed_agents", "List installed agents"),
    },
)
def agent():
    """Install, upload, list, download, and remove agents."""


__all__ = ["agent"]
