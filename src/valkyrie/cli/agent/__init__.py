import click

from valkyrie.cli.agent.lifecycle import agent_remove, download, install, list_installed_agents, push


@click.group()
def agent():
    """Agent command group"""
    pass


agent.add_command(install)
agent.add_command(push)
agent.add_command(agent_remove)
agent.add_command(download)
agent.add_command(list_installed_agents)

__all__ = ["agent"]
