from pydantic import BaseModel


class AgentContract(BaseModel):
    name: str
    """Name of the agent."""

    artifacts: list[str] = []
    """Paths to artifacts."""

    install_cmd: str
    """Command to install the agent."""

    run_cmd: str
    """Command to run the agent."""

    env: dict[str, str] = {}
    """Environment variables required to run the agent."""
