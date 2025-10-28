"""Agent registry for managing agent implementations."""

from pathlib import Path


class AgentRegistry:
    """Registry for managing agents."""

    def __init__(self, agents_dir: Path = Path("agents")):
        """Initialize agent registry.
        Args:
            agents_dir: Directory containing agent implementations
        """
        self.agents_dir = agents_dir

    def get_agent(self, agent_id: str) -> Path:
        """Given an agent ID, returns the corresponding agent directory path."""

        agent_path = self.agents_dir / agent_id
        if not agent_path.exists():
            raise FileNotFoundError(f"Agent not found: {agent_id} at {agent_path}")

        return agent_path
