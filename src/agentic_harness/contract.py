from abc import ABC, abstractmethod
from pathlib import Path

from tracker.database.models import AgentContractRequest

from agentic_harness.schemas import AgentConfig


class BaseAgentContract(ABC):
    """
    Base class for agent contracts.

    Agent contracts define how to install and run an agent in a sandbox environment.
    Subclasses must implement the required abstract properties (name, run_cmd, install_cmd)
    and can optionally override env and artifacts.
    """

    def __init__(self, agent_config: AgentConfig):
        """
        Initialize the agent contract.

        Args:
            agent_config: Optional configuration for the agent (e.g., model selection).
        """
        self._agent_config = agent_config

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The name of the agent contract.

        Returns:
            Agent name (e.g., "claude_code", "sweagent")
        """
        pass

    @property
    @abstractmethod
    def run_cmd(self) -> str:
        """
        Command to run the agent on a task.

        The command should include {{problem_statement}} as a placeholder,
        which will be replaced with the actual task prompt at runtime.

        Returns:
            Shell command to execute the agent (e.g., "claude code -p {{problem_statement}}")
        """
        pass

    @property
    @abstractmethod
    def install_cmd(self) -> str:
        """
        Command to install the agent and its dependencies.

        This runs once during sandbox setup with the working directory
        set to /bundle/<agent_name>/

        Returns:
            Shell command to install the agent (e.g., "bash setup.sh")
        """
        pass

    @property
    def env(self) -> dict[str, str]:
        """
        Environment variables required by the agent.

        Override this property to provide environment variables like API keys.
        Load secrets from your local environment using os.getenv().

        Returns:
            Dictionary of environment variable names and values (default: empty dict)
        """
        return {}

    @property
    def artifacts(self) -> list[str]:
        """
        List of files and directories to bundle with the agent.

        Paths are relative to the agent directory (e.g., agents/my_agent/).
        Common artifacts include setup scripts, agent code, and configuration files.

        Returns:
            List of artifact paths (default: empty list)
        """
        return []

    @property
    @abstractmethod
    def final_output(self) -> Path | None:
        """
        Path to the final output of the agent. Needs to be an absolute path.

        Returns:
            Path | None: The path to the final output that the agent writes to.
        """
        pass

    def to_request(self) -> AgentContractRequest:
        """
        Convert the contract to a request object for the tracker service.

        Returns:
            AgentContractRequest with all contract properties
        """
        return AgentContractRequest(
            name=self.name,
            run_cmd=self.run_cmd,
            install_cmd=self.install_cmd,
            env=self.env,
            artifacts=self.artifacts,
            final_output=str(self.final_output) if self.final_output else None,
        )
