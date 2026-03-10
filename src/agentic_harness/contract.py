from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from tracker.database.models import AgentContractRequest

from agentic_harness.schemas import AgentConfig


class BaseAgentContract(ABC):
    """
    Base class for agent contracts.

    Agent contracts define how to install and run an agent in a sandbox environment.
    Subclasses must implement the required abstract properties (name, run_cmd, install_cmd)
    and can optionally override secrets and artifacts.
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
        ...

    @abstractmethod
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        """
        Command to run the agent on a task.

        Args:
            problem_statement_path: str - Where the problem statement is copied in the sandbox (default: tmp/problem_statement)
            task_id: str - The readable task id (e.x astropy__astropy-12907)
            kwargs: dict[str, Any] - Extra args the user specified at run time

        Returns:
            Shell command to execute the agent (e.g., "claude code -p path/to/problem_statement")
        """
        ...

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
        ...

    @property
    def secrets(self) -> dict[str, str]:
        """
        Default secrets for the agent, resolved from AWS Secrets Manager at runtime.

        Override this to declare baseline secrets. These can be supplemented or
        overridden at runtime via the CLI ``-s`` / ``--secret`` flag.

        Returns:
            Mapping of {ENV_VAR_NAME: aws_secret_name} (default: empty dict)
        """
        return {}

    @property
    @abstractmethod
    def final_output(self) -> Path | None:
        """
        Path to the final output of the agent. Needs to be an absolute path.
        Will be saved in s3 when the task is done being processed

        Returns:
            Path | None: The path to the final output that the agent writes to.
        """
        ...

    def to_request(self) -> AgentContractRequest:
        """
        Convert the contract to a request object for the tracker service.

        Returns:
            AgentContractRequest with all contract properties
        """
        return AgentContractRequest(
            name=self.name,
            run_cmd=self.run_cmd(  # NOTE: We replace these fillers in the tracker
                problem_statement_path="{problem_statement_path}",
                task_id="{task_id}",
                kwargs=self._agent_config.kwargs,
            ),
            install_cmd=self.install_cmd,
            final_output=str(self.final_output) if self.final_output else None,
            secrets=self.secrets,
        )
