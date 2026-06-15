"""Base class for agent contracts written in Python (contract.py)."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from tracker.contract_schemas import AgentConfig
from tracker.database.models import AgentContractRequest, OutputArtifact, OutputArtifactSpec


__all__ = ["AgentConfig", "BaseAgentContract", "OutputArtifact", "OutputArtifactSpec"]


class BaseAgentContract(ABC):
    """
    Base class for agent contracts.

    Agent contracts define how to install and run an agent in a sandbox environment.
    Subclasses must implement the required abstract properties (name, run_cmd, install_cmd)
    and can optionally override secrets and artifacts.
    """

    def __init__(self, agent_config: AgentConfig):
        self._agent_config = agent_config

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str: ...

    @property
    @abstractmethod
    def install_cmd(self) -> str: ...

    @property
    def secrets(self) -> dict[str, str]:
        return {}

    @property
    def ingest_lambda(self) -> str | None:
        return None

    @property
    @abstractmethod
    def final_output(self) -> Path | None: ...

    @property
    def output_artifacts(self) -> list[OutputArtifactSpec]:
        return []

    def to_request(self) -> AgentContractRequest:
        return AgentContractRequest(
            name=self.name,
            model=self._agent_config.model,
            run_cmd=self.run_cmd(
                problem_statement_path="{problem_statement_path}",
                task_id="{task_id}",
                kwargs=self._agent_config.kwargs,
            ),
            install_cmd=self.install_cmd,
            final_output=str(self.final_output) if self.final_output else None,
            output_artifacts=self.output_artifacts,
            secrets=self.secrets,
        )
