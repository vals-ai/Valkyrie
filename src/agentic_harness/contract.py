from abc import ABC, abstractmethod

from tracker.types import AgentContractRequest
from agentic_harness.schemas import AgentConfig


class BaseAgentContract(ABC):
    """Base class for agent contracts."""

    def __init__(self, agent_config: AgentConfig | None = None):
        self._agent_config = agent_config

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def run_cmd(self) -> str:
        pass

    @property
    @abstractmethod
    def install_cmd(self) -> str:
        pass

    @property
    def env(self) -> dict[str, str]:
        return {}

    @property
    def artifacts(self) -> list[str]:
        return []

    def to_request(self) -> AgentContractRequest:
        return AgentContractRequest(
            name=self.name,
            run_cmd=self.run_cmd,
            install_cmd=self.install_cmd,
            env=self.env,
            artifacts=self.artifacts,
        )
