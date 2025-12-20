from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from src.base.types import EnvironmentKeys, Task

if TYPE_CHECKING:
    from src.base_agent import AgentRunner


class Environment(ABC):
    """
    Base environment class that all environments must implement.
    """

    _submodule_name: str
    _contract_name: str
    _config: dict[str, Any]

    def __init__(self, config: dict[str, Any], submodule_name: str, contract_name: str):
        self._submodule_name = submodule_name
        self._contract_name = contract_name
        self._config = config

    @staticmethod
    @abstractmethod
    def environment_keys() -> EnvironmentKeys:
        """
        Returns a pydantic model that contains details about the environment variables required to execute the environment.

        ```python
        import os
        from pydantic import BaseModel

        class DaytonaEnvironmentKeys(EnvironmentKeys):
            DAYTONA_API_KEY: str
            ...

        @staticmethod
        @override
        def environment_keys() -> DaytonaEnvironmentKeys:
            return DaytonaEnvironmentKeys(
                DAYTONA_API_KEY=os.getenv("DAYTONA_API_KEY"),
                ....
            )
        ```
        """
        ...

    @abstractmethod
    async def setup(self) -> None:
        """
        Sets up the environment for use, initialize the client.

        NOTE: This is called before we start executing tasks.

        ```python
        @override
        async def setup(self) -> None:
            #Creates the daytona environment and sets it up for later use
            _daytona = DaytonaEnvironment.create_config()

            self._daytona = _daytona
            ```
        """
        ...

    @abstractmethod
    async def create(self, task: Task, agent: "AgentRunner") -> None:
        """
        Creates the sandbox environment for the task, creating a session and starting the task inside.

        ```python
        @override
        async def create(self, task: Task, agent: "AgentRunner") -> None:
            try:
                environment_variables = agent.environment_variables
                _sandbox = await self._create_sandbox(task.sandbox.image_path, environment_variables)

                start_agent_command = agent.build_execute_command(task)

                logger.debug(f"Starting agent command: {start_agent_command}")

                response = await sandbox.process.execute_session_command(
                    session_id,
                    SessionExecuteRequest(command=start_agent_command, runAsync=True),
                )
                ...

            except DaytonaError as e:
                raise EnvironmentException(f"Could not create sandbox: {e}")
        ```
        """
        ...

    @staticmethod
    @abstractmethod
    async def close(sandbox_id: str) -> None:
        """
        Called inside of the sandbox after the task has finished executing,
        should close and remove the sandbox environment to prevent tasks from hanging.

        ```python
        @override
        async def close(sandbox_id: str) -> None:
           _daytona = DaytonaEnvironment.create_config()

            try:
                _daytona_sandbox = await _daytona.get(sandbox_id)

                await _daytona_sandbox.stop()

                logger.warning(
                    f"Sandbox {sandbox_id} stopped. deleting in {DaytonaEnvironment._AUTO_DELETE_TIMER} minutes."
                )
            except DaytonaError as e:
                raise EnvironmentException(f"Could not get sandbox: {e}")
        ```
        """
        ...
