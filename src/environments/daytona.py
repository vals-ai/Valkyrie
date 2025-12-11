import os
from typing import override

from daytona import AsyncDaytona, DaytonaConfig, DaytonaError

from src.base.environment import Environment
from src.base.types import Sandbox, Task
from src.exceptions import EnvironmentException


class DaytonaEnvironment(Environment):
    _daytona: AsyncDaytona
    _DEFAULT_TIMEOUT: int = 300  # 5 minutes
    _AUTO_DELETE_TIMER: int = 60  # auto deletes the sandbox after 1 hour

    @staticmethod
    def _validate_env() -> dict[str, str]:
        """Ensures that all of the environment variables requested are set before we start setting up the environment."""
        _env_variables: list[str] = ["DAYTONA_API_KEY", "DAYTONA_API_URL", "DAYTONA_TARGET"]
        _collected_variables: dict[str, str] = {}

        _missing_variables: list[str] = []
        for _env_variable in _env_variables:
            _env_value = os.getenv(_env_variable)

            if _env_value is None:
                _missing_variables.append(_env_variable)
            else:
                _collected_variables[_env_variable] = _env_value

        if _missing_variables:
            raise ValueError(
                f"The following environment variables are not set: {', '.join(_missing_variables)}. Please set them in your `.env` file so that they can be sourced."
            )

        return _collected_variables

    @staticmethod
    def create_config() -> AsyncDaytona:
        _collected_variables = DaytonaEnvironment._validate_env()

        try:
            _daytona = AsyncDaytona(
                config=DaytonaConfig(
                    api_key=_collected_variables["DAYTONA_API_KEY"],
                    api_url=_collected_variables["DAYTONA_API_URL"],
                    target=_collected_variables["DAYTONA_TARGET"],
                )
            )

            return _daytona
        except ValueError as e:
            raise EnvironmentException(f"Could not setup Daytona environment: {e}")

    @override
    async def setup(self) -> None:
        """Creates the daytona environment and sets it up for later use"""
        _daytona = DaytonaEnvironment.create_config()

        self._daytona = _daytona

    @override
    async def create(self, task: Task) -> None:
        """
        Create environment for the task and set it up, including copying over all dependencies needed to run the task
        """
        try:
            _sandbox = await self._daytona.create(timeout=self._DEFAULT_TIMEOUT)

            # Auto apply the auto delete timeout to the sandbox
            await _sandbox.set_auto_delete_interval(self._AUTO_DELETE_TIMER)

            # assign id to the sandbox so that we can reference it later
            task.sandbox = Sandbox(id=_sandbox.id)

        except DaytonaError as e:
            raise EnvironmentException(f"Could not create sandbox: {e}")

    @override
    async def execute(self, command: str) -> None:
        pass

    @override
    @staticmethod
    async def close(sandbox: Sandbox) -> None:
        """
        Reinitializes the Daytona environment and stops the sandbox

        NOTE: After the timeout we set earlier the sandbox will be deleted automatically
        """
        _daytona = DaytonaEnvironment.create_config()

        try:
            _daytona_sandbox = await _daytona.get(sandbox.id)

            await _daytona_sandbox.stop()
        except DaytonaError as e:
            exception_message = f"Could not get sandbox: {e}"
            raise EnvironmentException(exception_message)
