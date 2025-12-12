import os
from datetime import datetime

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromImageParams,
    DaytonaConfig,
    DaytonaError,
    Image,
    SessionExecuteRequest,
)
from typing_extensions import override

from src.base.environment import Environment
from src.base.types import EnvironmentKeys, Task
from src.base_agent import BaseAgent
from src.exceptions import EnvironmentException


class DaytonaEnvironmentKeys(EnvironmentKeys):
    DAYTONA_API_KEY: str
    DAYTONA_API_URL: str
    DAYTONA_TARGET: str


class DaytonaEnvironment(Environment):
    _daytona: AsyncDaytona
    _DEFAULT_TIMEOUT: int = 300  # 5 minutes
    _AUTO_DELETE_TIMER: int = 60  # auto deletes the sandbox after 1 hour
    _DOCKER_IMAGE_PATH: str = "src/Dockerfile.base"

    @staticmethod
    @override
    def _environment_keys() -> DaytonaEnvironmentKeys:
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

        return DaytonaEnvironmentKeys(**_collected_variables)

    @staticmethod
    def create_config() -> AsyncDaytona:
        _environment_keys = DaytonaEnvironment._environment_keys()

        try:
            _daytona = AsyncDaytona(
                config=DaytonaConfig(
                    api_key=_environment_keys.DAYTONA_API_KEY,
                    api_url=_environment_keys.DAYTONA_API_URL,
                    target=_environment_keys.DAYTONA_TARGET,
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

    async def _create_sandbox(self) -> AsyncSandbox:
        """Creates a sandbox in the daytona environment"""

        _image_definition = Image.from_dockerfile(self._DOCKER_IMAGE_PATH)

        _sandbox_params = CreateSandboxFromImageParams(
            image=_image_definition, env_vars=self._environment_keys().model_dump()
        )
        _sandbox = await self._daytona.create(_sandbox_params, timeout=self._DEFAULT_TIMEOUT)

        # Auto apply the auto delete timeout to the sandbox
        await _sandbox.set_auto_delete_interval(self._AUTO_DELETE_TIMER)

        return _sandbox

    async def _start_sandbox_session(self, sandbox: AsyncSandbox, task: Task, agent: BaseAgent) -> tuple[str, str]:
        """
        Creates a session inside of the sandbox that we can use to fetch logs from, returns the command id we can use to fetch logs from the session


        tuple: (cmd_id, session_id)

        """
        session_id = f"{task.id}-{agent.config.model}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Create session with the id that we can later use to fetch logs from
        await sandbox.process.create_session(session_id)

        start_agent_command = agent.build_execute_command(task)

        response = await sandbox.process.execute_session_command(
            session_id,
            SessionExecuteRequest(command=start_agent_command, runAsync=True),
            timeout=self._DEFAULT_TIMEOUT,
        )

        # A successful command will produce a command id, if not and no stderr was produced - something was butchered pretty badly
        if not response.cmd_id:
            message = (
                "did not return a stderr from the command."
                if not response.stderr
                else f"final message: `{response.stderr}`"
            )
            raise EnvironmentException(f"Daytona did not return a required command id, {message}")

        return str(response.cmd_id), session_id

    @override
    async def create(self, task: Task, agent: BaseAgent) -> None:
        """
        Create environment for the task and set it up, including copying over all dependencies needed to run the task
        """
        try:
            _sandbox = await self._create_sandbox()

            # assign id to the sandbox so that we can reference it later
            _, _ = await self._start_sandbox_session(_sandbox, task, agent)

        except DaytonaError as e:
            raise EnvironmentException(f"Could not create sandbox: {e}")

    @override
    async def execute(self, command: str) -> None:
        pass

    @override
    @staticmethod
    async def close(sandbox_id: str) -> None:
        """
        Reinitializes the Daytona environment and stops the sandbox

        NOTE: After the timeout we set earlier the sandbox will be deleted automatically
        """
        _daytona = DaytonaEnvironment.create_config()

        try:
            _daytona_sandbox = await _daytona.get(sandbox_id)

            await _daytona_sandbox.stop()

            print(f"Sandbox {sandbox_id} stopped. deleting in {DaytonaEnvironment._AUTO_DELETE_TIMER} minutes.")
        except DaytonaError as e:
            exception_message = f"Could not get sandbox: {e}"
            raise EnvironmentException(exception_message)
