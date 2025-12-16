import os
from datetime import datetime

from daytona import (
    AsyncDaytona,
    AsyncSandbox,
    CreateSandboxFromSnapshotParams,
    CreateSnapshotParams,
    DaytonaConfig,
    DaytonaError,
    DaytonaNotFoundError,
    Image,
    SessionExecuteRequest,
)
from daytona.common.snapshot import Snapshot
from typing_extensions import override

from src.base.environment import Environment
from src.base.types import EnvironmentKeys, Task
from src.base_agent import BaseAgent
from src.exceptions import EnvironmentException
from src.logger import get_logger

logger = get_logger(__name__)

stream_logger = get_logger(__name__, stream=True)


class DaytonaEnvironmentKeys(EnvironmentKeys):
    DAYTONA_API_KEY: str
    DAYTONA_API_URL: str
    DAYTONA_TARGET: str


class DaytonaEnvironment(Environment):
    _daytona: AsyncDaytona
    _DEFAULT_TIMEOUT: int = 300  # 5 minutes
    _AUTO_DELETE_TIMER: int = 60  # auto deletes the sandbox after 1 hour

    # TODO: Source from config file
    _DOCKER_IMAGE_PATH: str = "Dockerfile.base"
    _BASE_IMAGE_NAME: str = "agent.base.image"
    _EXTERNAL_IMAGE_NAME: str = "agent.external.image"

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

        logger.info("Daytona environment setup complete, required variables were sourced")

    async def _get_snapshot(self, name: str, path: str) -> Snapshot:
        """Either fetches snapshot if it exists already, or creates a new one from the given path"""

        created: bool = True
        try:
            _base_snapshot = await self._daytona.snapshot.get(name=name)
            created = False
        except DaytonaNotFoundError:
            _image_definition = Image.base(name).from_dockerfile(path)
            _base_snapshot = await self._daytona.snapshot.create(
                CreateSnapshotParams(image=_image_definition, name=name),
                timeout=self._DEFAULT_TIMEOUT,
            )

        logger.info(
            f"Base snapshot: `{_base_snapshot.name}`. Snapshot was {'created' if created else 'fetched from daytona (was already created)'}."
        )

        return _base_snapshot

    async def _create_external_snapshot(self, image_path: str) -> Snapshot:
        """Creates an external snapshot from the image path, using same params as the base snapshot"""

        # Need to create a new image from the base one we created earlier
        with open(image_path) as f:
            commands = [line.strip() for line in f if line.strip()]

        _image_definition = Image.from_dockerfile(self._DOCKER_IMAGE_PATH).dockerfile_commands(commands)

        # Create cached snapshot
        _snapshot = await self._daytona.snapshot.create(
            CreateSnapshotParams(image=_image_definition, name=self._EXTERNAL_IMAGE_NAME),
            timeout=self._DEFAULT_TIMEOUT,
        )

        return _snapshot

    def _create_params(self, name: str, environment_variables: dict[str, str]) -> CreateSandboxFromSnapshotParams:
        daytona_required_vars = self._environment_keys()

        combined_environment_variables: dict[str, str] = {**daytona_required_vars.model_dump(), **environment_variables}

        return CreateSandboxFromSnapshotParams(snapshot=name, env_vars=combined_environment_variables)

    async def _create_sandbox(self, image_path: str | None, environment_variables: dict[str, str]) -> AsyncSandbox:
        # 1. External snapshot is requested, we try to create a sandbox from that if it exists
        if image_path:
            try:
                _snapshot = await self._daytona.snapshot.get(name=self._EXTERNAL_IMAGE_NAME)

                return await self._daytona.create(
                    params=self._create_params(name=_snapshot.name, environment_variables=environment_variables)
                )
            except DaytonaNotFoundError:
                pass

        # 2. We upsert the base snapshot and then create a sandbox from that if no external snapshot is requested
        if not image_path:
            _base_snapshot = await self._get_snapshot(name=self._BASE_IMAGE_NAME, path=self._DOCKER_IMAGE_PATH)

            return await self._daytona.create(
                params=self._create_params(name=_base_snapshot.name, environment_variables=environment_variables)
            )

        logger.info(f"Creating external snapshot from image path: `{image_path}`")

        # 3. We create a new external snapshot and then create a sandbox from that
        _snapshot = await self._create_external_snapshot(image_path)

        logger.info(f"External snapshot created: `{_snapshot.name}`")

        # Auto apply the auto delete timeout to the sandbox
        _sandbox: AsyncSandbox = await self._daytona.create(
            params=self._create_params(name=_snapshot.name, environment_variables=environment_variables)
        )

        logger.info(f"Sandbox created from external snapshot: name: `{_snapshot.name}`, id: `{_sandbox.id}`")

        return _sandbox

    async def _start_sandbox_session(self, sandbox: AsyncSandbox, task: Task, agent: BaseAgent) -> tuple[str, str]:
        """
        Creates a session inside of the sandbox that we can use to fetch logs from, returns the command id we can use to fetch logs from the session


        tuple: (cmd_id, session_id)

        """
        session_id = f"{task.id}-{agent.config.model}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        logger.info(f"Creating session with id: {session_id}")

        # Create session with the id that we can later use to fetch logs from
        await sandbox.process.create_session(session_id)

        start_agent_command = agent.build_execute_command(task)

        logger.info(
            f"Build the execution command: {start_agent_command[:100] + '...' if len(start_agent_command) > 100 else start_agent_command}"
        )

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
            environment_variables = agent.environment_variables
            _sandbox = await self._create_sandbox(
                task.sandbox.image_path if task.sandbox else None, environment_variables
            )

            # assign id to the sandbox so that we can reference it later
            _, _ = await self._start_sandbox_session(_sandbox, task, agent)

        except DaytonaError as e:
            raise EnvironmentException(f"Could not create sandbox: {e}")

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
