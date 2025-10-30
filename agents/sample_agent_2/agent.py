from abc import abstractmethod
from functools import cached_property
from typing import Any, override

from vals_model_proxy.base import (
    InputItem,
    QueryResult,
    ToolDefinition,
)

from .model import Model
from .types import RunMetadata, Settings
from .utils import logger


class Agent:
    """
    Base class that the model provider will fill out,
    this handles parsing the input from a single api call and returning it to the benchmark to be processed.

    This does not have everything so its just a skeleton for what this "Model scaffold" would look like.
    """

    _model: Model
    _settings: Settings
    _raw_tools: list[ToolDefinition]
    _history: list[Any] = []

    def __init__(self, model: Model, settings: Settings, tools: list[ToolDefinition]):
        self._model = model
        self._settings = settings
        self._raw_tools = tools

    @cached_property
    def _tools(self) -> list[Any]:
        return [self._model.parse_tool_definition(tool) for tool in self._raw_tools]

    def wrapped_query(self, input: list[InputItem]) -> QueryResult:
        transformed_input = self._model.transform_input(input)

        result = self._model.query(transformed_input, self._history, self._tools)
        self._history.append(result)

        query_result = self._model.parse_query_result(result)

        return query_result


class BenchmarkRunner:
    """
    Generic interface for running a benchmark,
    This supports running a submodule, python agent, or a CLI agent.

    For each benchmark we would need to define a output _type_ so that we can unify the final output.
    In this example I just left it as a dict, but it can be anything.
    """

    @abstractmethod
    def run(self, input: list[InputItem]) -> dict[str, Any]:
        """
        Runs the entire benchmark, exit strategies must be handled inside of here.

        Example of running with the agent controller:
        ```python
        def run(self, input: list[InputItem]) -> dict[str, Any]:
            next_input = input

            last_query_result: QueryResult | None = None
            for _ in range(self._max_steps):
                last_query_result = self.step(next_input)

                next_input = self.forward(last_query_result)

            return self.process_final_result(last_query_result)
        ```


        Example of running with a agent harness:
        ```python
        import subprocess

        # Download the agent harness as a submodule, exposing the run method as a CLI command
        # Need to convert the input to text and serialize it to pass it in as a CLI argument
        try:
            input_str: str = format_input(input)
            output = subprocess.check_output(["python", "-m", "agents.sample_agent_2.agent", "run", "--input", input_str])
            result = json.loads(output)
        except subprocess.CalledProcessError as e:
            print(f"Error running agent: {e}")
            raise e

        return result
        ```
        """
        raise NotImplementedError()


class AgentController(BenchmarkRunner):
    """
    Acts as the middleware between the agent and the benchmark controller,
    tracking metadata and metrics about the run.

    This is where we would integrate our turn based logging logic, creating
    wrappers around the methods we want to log.
    """

    _agent: Agent
    _run_metadata: RunMetadata
    _max_steps: int

    def __init__(self, agent: Agent, max_steps: int):
        self._agent = agent
        self._run_metadata = RunMetadata()
        self._max_steps = max_steps

    @property
    def metadata(self) -> RunMetadata:
        """Exposes run metadata to the benchmark controller"""
        return self._run_metadata

    @logger
    def update_metadata(self, query_result: QueryResult) -> None:
        """Takes the query result and updates the run metadata"""
        self._run_metadata.add(query_result)

    def step(self, input: list[InputItem]) -> QueryResult:
        """Processes one api call or one agent turn and returns the query result"""
        query_result = self._agent.wrapped_query(input)

        self.update_metadata(query_result)

        return query_result

    @abstractmethod
    def forward(self, query_result: QueryResult) -> list[InputItem]:
        """
        Takes the query result and returns the next input for the agent.
        """
        raise NotImplementedError()

    @abstractmethod
    def process_final_result(self, query_result: QueryResult | None) -> dict[str, Any]:
        """
        Takes the final query result and returns the final output for the agent.

        If the agent failed to produce the first output, the query_result will be None.
        """
        raise NotImplementedError()

    @override
    def run(self, input: list[InputItem]) -> dict[str, Any]:
        next_input = input

        last_query_result: QueryResult | None = None
        for _ in range(self._max_steps):
            last_query_result = self.step(next_input)

            next_input = self.forward(last_query_result)

        return self.process_final_result(last_query_result)
