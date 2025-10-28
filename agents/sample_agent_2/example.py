from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from vals_model_proxy.base import (
    FileInput,
    FileWithBase64,
    InputItem,
    QueryResult,
    TextInput,
    ToolDefinition,
    ToolResult,
)


class Settings:
    """
    Example class of what we would use to store the hyperparameters for a model.

    We would read these from a config.yaml file that we would pass into the model class and store.
    """

    temperature: float
    max_tokens: int
    top_p: float
    kwargs: dict[str, Any]

    def __init__(
        self, temperature: float, max_tokens: int, top_p: float, kwargs: dict[str, Any]
    ):
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.top_p = top_p
        self.kwargs = kwargs


class AgenticModel(ABC):
    """
    Base class that the model provider will fill out,
    this handles parsing the input from a single api call and returning it to the benchmark to be processed.

    This does not have everything so its just a skeleton for what this "Model scaffold" would look like.
    """

    _client: Any
    _settings: Settings
    _tools: list[ToolDefinition]
    _history: list[Any] = []

    def __init__(self, client: Any, settings: Settings, tools: list[ToolDefinition]):
        self._client = client
        self._settings = settings
        self._tools = tools

    @abstractmethod
    def parse_text_input(self, text_input: TextInput) -> dict[str, Any]:
        """
        Takes a TextInput object and transforms it to a format that is expected by the model.

        this should return something like the following:
        {"type": "input_text", "text": text_input.text}
        """
        raise NotImplementedError()

    @abstractmethod
    def parse_file_input(self, file_input: FileInput) -> dict[str, Any]:
        """
        Takes a FileInput object and transforms it to a format that is expected by the model.

        this should return something like the following:
        {
            "type": "image_url",
            "image_url": {
                "detail": "auto",
                "url": f"data:image/{file_input.mime};base64,{file_input.base64}",
            },
        }
        """
        raise NotImplementedError()

    @abstractmethod
    def parse_tool_result(self, tool_result: ToolResult) -> dict[str, Any]:
        """
        Takes a ToolResult object and transforms it to a format that is expected by the model.

        this should return something like the following:
        {
            "role": "tool",
            "tool_call_id": tool_result.tool_call.id,
            "content": tool_result.result,
        }
        """
        raise NotImplementedError()

    def transform_input(self, input: list[InputItem]) -> list[dict[str, Any]]:
        """
        Takes the list of input items and transforms them to a format that
        the model expects to take in, current options include

        TextInput - Raw text response
        FileWithBase64 - Base64 file
        ToolResult - Tool call response
        """

        def parse_input(input: InputItem) -> dict[str, Any]:
            match input:
                case TextInput():
                    return self.parse_text_input(input)
                case FileWithBase64():
                    return self.parse_file_input(input)
                case ToolResult():
                    return self.parse_tool_result(input)
                case _:
                    raise ValueError(f"Unsupported input type: {type(input)}")

        return [parse_input(item) for item in input]

    @abstractmethod
    def parse_query_result(self, result: Any) -> QueryResult:
        """
        takes a result from the model and tranforms it to a QueryResult object.

        Fields that you will need to fill out:
        class QueryResult(BaseModel):
            output_text: str | None = None
            reasoning: str | None = None
            metadata: QueryResultMetadata = Field(default_factory=QueryResultMetadata)
            tool_calls: list[ToolCall] = Field(default_factory=list)

        Leave the history field empty, we store that inside of the class itself.
        """
        raise NotImplementedError()

    def wrapped_query(self, input: list[InputItem]) -> QueryResult:
        transformed_input = self.transform_input(input)

        result = self.query(transformed_input, self._history)
        self._history.append(result)

        query_result = self.parse_query_result(result)

        return query_result

    @abstractmethod
    def query(self, input: list[dict[str, Any]], history: list[Any]) -> Any:
        """
        Takes in the input returned by self.transform_input and the history found at self._history and queries the model.

        This should be a base method that only queries the model and returns a result, like a single api call.

        The method found at `self.parse_query_result` then transforms the result of this query into a QueryResult object that will be passed back into the benchmark and processed.

        This is what we call externally
        ```python
        def wrapped_query(self, input: list[InputItem]) -> QueryResult:
            transformed_input = self.transform_input(input)

            result = self.query(transformed_input, self._history)
            self._history.append(result)

            query_result = self.parse_query_result(result)

            return query_result
        ```
        """
        raise NotImplementedError()


def logger(function: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        print(f"Calling {function.__name__} with args: {args} and kwargs: {kwargs}")

        return function(*args, **kwargs)

    return wrapper


class RunMetadata:
    """
    Would contain run level metadata that we can extract from the query result each turn
    """

    total_input_tokens: int = 0
    total_output_tokens: int = 0

    def add(self, query_result: QueryResult) -> None:
        self.total_input_tokens += query_result.metadata.in_tokens
        self.total_output_tokens += query_result.metadata.out_tokens


class AgentController:
    """
    Acts as the middleware between the agent and the benchmark controller,
    tracking metadata and metrics about the run.

    This is where we would integrate our turn based logging logic, creating
    wrappers around the methods we want to log.
    """

    _agentic: AgenticModel
    _run_metadata: RunMetadata

    def __init__(self, agentic: AgenticModel):
        self._agentic = agentic
        self._run_metadata = RunMetadata()

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
        query_result = self._agentic.wrapped_query(input)

        self.update_metadata(query_result)

        return query_result


class RunConfig:
    max_steps: int


class FinalResult(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0


class BenchmarkController:
    """
    Runs the entire benchmark, handles looping, processes query result to tool results, and getting the final result.

    The logic inside here would change on a benchmark basis, but we would adhere to the same response interface (or we can just use polymorphism to handle this)
    """

    _agent_controller: AgentController
    _run_config: RunConfig
    _final_result: FinalResult

    def __init__(self, agent_controller: AgentController, run_config: RunConfig):
        self._agent_controller = agent_controller
        self._run_config = run_config
        self._final_result = FinalResult()

    @abstractmethod
    def process_query_result(self, query_result: QueryResult) -> list[InputItem]:
        """Implements the tool calling logic and returns the next input we provide to the model"""
        raise NotImplementedError()

    @logger
    def forward(self, query_result: QueryResult) -> list[InputItem]:
        """Takes the query result from the model and generates the next series of input items for the next turn"""
        processed_input = self.process_query_result(query_result)

        return processed_input

    def process_final_result(self) -> dict[str, Any]:
        """Creates the final result json that we end up outputting and showing the user"""
        return self._final_result.model_dump()

    def run(self, input: list[InputItem]) -> dict[str, Any]:
        """
        Runs the entire benchmark, breaks out when we reach the max step count.

        If we were to define a different exit strategy, we would need to add a condition to the loop to break out early.
        """
        next_input = input

        for _ in range(self._run_config.max_steps):
            last_query_result = self._agent_controller.step(next_input)

            next_input = self.forward(last_query_result)

        return self.process_final_result()

