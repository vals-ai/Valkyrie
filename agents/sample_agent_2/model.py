from abc import ABC, abstractmethod
from typing import Any

from vals_model_proxy.base import (
    FileInput,
    FileWithBase64,
    InputItem,
    QueryResult,
    TextInput,
    ToolDefinition,
    ToolResult,
)

from .types import Settings


class Model(ABC):
    """
    Base class that the model provider will fill out,
    this handles parsing the input from a single api call and returning it to the benchmark to be processed.

    This does not have everything so its just a skeleton for what this "Model scaffold" would look like.
    """

    _settings: Settings

    def __init__(self, settings: Settings):
        self._settings = settings

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
    def parse_tool_definition(self, tool_definition: ToolDefinition) -> dict[str, Any]:
        """
        Takes a ToolDefinition object and transforms it to a format that is expected by the model.

        this should return something like the following:
        {
            "name": tool_definition.name,
            "description": tool_definition.description,
            "parameters": tool_definition.parameters.model_dump(),
            "required": tool_definition.required,
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

    @abstractmethod
    def query(self, input: list[dict[str, Any]], history: list[Any], tools: list[Any]) -> Any:
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
