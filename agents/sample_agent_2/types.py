from typing import Any

from vals_model_proxy.base import QueryResult


class Settings:
    """
    Example class of what we would use to store the hyperparameters for a model.

    We would read these from a config.yaml file that we would pass into the model class and store.
    """

    temperature: float
    max_tokens: int
    top_p: float
    kwargs: dict[str, Any]

    def __init__(self, temperature: float, max_tokens: int, top_p: float, kwargs: dict[str, Any]):
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.top_p = top_p
        self.kwargs = kwargs


class RunMetadata:
    """
    Would contain run level metadata that we can extract from the query result each turn
    """

    total_input_tokens: int = 0
    total_output_tokens: int = 0

    def add(self, query_result: QueryResult) -> None:
        self.total_input_tokens += query_result.metadata.in_tokens
        self.total_output_tokens += query_result.metadata.out_tokens
