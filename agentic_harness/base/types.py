from pydantic import BaseModel
from typing import Any


class BaseParameters(BaseModel):
    """
    Base parameters for an agent.
    """

    model: str
    temperature: float
    max_tokens: int
    top_p: float
    kwargs: dict[str, Any]
