from typing import Any

from pydantic import BaseModel
from vals_model_proxy.base import InputItem


class Task(BaseModel):
    input: list[InputItem]
    extra: dict[str, Any]
