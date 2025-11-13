from typing import Any

from model_library.base import InputItem
from pydantic import BaseModel


class Task(BaseModel):
    input: list[InputItem]
    extra: dict[str, Any]
