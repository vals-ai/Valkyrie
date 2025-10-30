from collections.abc import Callable
from typing import Any


def logger(function: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        print(f"Calling {function.__name__} with args: {args} and kwargs: {kwargs}")

        return function(*args, **kwargs)

    return wrapper
