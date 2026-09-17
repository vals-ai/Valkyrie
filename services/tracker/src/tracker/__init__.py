"""
Methods and types we want to export from the tracker to the CLI
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tracker.aws.s3 import handle_s3_error as handle_s3_error

__all__ = ["handle_s3_error"]


def __getattr__(name: str) -> object:
    # ExecutorHost loads only the release readers, before the Tracker artifact exists.
    if name == "handle_s3_error":
        from tracker.aws.s3 import handle_s3_error

        return handle_s3_error
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
