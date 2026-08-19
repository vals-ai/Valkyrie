"""Supported interface for generated reference documentation."""

from .collect import collect_cli_commands, collect_sdk_reference
from .generate import check_reference, main, write_reference
from .model import CLICommandReference, CLIParameterReference, SDKReference
from .render import render_reference

__all__ = (
    "CLICommandReference",
    "CLIParameterReference",
    "SDKReference",
    "check_reference",
    "collect_cli_commands",
    "collect_sdk_reference",
    "main",
    "render_reference",
    "write_reference",
)
