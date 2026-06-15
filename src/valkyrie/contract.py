"""Shim — agent contracts now live in tracker.contract."""

from tracker.contract import BaseAgentContract, OutputArtifact, OutputArtifactSpec


__all__ = ["BaseAgentContract", "OutputArtifact", "OutputArtifactSpec"]
