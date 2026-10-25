"""Normalize per-task generation allowance and orchestration settings."""

from dataclasses import dataclass

from benchmark_service.schemas import Episode, RetrieveTaskResponse


@dataclass(frozen=True)
class GenerationPlan:
    allowance_seconds: float | None
    staged: bool
    episode: Episode | None

    @classmethod
    def from_task(cls, task_data: RetrieveTaskResponse) -> "GenerationPlan":
        return cls(
            allowance_seconds=(
                task_data.credited_generation.allowance_seconds
                if task_data.credited_generation is not None
                else task_data.agent_timeout
            ),
            staged=(
                task_data.credited_generation is not None and task_data.credited_generation.stage_protocol is not None
            ),
            episode=task_data.episode,
        )
