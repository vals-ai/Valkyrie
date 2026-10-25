"""Generation allowance normalization at the benchmark task boundary."""

import pytest

from benchmark_service.sandbox.types import ImageSource, Resources
from benchmark_service.schemas import CreditedGeneration, Episode, RetrieveTaskResponse
from tracker.generation_plan import GenerationPlan


@pytest.mark.parametrize(
    ("credited_generation", "agent_timeout", "expected_allowance", "expected_staged"),
    [
        (None, None, None, False),
        (None, 12.0, 12.0, False),
        (CreditedGeneration(allowance_seconds=30.0, stage_protocol=None), 12.0, 30.0, False),
        (CreditedGeneration(allowance_seconds=30.0, stage_protocol="valkyrie-stage/1"), 12.0, 30.0, True),
    ],
)
def test_task_normalizes_default_clock_and_staging(
    credited_generation: CreditedGeneration | None,
    agent_timeout: float | None,
    expected_allowance: float | None,
    expected_staged: bool,
) -> None:
    episode = Episode(command="run-stage", parallel_agents=1) if expected_staged else None
    task = RetrieveTaskResponse(
        source=ImageSource(image="test-image:latest"),
        problem_path="/task/problem.txt",
        cwd="/task",
        resources=Resources(vcpu=2, memory=4, disk=5),
        agent_timeout=agent_timeout,
        credited_generation=credited_generation,
        episode=episode,
    )

    plan = GenerationPlan.from_task(task)

    assert plan.allowance_seconds == expected_allowance
    assert plan.staged is expected_staged
