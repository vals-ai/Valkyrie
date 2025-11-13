from abc import ABC, abstractmethod
from typing import Any

from model_library.base import InputItem


class Agent(ABC):
    """
    Abstract base class defining the interface for agent execution.

    The Agent class provides a flexible interface for running different types of agents:
    Python-based agents, subprocess/CLI agents, or external agent harnesses. It receives
    prepared inputs from a Benchmark and returns structured output for evaluation.

    Relationship to other classes:
    - Called by: Benchmark (via agent.run() with prepared InputItems)
    - Receives: list[InputItem] from Benchmark.prepare_task()
    - Returns: dict[str, Any] that gets passed to Benchmark.evaluate()

    Implementation flexibility:
    - Implement as an in-process Python agent with direct model calls
    - Implement as a subprocess wrapper calling external CLI agents
    - Implement as a client to external agent services
    - Define your own output structure (dict is the base type)
    """

    @abstractmethod
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
        """
        Execute the agent with the provided inputs and return results.

        This method is the core execution interface for the agent. It receives prepared
        InputItems from the Benchmark and must return a structured dictionary containing
        the agent's output. All agent logic, including agentic loops, tool usage, and
        exit strategies, should be implemented here.

        Args:
            input_items: List of InputItem objects prepared by Benchmark.prepare_task().
                        Typically includes system prompts, examples, and task inputs.

        Returns:
            dict[str, Any]: Agent output in a structured format. Must include at minimum
                           the agent's response/answer. Structure depends on your needs.

        Example 1 - In-process agent with agentic loop:
            ```python
            async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
                # Initialize agent state
                next_input = input_items
                last_result = None

                # Agentic loop with max steps
                for step in range(self._max_steps):
                    # Single agent step (model query + tool use)
                    last_result = await self.step(next_input)

                    # Check if agent wants to exit
                    if last_result.is_complete:
                        break

                    # Prepare inputs for next iteration
                    next_input = self.forward(last_result)

                # Return formatted output
                return {
                    "response": last_result.final_answer,
                    "steps_taken": step + 1,
                    "reasoning": last_result.reasoning
                }
            ```

        Example 2 - Subprocess/CLI agent wrapper:
            ```python
            import subprocess
            import json

            async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
                # Serialize inputs for CLI agent
                input_str = json.dumps([item.to_dict() for item in input_items])

                # Execute external agent as subprocess
                try:
                    output = subprocess.check_output(
                        ["python", "-m", "agents.my_agent", "run", "--input", input_str],
                        timeout=300
                    )
                    result = json.loads(output)
                except subprocess.CalledProcessError as e:
                    return {"error": str(e), "response": ""}

                return result
            ```
        """

        ...
