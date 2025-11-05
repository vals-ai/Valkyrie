"""Simple math agent that solves addition problems."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from agent_runner import AgentRunner


class MathAgent(AgentRunner):
    """Simple agent that solves addition problems."""

    def run(self, input_items: list) -> dict[str, str]:
        """Solve addition problems."""
        results = {}

        for item in input_items:
            try:
                task_id = item.get("task_id") if isinstance(item, dict) else item.task_id
                data = item.get("input") if isinstance(item, dict) else item.input
                a = data.get("a")
                b = data.get("b")

                if a is not None and b is not None:
                    answer = a + b
                    results[task_id] = str(answer)
                else:
                    results[task_id] = "error"
            except Exception as e:
                results[task_id] = f"error: {str(e)}"

        return results


if __name__ == "__main__":
    from main import run_agent_on_benchmark
    
    agent = MathAgent()
    result = run_agent_on_benchmark(
        agent_runner=agent,
        benchmark_name="addition"
    )
    
    print("\n" + "="*50)
    print("BENCHMARK RESULTS")
    print("="*50)
    print(f"Benchmark: {result['benchmark']}")
    print(f"Dataset size: {result['dataset_size']}")
    print(f"Output: {result['output']}")
    if result.get('evaluation'):
        print(f"Evaluation: {result['evaluation']}")
    print("="*50 + "\n")
