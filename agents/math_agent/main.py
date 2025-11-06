"""Simple math agent that solves addition problems."""

from src.agent_runner import AgentRunner
from vals_model_proxy.base import InputItem


class MathAgent(AgentRunner):
    """Solves addition problems."""

    def run(self, input_items: list[InputItem]) -> dict[str, str]:
        results = {}
        for item in input_items:
            try:
                task_id = item.get("task_id") if isinstance(item, dict) else item.task_id
                data = item.get("input") if isinstance(item, dict) else item.input
                a = data.get("a")
                b = data.get("b")
                results[task_id] = str(a + b) if a is not None and b is not None else "error"
            except Exception as e:
                results[task_id] = f"error: {str(e)}"
        return results


if __name__ == "__main__":
    from src.main import run_agent_on_benchmark

    result = run_agent_on_benchmark(agent_runner=MathAgent(), benchmark_name="addition")
    print("\n" + "="*50)
    print("BENCHMARK RESULTS")
    print("="*50)
    print(f"Benchmark: {result['benchmark']}")
    print(f"Dataset size: {result['dataset_size']}")
    print(f"Output: {result['output']}")
    if result.get('evaluation'):
        print(f"Evaluation: {result['evaluation']}")
    print("="*50 + "\n")
