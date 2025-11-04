# Benchmark Contract

## Agent

Implement `run()` in `agents/your_agent/main.py`:

```python
def run(input: dict[str, dict], **kwargs) -> dict[str, str]:
    """Process tasks and return outputs.
    
    Args:
        input: Dict mapping task_id to task_data
        **kwargs: Additional arguments
        
    Returns:
        Dict mapping task_id to agent output
    """
    return {"task_1": "output", "task_2": "output"}
```

## Benchmark

Implement `get_dataset()` in `benchmarks/your_benchmark/main.py`:

```python
def get_dataset() -> dict[str, dict]:
    """Return benchmark tasks."""
    return {"task_1": {"input": "..."}, "task_2": {"input": "..."}}

# Optional
requires_sandbox = False
setup_script = "setup.sh"
evaluate_output = lambda output, run_id: {...}  # Optional evaluation
```

## Execution Flow

1. Load benchmark module
2. Call `get_dataset()` → `dict[task_id, task_data]`
3. Call `agent.run(dataset)` → `dict[task_id, output]`
4. Call `evaluate_output()` if available
